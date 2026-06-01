use super::*;

impl ViewManager {
    /// Check if incremental sort update can be used instead of full recompute.
    /// Returns true if the mutation is safe for incremental handling.
    pub(super) fn can_use_incremental_sort(
        &self,
        client_id: &str,
        view_path: &str,
        query_id: &str,
        event: &MutationEvent,
        remaining_segments: &[&str],
    ) -> bool {
        // Must be a direct child mutation (not deeper nested)
        if remaining_segments.len() != 1 {
            return false;
        }

        // Must not be a removal (removals need full recompute to find replacement)
        if event.mutation_type == "remove" {
            return false;
        }

        let view = match self.get_view(client_id, view_path, query_id) {
            Some(v) => v,
            None => return false,
        };

        // Must have a limit (otherwise no boundary tracking needed)
        if !view.has_limit() {
            return false;
        }

        // Range constraints are now supported - we check in_range during swap logic

        // Must have sort_key_cache populated (i.e., we've done at least one recompute)
        if view.shared_view.sort_key_cache.is_empty() && !view.ordered_keys().is_empty() {
            return false;
        }

        true
    }

    /// Handle incremental sort update for limited queries.
    /// Returns events if the update could be handled incrementally, None if full recompute needed.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn handle_incremental_sort_update(
        &mut self,
        client_id: &str,
        view_path: &str,
        query_id: &str,
        event: &MutationEvent,
        remaining_segments: &[&str],
        tree: &Tree,
        is_writer_echo: bool,
        tag: Option<i32>,
    ) -> Option<Vec<ClientEvent>> {
        let trigger_key = remaining_segments[0].to_string();

        // Get the node at view path to extract new sort value
        let view_path_obj = Path::parse(view_path);
        let node = tree.get(&view_path_obj)?;
        let trigger_child = node.get(&trigger_key);

        // Get current view state from shared view
        let (is_in_view, query, limit, ordered_keys, boundary, old_sort_value) = {
            let view_key = ViewKey::new(view_path, query_id);
            let shared_view = self.shared_views.get(&view_key)?;
            (
                shared_view.is_key_in_view(&trigger_key),
                shared_view.query.clone(),
                shared_view.query.limit?,
                shared_view.ordered_keys.clone(),
                shared_view.boundary.clone(),
                shared_view.sort_key_cache.get(&trigger_key).cloned(),
            )
        };

        // Get new sort value for the trigger key
        let new_sort_value = trigger_child
            .as_ref()
            .and_then(|c| c.get_sort_value(&query.order_by));

        // Determine if this is limitToFirst or limitToLast
        let is_limit_to_first = matches!(limit, Limit::First(_));

        if is_in_view {
            // Case 1: Item is currently in the view
            // Check if it should stay or be replaced by an outside item
            self.handle_in_view_sort_change(
                client_id,
                view_path,
                query_id,
                &trigger_key,
                new_sort_value,
                old_sort_value.flatten(),
                &query,
                is_limit_to_first,
                &ordered_keys,
                tree,
                event,
                is_writer_echo,
                tag,
            )
        } else {
            // Case 2: Item is outside the view
            // Check if it should enter by beating the boundary
            self.handle_outside_view_sort_change(
                client_id,
                view_path,
                query_id,
                &trigger_key,
                new_sort_value,
                &query,
                is_limit_to_first,
                &ordered_keys,
                boundary.as_ref(),
                tree,
                event,
                is_writer_echo,
                tag,
            )
        }
    }

    /// Handle sort field change for an item currently in the view.
    #[allow(clippy::too_many_arguments)]
    fn handle_in_view_sort_change(
        &mut self,
        client_id: &str,
        view_path: &str,
        query_id: &str,
        trigger_key: &str,
        new_sort_value: Option<SortKey>,
        old_sort_value: Option<SortKey>,
        query: &Query,
        _is_limit_to_first: bool,
        ordered_keys: &[String],
        _tree: &Tree,
        event: &MutationEvent,
        _is_writer_echo: bool,
        tag: Option<i32>,
    ) -> Option<Vec<ClientEvent>> {
        // Check if the item's new sort value is still in range
        // If it falls out of range, we need to find a replacement - fall back to recompute
        if !is_in_range(new_sort_value.as_ref(), trigger_key, query) {
            // Item fell out of range - needs replacement, fall back to recompute
            return None;
        }

        // For in-view changes, we take a simpler approach:
        // - If position doesn't change, just update the cache
        // - If position might change, fall back to recompute
        // This avoids an expensive O(N) scan of all children outside the view.
        // Items only get "pushed out" when a NEW item enters that beats the boundary,
        // which is handled by handle_outside_view_sort_change.
        {
            // Check if position changed within the view BEFORE getting mutable borrow
            // This uses only data we've already extracted
            let position_changed = self.check_position_changed(
                trigger_key,
                new_sort_value.as_ref(),
                old_sort_value.as_ref(),
                ordered_keys,
                &query.order_by,
            );

            if position_changed {
                // Position changed - need to update ordered_keys and boundary
                // Fall back to recompute for now to handle this correctly
                return None;
            }

            // Now update the sort key cache and boundary
            if let Some(view) = self.get_view_mut(client_id, view_path, query_id) {
                view.sort_key_cache
                    .insert(trigger_key.to_string(), new_sort_value.clone());

                // Update boundary if trigger is the boundary
                if let Some(ref mut boundary) = view.boundary
                    && boundary.key == trigger_key
                {
                    boundary.sort_value = new_sort_value.clone();
                }
            }

            // Generate change event (item stayed in view, just value changed)
            // All query view events must bypass rate limiting to maintain correct client state
            let msg = self.build_change_message(view_path, trigger_key, event, tag);
            Some(vec![ClientEvent::new(
                client_id.to_string(),
                view_path.to_string(),
                query_id.to_string(),
                msg,
                event.volatile,
                EventCategory::Change,
            )])
        }
    }

    /// Handle sort field change for an item outside the view.
    /// If the item should enter the view, performs a direct swap with the boundary.
    #[allow(clippy::too_many_arguments)]
    fn handle_outside_view_sort_change(
        &mut self,
        client_id: &str,
        view_path: &str,
        query_id: &str,
        trigger_key: &str,
        new_sort_value: Option<SortKey>,
        query: &Query,
        is_limit_to_first: bool,
        ordered_keys: &[String],
        boundary: Option<&BoundaryItem>,
        tree: &Tree,
        event: &MutationEvent,
        _is_writer_echo: bool,
        tag: Option<i32>,
    ) -> Option<Vec<ClientEvent>> {
        // First check if the item is in range (for queries with range constraints)
        // If not in range, it can never enter the view
        if !is_in_range(new_sort_value.as_ref(), trigger_key, query) {
            // Item is out of range - no events needed, stays outside view
            return Some(Vec::new());
        }

        // Check if trigger beats the boundary
        let boundary = match boundary {
            Some(b) => b,
            None => {
                // No boundary means view is empty or not full yet
                // Fall back to full recompute to handle this correctly
                return None;
            }
        };

        let cmp = self.compare_sort_entries_with_key(
            new_sort_value.as_ref(),
            trigger_key,
            boundary.sort_value.as_ref(),
            &boundary.key,
            &query.order_by,
        );

        let should_enter = if is_limit_to_first {
            // For limitToFirst: trigger enters if it's LESS than boundary
            cmp == Ordering::Less
        } else {
            // For limitToLast: trigger enters if it's GREATER than boundary
            cmp == Ordering::Greater
        };

        if !should_enter {
            // Trigger stays outside - no events needed
            return Some(Vec::new());
        }

        // Trigger should enter, boundary exits - perform direct swap
        // Get the full value of the entering item for the event
        let view_path_obj = Path::parse(view_path);
        let node = tree.get(&view_path_obj)?;

        // Verify trigger exists (we need it for the snapshot)
        node.get(trigger_key)?;

        let exiting_key = boundary.key.clone();

        // Find insertion position for the new item
        // We need to get the sort_key_cache from the shared view
        let sort_key_cache = {
            let view = self.get_view(client_id, view_path, query_id)?;
            view.shared_view.sort_key_cache.clone()
        };

        let insertion_pos = self.find_insertion_position(
            trigger_key,
            new_sort_value.as_ref(),
            ordered_keys,
            &query.order_by,
            &sort_key_cache,
        );

        // Update view state
        let new_boundary = {
            let view = self.get_view_mut(client_id, view_path, query_id)?;

            // Remove the exiting boundary from ordered_keys
            let exit_pos = if is_limit_to_first {
                // limitToFirst: boundary is at the end
                view.ordered_keys.len().saturating_sub(1)
            } else {
                // limitToLast: boundary is at the beginning
                0
            };

            if exit_pos < view.ordered_keys.len() {
                view.ordered_keys.remove(exit_pos);
            }

            // Adjust insertion position if we removed an item before it.
            // The two branches coincide but cover distinct limit directions.
            #[allow(clippy::if_same_then_else)]
            let adjusted_pos = if !is_limit_to_first && insertion_pos > 0 {
                insertion_pos - 1
            } else if is_limit_to_first && insertion_pos > exit_pos {
                insertion_pos - 1
            } else {
                insertion_pos
            };

            // Insert the new item at the correct position
            let insert_at = adjusted_pos.min(view.ordered_keys.len());
            view.ordered_keys.insert(insert_at, trigger_key.to_string());

            // Update sort_key_cache
            view.sort_key_cache.remove(&exiting_key);
            view.sort_key_cache
                .insert(trigger_key.to_string(), new_sort_value.clone());

            // Compute new boundary
            let new_boundary_key = if is_limit_to_first {
                // limitToFirst: boundary is the last item (highest)
                view.ordered_keys.last()
            } else {
                // limitToLast: boundary is the first item (lowest)
                view.ordered_keys.first()
            };

            new_boundary_key.map(|key| {
                let sort_val = view.sort_key_cache.get(key).cloned().flatten();
                BoundaryItem {
                    key: key.clone(),
                    sort_value: sort_val,
                }
            })
        };

        // Update the boundary
        if let Some(view) = self.get_view_mut(client_id, view_path, query_id) {
            view.boundary = new_boundary;
        }

        // Send a single atomic patch: remove exiting boundary + add entering item.
        // Serializes only the two changed children instead of the entire query view.
        let mut patch_map = HashMap::new();
        patch_map.insert(format!("/{}", exiting_key), ArcValue::Null);
        if let Some(child) = node.get(trigger_key) {
            patch_map.insert(format!("/{}", trigger_key), child.clone());
        }

        let mut msg = ServerMessage::patch_event_arc(
            view_path,
            "/",
            ArcValue::Object(Arc::new(patch_map)),
            event.volatile,
        );
        if let Some(t) = tag {
            msg.tag = Some(t);
        }

        Some(vec![ClientEvent::new(
            client_id.to_string(),
            view_path.to_string(),
            query_id.to_string(),
            msg,
            event.volatile,
            EventCategory::Change,
        )])
    }

    /// Find the correct insertion position for a new item in the ordered_keys list.
    /// Uses binary search for O(log N) performance.
    fn find_insertion_position(
        &self,
        key: &str,
        sort_value: Option<&SortKey>,
        ordered_keys: &[String],
        order_by: &OrderBy,
        sort_key_cache: &HashMap<String, Option<SortKey>>,
    ) -> usize {
        if ordered_keys.is_empty() {
            return 0;
        }

        // Binary search to find insertion point
        let mut low = 0;
        let mut high = ordered_keys.len();

        while low < high {
            let mid = (low + high) / 2;
            let mid_key = &ordered_keys[mid];
            let mid_sort_value = sort_key_cache.get(mid_key).and_then(|v| v.as_ref());

            let cmp = self.compare_sort_entries_with_key(
                sort_value,
                key,
                mid_sort_value,
                mid_key,
                order_by,
            );

            match cmp {
                Ordering::Less => high = mid,
                Ordering::Greater => low = mid + 1,
                Ordering::Equal => return mid, // Exact match (shouldn't happen for new items)
            }
        }

        low
    }

    /// Compare two sort entries, using key as tie-breaker.
    fn compare_sort_entries_with_key(
        &self,
        a_sort: Option<&SortKey>,
        a_key: &str,
        b_sort: Option<&SortKey>,
        b_key: &str,
        order_by: &OrderBy,
    ) -> Ordering {
        // For orderByKey, just compare keys
        if matches!(order_by, OrderBy::Key) {
            return crate::db::value::compare_keys(a_key, b_key);
        }

        // Compare sort values first
        match (a_sort, b_sort) {
            (Some(a), Some(b)) => {
                let cmp = compare_sort_keys(a, b);
                if cmp == Ordering::Equal {
                    // Tie-breaker: compare keys
                    crate::db::value::compare_keys(a_key, b_key)
                } else {
                    cmp
                }
            }
            (Some(_), None) => Ordering::Greater, // Items with sort value come after null
            (None, Some(_)) => Ordering::Less,
            (None, None) => crate::db::value::compare_keys(a_key, b_key),
        }
    }

    /// Check if an item's position changed within the view after sort value update.
    fn check_position_changed(
        &self,
        trigger_key: &str,
        new_sort_value: Option<&SortKey>,
        old_sort_value: Option<&SortKey>,
        ordered_keys: &[String],
        _order_by: &OrderBy,
    ) -> bool {
        // Find current position
        let pos = match ordered_keys.iter().position(|k| k == trigger_key) {
            Some(p) => p,
            None => return true, // Not found, definitely changed
        };

        // Check if new value would compare differently with neighbors
        // Check predecessor
        if pos > 0 {
            let _pred_key = &ordered_keys[pos - 1];
            // We don't have predecessor's sort value cached here, so be conservative
            // If sort value changed at all, assume position might have changed
            if new_sort_value != old_sort_value {
                return true;
            }
        }

        // Check successor
        if pos < ordered_keys.len() - 1 {
            let _succ_key = &ordered_keys[pos + 1];
            if new_sort_value != old_sort_value {
                return true;
            }
        }

        false
    }

    /// Build a change message for an item that stayed in view.
    fn build_change_message(
        &self,
        view_path: &str,
        trigger_key: &str,
        event: &MutationEvent,
        tag: Option<i32>,
    ) -> ServerMessage {
        let mut msg = if event.mutation_type == "update" {
            if let Some(ref updates) = event.updates {
                let mut patch_values = Map::new();
                for (update_path, update_value) in updates {
                    patch_values.insert(
                        format!("/{}/{}", trigger_key, update_path),
                        update_value.clone(),
                    );
                }
                ServerMessage::patch_event(view_path, "/", patch_values, event.volatile)
            } else {
                let value = event.new_value.clone().unwrap_or(Value::Null);
                ServerMessage::put_event(
                    view_path,
                    &format!("/{}", trigger_key),
                    value,
                    event.volatile,
                )
            }
        } else {
            let value = event.new_value.clone().unwrap_or(Value::Null);
            ServerMessage::put_event(
                view_path,
                &format!("/{}", trigger_key),
                value,
                event.volatile,
            )
        };

        if let Some(t) = tag {
            msg.tag = Some(t);
        }

        msg
    }
}
