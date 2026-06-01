use super::*;

impl ViewManager {
    /// Generate events for a query view (with enter/exit detection).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn generate_query_view_events(
        &mut self,
        client_id: &str,
        view_path: &str,
        query_id: &str,
        event: &MutationEvent,
        remaining_segments: &[&str],
        tree: &Tree,
        is_writer_echo: bool,
        tag: Option<i32>,
    ) -> Vec<ClientEvent> {
        // Mutation at view path itself - full recompute
        if remaining_segments.is_empty() {
            return self.recompute_query_view(
                client_id,
                view_path,
                query_id,
                event,
                remaining_segments,
                tree,
                is_writer_echo,
                tag,
            );
        }

        let child_key = remaining_segments[0];

        // Get view state
        let (is_in_view, _has_limit, order_by) = {
            let view = match self.get_view(client_id, view_path, query_id) {
                Some(v) => v,
                None => return Vec::new(),
            };
            (
                view.is_key_in_view(child_key),
                view.has_limit(),
                view.query().order_by.clone(),
            )
        };

        let is_child_mutation = remaining_segments.len() == 1;
        let is_removal = event.mutation_type == "remove" && is_child_mutation;

        // Case 1: Item is in the view
        if is_in_view {
            // If item was removed or sort field changed, may need recompute
            if is_removal || self.is_sort_field_change(&order_by, event, remaining_segments) {
                // Try incremental update first for sort field changes (not removals)
                if !is_removal
                    && self.can_use_incremental_sort(
                        client_id,
                        view_path,
                        query_id,
                        event,
                        remaining_segments,
                    )
                    && let Some(events) = self.handle_incremental_sort_update(
                        client_id,
                        view_path,
                        query_id,
                        event,
                        remaining_segments,
                        tree,
                        is_writer_echo,
                        tag,
                    )
                {
                    return events;
                }
                // Incremental update returned None - fall back to full recompute
                return self.recompute_query_view(
                    client_id,
                    view_path,
                    query_id,
                    event,
                    remaining_segments,
                    tree,
                    is_writer_echo,
                    tag,
                );
            }

            // Non-removal, non-sort-field change - just send delta
            // Verify view exists
            if self.get_view(client_id, view_path, query_id).is_none() {
                return Vec::new();
            }

            // For update operations, send PATCH with the specific changed fields
            let msg = if event.mutation_type == "update" {
                if let Some(ref updates) = event.updates {
                    let mut patch_values = Map::new();
                    for (update_path, update_value) in updates {
                        let full_path = format!("/{}/{}", child_key, update_path);
                        patch_values.insert(full_path, update_value.clone());
                    }
                    let mut msg =
                        ServerMessage::patch_event(view_path, "/", patch_values, event.volatile);
                    if let Some(t) = tag {
                        msg.tag = Some(t);
                    }
                    msg
                } else {
                    // Fallback to PUT if no updates map
                    let relative_path = format!("/{}", remaining_segments.join("/"));
                    let value = event.new_value.clone().unwrap_or(Value::Null);
                    let mut msg =
                        ServerMessage::put_event(view_path, &relative_path, value, event.volatile);
                    if let Some(t) = tag {
                        msg.tag = Some(t);
                    }
                    msg
                }
            } else {
                // For set operations, send PUT
                let relative_path = format!("/{}", remaining_segments.join("/"));
                let value = event.new_value.clone().unwrap_or(Value::Null);
                let mut msg =
                    ServerMessage::put_event(view_path, &relative_path, value, event.volatile);
                if let Some(t) = tag {
                    msg.tag = Some(t);
                }
                msg
            };

            // All query view events must bypass rate limiting to maintain correct client state
            return vec![ClientEvent::new(
                client_id.to_string(),
                view_path.to_string(),
                query_id.to_string(),
                msg,
                event.volatile,
                EventCategory::Change,
            )];
        }

        // Case 2: Item is NOT in the view
        // Check if this could cause it to enter
        if is_child_mutation && event.mutation_type != "remove" {
            // New child added or sort field changed - check if it should enter
            // Try incremental update first
            if self.can_use_incremental_sort(
                client_id,
                view_path,
                query_id,
                event,
                remaining_segments,
            ) && let Some(events) = self.handle_incremental_sort_update(
                client_id,
                view_path,
                query_id,
                event,
                remaining_segments,
                tree,
                is_writer_echo,
                tag,
            ) {
                return events;
            }
            // Incremental update returned None - fall back to full recompute
            return self.recompute_query_view(
                client_id,
                view_path,
                query_id,
                event,
                remaining_segments,
                tree,
                is_writer_echo,
                tag,
            );
        }

        if self.is_sort_field_change(&order_by, event, remaining_segments) {
            // Sort field changed for item outside view - might enter
            // Try incremental update first
            if self.can_use_incremental_sort(
                client_id,
                view_path,
                query_id,
                event,
                remaining_segments,
            ) && let Some(events) = self.handle_incremental_sort_update(
                client_id,
                view_path,
                query_id,
                event,
                remaining_segments,
                tree,
                is_writer_echo,
                tag,
            ) {
                return events;
            }
            // Incremental update returned None - fall back to full recompute
            return self.recompute_query_view(
                client_id,
                view_path,
                query_id,
                event,
                remaining_segments,
                tree,
                is_writer_echo,
                tag,
            );
        }

        // Item outside view and change doesn't affect view - no event
        Vec::new()
    }

    /// Check if a mutation affects the sort field for a query.
    fn is_sort_field_change(
        &self,
        order_by: &OrderBy,
        event: &MutationEvent,
        remaining_segments: &[&str],
    ) -> bool {
        // Update at child level - check if any update path matches sort field
        if event.mutation_type == "update"
            && let Some(updates) = event.updates.as_ref()
            && remaining_segments.len() == 1
        {
            return self.update_affects_sort_field(order_by, updates);
        }

        if remaining_segments.len() < 2 {
            // Direct child change always affects sort
            return true;
        }

        // Path within the child: remaining[0] is child key, rest is subpath
        let subpath = remaining_segments[1..].join("/");

        match order_by {
            OrderBy::Child(child_path) => {
                // Check if mutation is to the orderByChild path
                subpath == *child_path || child_path.starts_with(&format!("{}/", subpath))
            }
            OrderBy::Value => remaining_segments.len() == 1,
            OrderBy::Key => false,
            // Priority ordering: changes to .priority affect sort order
            OrderBy::Priority => subpath == ".priority",
        }
    }

    fn update_affects_sort_field(&self, order_by: &OrderBy, updates: &Map<String, Value>) -> bool {
        match order_by {
            OrderBy::Child(child_path) => {
                for update_path in updates.keys() {
                    if update_path == child_path
                        || child_path.starts_with(&format!("{}/", update_path))
                        || update_path.starts_with(&format!("{}/", child_path))
                    {
                        return true;
                    }
                }
                false
            }
            OrderBy::Value => true,
            OrderBy::Key => false,
            // Priority ordering: updates to .priority affect sort order
            OrderBy::Priority => updates.contains_key(".priority"),
        }
    }

    /// Recompute a query view and generate enter/exit/move events.
    ///
    /// OPTIMIZATION: This function uses lazy value fetching to avoid copying
    /// all child values. It only extracts sort values for sorting/filtering,
    /// then fetches full values only for keys that actually need them.
    #[allow(clippy::too_many_arguments)]
    fn recompute_query_view(
        &mut self,
        client_id: &str,
        view_path: &str,
        query_id: &str,
        event: &MutationEvent,
        remaining_segments: &[&str],
        tree: &Tree,
        _is_writer_echo: bool,
        tag: Option<i32>,
    ) -> Vec<ClientEvent> {
        // Get the node at view path
        let view_path_obj = Path::parse(view_path);
        let node = tree.get(&view_path_obj);

        // Get current view state
        let (old_keys, query) = {
            let view = match self.get_view(client_id, view_path, query_id) {
                Some(v) => v,
                None => return Vec::new(),
            };
            (view.ordered_keys().to_vec(), view.query().clone())
        };

        // Compute new ordered keys using LIGHTWEIGHT sort entries (no full value copies)
        // Also build sort_key_cache and boundary for incremental updates.
        let (new_keys, new_sort_key_cache, new_boundary) = if let Some(node) = node {
            let children_keys: Vec<String> = node.keys().map(|s| s.to_string()).collect();

            // Build lightweight sort entries - only extract sort values, not full values
            let sort_entries: Vec<SortEntry> = children_keys
                .iter()
                .filter_map(|key| {
                    let child = node.get(key)?;
                    // Use efficient sort value extraction (doesn't copy entire child)
                    let sort_value = child.get_sort_value(&query.order_by);
                    Some(SortEntry::new(key.clone(), sort_value))
                })
                .collect();

            // Build a map of key -> sort_value before filtering
            let all_sort_values: HashMap<String, Option<SortKey>> = sort_entries
                .iter()
                .map(|e| (e.key.clone(), e.sort_value.clone()))
                .collect();

            // Apply query to get filtered/sorted keys
            let result_keys = apply_query_to_sort_entries(sort_entries, &query);

            // Build cache only for keys in the result (saves memory)
            let cache: HashMap<String, Option<SortKey>> = result_keys
                .iter()
                .filter_map(|key| all_sort_values.get(key).map(|v| (key.clone(), v.clone())))
                .collect();

            // Compute boundary based on limit type
            // IMPORTANT: Only set boundary when view is FULL (at capacity)
            // This ensures we only do swaps when adding an item would exceed the limit
            let boundary = match query.limit {
                Some(Limit::First(limit_val)) if result_keys.len() == limit_val => {
                    // limitToFirst: boundary is the LAST item (highest in view)
                    result_keys.last().map(|key| BoundaryItem {
                        key: key.clone(),
                        sort_value: cache.get(key).cloned().flatten(),
                    })
                }
                Some(Limit::Last(limit_val)) if result_keys.len() == limit_val => {
                    // limitToLast: boundary is the FIRST item (lowest in view)
                    result_keys.first().map(|key| BoundaryItem {
                        key: key.clone(),
                        sort_value: cache.get(key).cloned().flatten(),
                    })
                }
                _ => None, // No limit, or view not at capacity yet
            };

            (result_keys, cache, boundary)
        } else {
            (Vec::new(), HashMap::new(), None)
        };

        // Update view state with new keys, cache, and boundary
        if let Some(view) = self.get_view_mut(client_id, view_path, query_id) {
            view.ordered_keys = new_keys.clone();
            view.sort_key_cache = new_sort_key_cache;
            view.boundary = new_boundary;
        }

        // If node doesn't exist, send null
        if node.is_none() {
            let mut msg = ServerMessage::put_event(view_path, "/", Value::Null, event.volatile);
            if let Some(t) = tag {
                msg.tag = Some(t);
            }

            return vec![ClientEvent::new(
                client_id.to_string(),
                view_path.to_string(),
                query_id.to_string(),
                msg,
                event.volatile,
                EventCategory::Remove,
            )];
        }

        // SPECIAL CASE: Mutation at view path itself (e.g., set('/messages', {...}))
        // Send a single PUT event with the full filtered result.
        // Here we DO need full values, but only for keys in the result (not all children).
        // OPTIMIZATION: Build ArcValue::Object directly using O(1) Arc clones instead of to_value().
        if remaining_segments.is_empty() {
            let node = node.unwrap();
            let arc_value = if new_keys.is_empty() {
                ArcValue::Null
            } else {
                let mut value_map = HashMap::new();
                for key in &new_keys {
                    if let Some(child) = node.get(key) {
                        // O(1) Arc clone instead of O(n) to_value()
                        value_map.insert(key.clone(), child.clone());
                    }
                }
                ArcValue::Object(Arc::new(value_map))
            };

            let mut msg = ServerMessage::put_event_arc(view_path, "/", arc_value, event.volatile);
            if let Some(t) = tag {
                msg.tag = Some(t);
            }

            return vec![ClientEvent::new(
                client_id.to_string(),
                view_path.to_string(),
                query_id.to_string(),
                msg,
                event.volatile,
                EventCategory::Initial,
            )];
        }

        // Find entered, exited, and moved keys
        let old_set: HashSet<_> = old_keys.iter().cloned().collect();
        let new_set: HashSet<_> = new_keys.iter().cloned().collect();

        let entered: Vec<_> = new_keys
            .iter()
            .filter(|k| !old_set.contains(*k))
            .cloned()
            .collect();
        let exited: Vec<_> = old_keys
            .iter()
            .filter(|k| !new_set.contains(*k))
            .cloned()
            .collect();

        // If both entered AND exited (boundary swap), send a single atomic patch
        // containing null for exited children and data for entered children.
        // This is processed atomically by the SDK (one value callback).
        if !entered.is_empty() && !exited.is_empty() {
            let node = node.unwrap();
            let mut patch_map = HashMap::new();

            for key in &exited {
                patch_map.insert(format!("/{}", key), ArcValue::Null);
            }
            for key in &entered {
                if let Some(child) = node.get(key) {
                    patch_map.insert(format!("/{}", key), child.clone());
                }
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

            return vec![ClientEvent::new(
                client_id.to_string(),
                view_path.to_string(),
                query_id.to_string(),
                msg,
                event.volatile,
                EventCategory::Change,
            )];
        }

        let node = node.unwrap();
        let mut events = Vec::new();

        // Identify the triggering key (if any)
        let trigger_key = if !remaining_segments.is_empty() {
            Some(remaining_segments[0].to_string())
        } else {
            None
        };

        // Find moved items (in both lists, but predecessor changed)
        let old_predecessor: std::collections::HashMap<_, _> = old_keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                (
                    k.clone(),
                    if i == 0 {
                        String::new()
                    } else {
                        old_keys[i - 1].clone()
                    },
                )
            })
            .collect();
        let new_predecessor: std::collections::HashMap<_, _> = new_keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                (
                    k.clone(),
                    if i == 0 {
                        String::new()
                    } else {
                        new_keys[i - 1].clone()
                    },
                )
            })
            .collect();

        let mut moved: Vec<String> = Vec::new();
        for key in &new_keys {
            if old_set.contains(key) {
                let old_pred = old_predecessor.get(key).map(|s| s.as_str()).unwrap_or("");
                let new_pred = new_predecessor.get(key).map(|s| s.as_str()).unwrap_or("");
                if old_pred != new_pred {
                    moved.push(key.clone());
                }
            }
        }

        // Generate exit events
        for key in &exited {
            let mut msg = ServerMessage::put_event(
                view_path,
                &format!("/{}", key),
                Value::Null,
                event.volatile,
            );
            if let Some(t) = tag {
                msg.tag = Some(t);
            }

            events.push(ClientEvent::new(
                client_id.to_string(),
                view_path.to_string(),
                query_id.to_string(),
                msg,
                event.volatile,
                EventCategory::Remove,
            ));
        }

        // Generate enter events
        // OPTIMIZATION: Use put_event_arc to avoid to_value() conversion.
        for key in &entered {
            if let Some(child) = node.get(key) {
                // O(1) Arc clone instead of O(n) to_value()
                let mut msg = ServerMessage::put_event_arc(
                    view_path,
                    &format!("/{}", key),
                    child.clone(),
                    event.volatile,
                );
                if let Some(t) = tag {
                    msg.tag = Some(t);
                }

                events.push(ClientEvent::new(
                    client_id.to_string(),
                    view_path.to_string(),
                    query_id.to_string(),
                    msg,
                    event.volatile,
                    EventCategory::Add,
                ));
            }
        }

        // Handle moves and changes: if the trigger key moved or is in the view, send change event
        if let Some(ref trigger) = trigger_key {
            let trigger_moved = moved.contains(trigger);
            let trigger_in_view = new_set.contains(trigger);
            let trigger_entered = entered.contains(trigger);

            // Case 1: Trigger key moved - send PATCH with the changed data
            if trigger_moved {
                let mut patch_values = Map::new();
                if event.mutation_type == "update" {
                    // Update operation - use the updates map for specific paths
                    if let Some(ref updates) = event.updates {
                        for (update_path, update_value) in updates {
                            patch_values.insert(
                                format!("/{}/{}", trigger, update_path),
                                update_value.clone(),
                            );
                        }
                    }
                } else if remaining_segments.len() == 1 {
                    // Direct child set - send the full value at /childKey
                    patch_values.insert(
                        format!("/{}", trigger),
                        event.new_value.clone().unwrap_or(Value::Null),
                    );
                } else {
                    // Nested set - send the specific path that changed
                    let relative_path = format!("/{}", remaining_segments.join("/"));
                    patch_values.insert(
                        relative_path,
                        event.new_value.clone().unwrap_or(Value::Null),
                    );
                }

                let mut msg =
                    ServerMessage::patch_event(view_path, "/", patch_values, event.volatile);
                if let Some(t) = tag {
                    msg.tag = Some(t);
                }

                events.push(ClientEvent::new(
                    client_id.to_string(),
                    view_path.to_string(),
                    query_id.to_string(),
                    msg,
                    event.volatile,
                    EventCategory::Change,
                ));
            }
            // Case 2: Trigger key is in view, didn't move, but also didn't enter (sort field changed without position change)
            else if moved.is_empty() && trigger_in_view && !trigger_entered {
                let mut patch_values = Map::new();
                if event.mutation_type == "update" {
                    // Update operation - use the updates map for specific paths
                    if let Some(ref updates) = event.updates {
                        for (update_path, update_value) in updates {
                            patch_values.insert(
                                format!("/{}/{}", trigger, update_path),
                                update_value.clone(),
                            );
                        }
                    }
                } else if remaining_segments.len() == 1 {
                    // Direct child mutation - send the full value at /childKey
                    patch_values.insert(
                        format!("/{}", trigger),
                        event.new_value.clone().unwrap_or(Value::Null),
                    );
                } else {
                    // Nested set - send the specific path that changed
                    let relative_path = format!("/{}", remaining_segments.join("/"));
                    patch_values.insert(
                        relative_path,
                        event.new_value.clone().unwrap_or(Value::Null),
                    );
                }

                let mut msg =
                    ServerMessage::patch_event(view_path, "/", patch_values, event.volatile);
                if let Some(t) = tag {
                    msg.tag = Some(t);
                }

                events.push(ClientEvent::new(
                    client_id.to_string(),
                    view_path.to_string(),
                    query_id.to_string(),
                    msg,
                    event.volatile,
                    EventCategory::Change,
                ));
            }
        }

        events
    }
}
