//! Incremental update methods on BlobSession.
//!
//! These methods were moved from standalone functions in `incremental.rs`
//! so they have access to BlobSession state (`self.io`, `self.header`,
//! `self.dict`, `self.field_id_size`, `self.free_list`).

use crate::arc_value::ArcValue;
use crate::dictionary::hash_field_name;
use crate::error::{BlobError, Result};
use crate::format::*;
use crate::incremental::{IncrementalStats, TargetInfo, UpdateNode};
use crate::io::BlobIO;
use crate::nav_cache::read_container;
use crate::session::BlobSession;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::trace;

impl<IO: BlobIO> BlobSession<IO> {
    /// Walk the UpdateNode tree depth-first and apply updates.
    ///
    /// At each Merge node over a TYPE_COLLECTION, collects Set children and
    /// batch-inserts them via `batch_insert_into_collection`. All other operations
    /// (scalar updates, deletes, object rewrites) delegate to `apply_single_update`.
    pub(crate) async fn apply_tree(
        &mut self,
        src: &IO,
        tree: &HashMap<String, UpdateNode>,
        path_so_far: &mut Vec<String>,
        stats: &mut IncrementalStats,
    ) -> Result<()> {
        let mut nodes_since_yield: u32 = 0;
        for (key, node) in tree {
            nodes_since_yield += 1;
            if nodes_since_yield >= 1024 {
                self.io.yield_point().await;
                nodes_since_yield = 0;
            }
            match node {
                UpdateNode::Set(_) | UpdateNode::Delete => {
                    path_so_far.push(key.clone());
                    let path_refs: Vec<&str> = path_so_far.iter().map(|s| s.as_str()).collect();
                    let value = match node {
                        UpdateNode::Set(v) => Some(v),
                        UpdateNode::Delete => None,
                        _ => unreachable!(),
                    };
                    self.apply_single_update(src, &path_refs, value, stats)
                        .await?;
                    path_so_far.pop();
                }
                UpdateNode::Merge(children) => {
                    path_so_far.push(key.clone());

                    let path_refs: Vec<&str> = path_so_far.iter().map(|s| s.as_str()).collect();
                    let nav_result = self.navigate_to_depth(&path_refs).await?;

                    let is_collection = if nav_result.1 == path_refs.len() {
                        let deepest = &nav_result.0[nav_result.0.len() - 1];
                        let hint = if deepest.subtree_size > 0 {
                            Some(deepest.subtree_size)
                        } else {
                            None
                        };
                        match read_container(&self.io, deepest.resolved_offset, hint).await {
                            Ok(container) => container.tag == TYPE_COLLECTION,
                            Err(_) => false,
                        }
                    } else {
                        false
                    };

                    if is_collection {
                        let mut new_inserts: Vec<(&str, &ArcValue)> = Vec::new();
                        let mut other_children: Vec<(&String, &UpdateNode)> = Vec::new();

                        for (child_key, child_node) in children {
                            match child_node {
                                UpdateNode::Set(val) => {
                                    new_inserts.push((child_key.as_str(), val));
                                }
                                _ => {
                                    other_children.push((child_key, child_node));
                                }
                            }
                        }

                        if !new_inserts.is_empty() {
                            let (ancestors, depth) = self.navigate_to_depth(&path_refs).await?;
                            if depth == path_refs.len() {
                                let deepest = &ancestors[ancestors.len() - 1];
                                let hint = if deepest.subtree_size > 0 {
                                    Some(deepest.subtree_size)
                                } else {
                                    None
                                };
                                let container =
                                    read_container(&self.io, deepest.resolved_offset, hint).await?;

                                let mut truly_new: Vec<(&str, &ArcValue)> = Vec::new();
                                let mut existing_updates: Vec<(&str, &ArcValue)> = Vec::new();

                                for &(key, val) in &new_inserts {
                                    match container.find_in_collection(key, &self.dict)? {
                                        Some(_) => existing_updates.push((key, val)),
                                        None => truly_new.push((key, val)),
                                    }
                                }

                                if !truly_new.is_empty() {
                                    self.batch_insert_into_collection(
                                        src, &ancestors, &truly_new, stats,
                                    )
                                    .await?;
                                    stats.updates_applied += truly_new.len() as u32;
                                }

                                for (key, val) in existing_updates {
                                    path_so_far.push(key.to_string());
                                    let full_path: Vec<&str> =
                                        path_so_far.iter().map(|s| s.as_str()).collect();
                                    self.apply_single_update(src, &full_path, Some(val), stats)
                                        .await?;
                                    path_so_far.pop();
                                }
                            } else {
                                for (key, val) in new_inserts {
                                    path_so_far.push(key.to_string());
                                    let full_path: Vec<&str> =
                                        path_so_far.iter().map(|s| s.as_str()).collect();
                                    self.apply_single_update(src, &full_path, Some(val), stats)
                                        .await?;
                                    path_so_far.pop();
                                }
                            }
                        }

                        for (child_key, child_node) in other_children {
                            match child_node {
                                UpdateNode::Delete => {
                                    path_so_far.push(child_key.clone());
                                    let full_path: Vec<&str> =
                                        path_so_far.iter().map(|s| s.as_str()).collect();
                                    self.apply_single_update(src, &full_path, None, stats)
                                        .await?;
                                    path_so_far.pop();
                                }
                                UpdateNode::Merge(sub_children) => {
                                    path_so_far.push(child_key.clone());
                                    Box::pin(self.apply_tree(
                                        src,
                                        sub_children,
                                        path_so_far,
                                        stats,
                                    ))
                                    .await?;
                                    path_so_far.pop();
                                }
                                UpdateNode::Set(val) => {
                                    path_so_far.push(child_key.clone());
                                    let full_path: Vec<&str> =
                                        path_so_far.iter().map(|s| s.as_str()).collect();
                                    self.apply_single_update(src, &full_path, Some(val), stats)
                                        .await?;
                                    path_so_far.pop();
                                }
                            }
                        }
                    } else {
                        Box::pin(self.apply_tree(src, children, path_so_far, stats)).await?;
                    }

                    path_so_far.pop();
                }
            }
        }

        Ok(())
    }

    /// Apply a single update at a specific path.
    pub(crate) async fn apply_single_update(
        &mut self,
        src: &IO,
        path: &[&str],
        new_value: Option<&ArcValue>,
        stats: &mut IncrementalStats,
    ) -> Result<()> {
        if path.is_empty() {
            return Ok(());
        }

        let parent_path = &path[..path.len() - 1];
        let target_key = path[path.len() - 1];

        let (ancestors, depth_reached) = self.navigate_to_depth(parent_path).await?;
        let deepest_nav = ancestors.last().ok_or(BlobError::UnexpectedEof)?.clone();

        if depth_reached < parent_path.len() {
            // Semantics: SET-null is delete; deleting a path that
            // doesn't exist is a no-op. Both `None` (true Delete entry) and
            // `Some(Null)` (SET-with-null-value, which is what the WAL→blob
            // path produces for SET-null operations) must short-circuit here
            // — otherwise the "build wrapper" branch below would clobber the
            // primitive ancestor with `Object{leaf: Null}`, losing the
            // primitive's value (regression test:
            // incremental.rs::test_set_null_at_child_of_primitive_is_noop).
            let value = match new_value {
                Some(v) if !v.is_null() => v,
                _ => return Ok(()),
            };

            let missing_segments = &parent_path[depth_reached..];
            let insert_key = missing_segments[0];
            let remaining = &missing_segments[1..];

            let mut wrapped = value.clone();
            let mut map = std::collections::HashMap::new();
            map.insert(target_key.to_string(), wrapped);
            wrapped = ArcValue::Object(Arc::new(map));
            for &seg in remaining.iter().rev() {
                let mut map = std::collections::HashMap::new();
                map.insert(seg.to_string(), wrapped);
                wrapped = ArcValue::Object(Arc::new(map));
            }

            let hint = if deepest_nav.subtree_size > 0 {
                Some(deepest_nav.subtree_size)
            } else {
                None
            };
            let parsed = read_container(&self.io, deepest_nav.resolved_offset, hint).await?;
            let existing = parsed.find_in_collection(insert_key, &self.dict)?;

            if let Some((
                index_entry_abs_pos,
                existing_type_flags,
                existing_child_abs_offset,
                existing_child_size,
            )) = existing
            {
                if is_forwarded_flag(existing_type_flags) {
                    self.free_list
                        .free(existing_child_abs_offset, existing_child_size);
                }

                let (new_bytes, _) = self.serialize_value_to_bytes(&wrapped)?;
                let new_size = new_bytes.len() as u64;
                let new_abs_offset = self
                    .free_list
                    .write_or_append(&self.io, &new_bytes)
                    .await
                    .map_err(BlobError::Io)?;
                self.io.sync().await?;

                let type_flags_pos = index_entry_abs_pos + 8;
                let new_type_tag = new_bytes[0];
                let new_type_flags = make_type_flags(new_type_tag, true);
                let mut entry_buf = [0u8; 17];
                entry_buf[0] = new_type_flags;
                entry_buf[1..9].copy_from_slice(&new_abs_offset.to_le_bytes());
                entry_buf[9..17].copy_from_slice(&new_size.to_le_bytes());
                self.io.pwrite_deferred(type_flags_pos, &entry_buf).await?;

                stats.forward_updates += 1;
                stats.bytes_appended += new_size;
                self.bump_and_cascade(src, &ancestors, new_size as u32, stats)
                    .await?;
            } else {
                self.insert_into_collection(src, &ancestors, insert_key, &wrapped, stats)
                    .await?;
            }
            stats.updates_applied += 1;
            return Ok(());
        }

        // Full parent path exists — proceed with normal update/insert/delete logic
        let parent_nav = deepest_nav;

        let hint = if parent_nav.subtree_size > 0 {
            Some(parent_nav.subtree_size)
        } else {
            None
        };
        let container = read_container(&self.io, parent_nav.resolved_offset, hint).await?;
        let target_in_parent = container.find_in_collection(target_key, &self.dict)?;

        match (target_in_parent, new_value) {
            // Target exists and we're setting a new value
            (
                Some((index_entry_abs_pos, type_flags, child_abs_offset, child_size)),
                Some(value),
            ) => {
                let new_bytes = self.serialize_value_to_bytes(value)?.0;
                let new_size = new_bytes.len() as u64;
                let old_size = child_size;

                // In-place overwrite is only safe for non-container values (scalars,
                // strings, etc.). Containers have internal sub-headers that may be
                // cached by readers — overwriting them in-place would corrupt those
                // cached references. Containers always go through the forward-to-EOF path.
                let old_type_tag = extract_type_tag(type_flags);
                let old_is_container =
                    old_type_tag == TYPE_COLLECTION || old_type_tag == TYPE_ARRAY;
                if new_size <= old_size && !old_is_container {
                    self.io.pwrite(child_abs_offset, &new_bytes).await?;

                    let new_type_tag = new_bytes[0];
                    if new_type_tag != old_type_tag || new_size != old_size {
                        let type_flags_pos = index_entry_abs_pos + 8;
                        let updated_flags =
                            make_type_flags(new_type_tag, is_forwarded_flag(type_flags));
                        let mut entry_buf = [0u8; 17];
                        entry_buf[0] = updated_flags;
                        let offset_pos = type_flags_pos + 1;
                        let mut offset_buf = [0u8; 8];
                        self.io.pread_into(offset_pos, &mut offset_buf).await?;
                        entry_buf[1..9].copy_from_slice(&offset_buf);
                        entry_buf[9..17].copy_from_slice(&new_size.to_le_bytes());
                        self.io.pwrite(type_flags_pos, &entry_buf).await?;
                    }

                    stats.in_place_updates += 1;
                } else {
                    if is_forwarded_flag(type_flags) {
                        self.free_list.free(child_abs_offset, old_size);
                    }

                    let new_abs_offset = self
                        .free_list
                        .write_or_append(&self.io, &new_bytes)
                        .await
                        .map_err(BlobError::Io)?;
                    self.io.sync().await?;

                    let type_flags_pos = index_entry_abs_pos + 8;

                    let new_type_tag = new_bytes[0];
                    let new_type_flags = make_type_flags(new_type_tag, true);

                    let mut entry_buf = [0u8; 17];
                    entry_buf[0] = new_type_flags;
                    entry_buf[1..9].copy_from_slice(&new_abs_offset.to_le_bytes());
                    entry_buf[9..17].copy_from_slice(&new_size.to_le_bytes());
                    self.io.pwrite_deferred(type_flags_pos, &entry_buf).await?;

                    stats.forward_updates += 1;
                    stats.bytes_appended += new_size;
                    self.bump_and_cascade(src, &ancestors, new_size as u32, stats)
                        .await?;
                }
                stats.updates_applied += 1;
            }

            // Target exists and we're deleting — NULL tombstone
            (Some((index_entry_abs_pos, _type_flags, child_abs_offset, _child_size)), None) => {
                trace!(
                    offset = child_abs_offset,
                    parent_offset = parent_nav.resolved_offset,
                    key = target_key,
                    "collection child deleted (TYPE_NULL overwrite)"
                );
                self.io.pwrite(child_abs_offset, &[TYPE_NULL]).await?;
                let type_flags_pos = index_entry_abs_pos + 8;
                let null_flags = make_type_flags(TYPE_NULL, false);
                // Write type_flags + offset (unchanged) + size=0 to mark as tombstone
                // (size=0 distinguishes tombstones from genuine Null values which have size=1)
                let mut entry_buf = [0u8; 17];
                entry_buf[0] = null_flags;
                // Preserve the offset (unchanged)
                let offset_pos = type_flags_pos + 1;
                let mut offset_buf = [0u8; 8];
                self.io.pread_into(offset_pos, &mut offset_buf).await?;
                entry_buf[1..9].copy_from_slice(&offset_buf);
                // size = 0 (tombstone marker)
                entry_buf[9..17].copy_from_slice(&0u64.to_le_bytes());
                self.io.pwrite_deferred(type_flags_pos, &entry_buf).await?;
                self.io.sync().await?;
                stats.in_place_updates += 1;

                self.bump_and_cascade(src, &ancestors, _child_size as u32, stats)
                    .await?;
                stats.updates_applied += 1;
            }

            // Target doesn't exist and we're setting — insert into collection
            (None, Some(value)) => {
                self.insert_into_collection(src, &ancestors, target_key, value, stats)
                    .await?;
                stats.updates_applied += 1;
            }

            // Target doesn't exist and we're deleting -> no-op
            (None, None) => {}
        }

        Ok(())
    }

    /// Navigate as far as possible along a path. Returns (ancestors, depth_reached).
    pub(crate) async fn navigate_to_depth(
        &self,
        path: &[&str],
    ) -> Result<(Vec<TargetInfo>, usize)> {
        let mut current_offset = self.header.root_offset;
        let mut current_size_hint: Option<u64> = None;

        let root_info = TargetInfo {
            resolved_offset: current_offset,
            original_offset: current_offset,
            parent_index_entry_pos: None,
            parent_tag: None,
            subtree_size: 0,
            is_forwarded: false,
        };
        let mut ancestors = vec![root_info];
        let mut depth = 0;

        for &segment in path {
            let container = match read_container(&self.io, current_offset, current_size_hint).await
            {
                Ok(c) => c,
                Err(BlobError::NotAContainer(_, _)) => break,
                Err(e) => return Err(e),
            };

            let next_info: Option<(u64, u64, u64, u8, bool)> = match container.tag {
                TYPE_COLLECTION => {
                    match container.find_in_collection(segment, &self.dict)? {
                        Some((index_entry_pos, type_flags, child_abs_offset, child_size)) => {
                            let child_type = extract_type_tag(type_flags);
                            if child_type != TYPE_COLLECTION && child_type != TYPE_ARRAY {
                                // Non-container (null, string, number, bool) — can't navigate deeper.
                                None
                            } else {
                                Some((
                                    child_abs_offset,
                                    child_size,
                                    index_entry_pos,
                                    TYPE_COLLECTION,
                                    is_forwarded_flag(type_flags),
                                ))
                            }
                        }
                        None => None,
                    }
                }
                _ => None,
            };

            match next_info {
                Some((offset, size, parent_index_pos, parent_tag, is_fwd)) => {
                    current_offset = offset;
                    current_size_hint = Some(size);
                    ancestors.push(TargetInfo {
                        resolved_offset: current_offset,
                        original_offset: current_offset,
                        parent_index_entry_pos: Some(parent_index_pos),
                        parent_tag: Some(parent_tag),
                        subtree_size: size,
                        is_forwarded: is_fwd,
                    });
                    depth += 1;
                }
                None => break,
            }
        }

        Ok((ancestors, depth))
    }

    /// Insert a new child into a collection object.
    async fn insert_into_collection(
        &mut self,
        src: &IO,
        ancestors: &[TargetInfo],
        new_key: &str,
        new_value: &ArcValue,
        stats: &mut IncrementalStats,
    ) -> Result<()> {
        let parent = &ancestors[ancestors.len() - 1];
        let parent_offset = parent.resolved_offset;

        let hint = if parent.subtree_size > 0 {
            Some(parent.subtree_size)
        } else {
            None
        };
        let mut parsed = read_container(&self.io, parent.resolved_offset, hint).await?;

        let key_hash = hash_field_name(new_key);
        let new_key_entry_size = 2 + new_key.len() as u32;

        let can_insert_in_place = parsed.reserved_count > 0
            && parsed.key_data_used + new_key_entry_size <= parsed.key_data_reserved;

        if can_insert_in_place {
            let (new_child_bytes, _) = self.serialize_value_to_bytes(new_value)?;
            let new_child_abs_offset = self
                .free_list
                .write_or_append(&self.io, &new_child_bytes)
                .await
                .map_err(BlobError::Io)?;

            let child_type_tag = new_child_bytes[0];
            let type_flags = make_type_flags(child_type_tag, true);
            let new_child_size = new_child_bytes.len() as u64;

            parsed.insert_child(
                new_key,
                key_hash,
                type_flags,
                new_child_abs_offset,
                new_child_size,
                &self.dict,
            );

            // Track inline keys for future dictionary absorption
            if self.dict.lookup(new_key).is_none() && !crate::dictionary::is_collection_key(new_key)
            {
                self.pending_keys.insert(new_key.to_string());
            }

            let buf = parsed.to_bytes();

            self.io.sync().await?;
            self.io.pwrite_deferred(parent_offset, &buf).await?;

            self.bump_and_cascade(src, ancestors, new_child_bytes.len() as u32, stats)
                .await?;

            stats.collection_inserts += 1;
            stats.bytes_appended += new_child_bytes.len() as u64;
        } else {
            let (new_child_bytes, _) = self.serialize_value_to_bytes(new_value)?;

            let hint = if parent.subtree_size > 0 {
                Some(parent.subtree_size)
            } else {
                None
            };
            let parsed = read_container(&self.io, parent.resolved_offset, hint).await?;
            trace!(
                offset = parent_offset,
                child_count = parsed.child_count,
                reserved_count = parsed.reserved_count,
                key_data_used = parsed.key_data_used,
                key_data_reserved = parsed.key_data_reserved,
                key = new_key,
                "collection insert fallback: reserved space exhausted, structural copy"
            );
            let op = crate::compact::CompactOp::InsertCollection {
                key: new_key,
                value_bytes: &new_child_bytes,
            };
            if parent.is_forwarded {
                self.free_list
                    .free(parent.resolved_offset, parsed.subtree_size);
            }

            let (new_offset, copy_stats) = crate::compact::compact_container(
                src,
                &self.io,
                &parsed,
                self.field_id_size,
                &op,
                &mut self.free_list,
                &self.dict,
            )
            .await?;
            let buf_size = copy_stats.bytes_written;

            let grandparent_offset = if ancestors.len() > 1 {
                ancestors[ancestors.len() - 2].original_offset
            } else {
                self.header.root_offset
            };
            self.forward_via_parent_index(
                parent,
                new_offset,
                buf_size,
                TYPE_COLLECTION,
                grandparent_offset,
            )
            .await?;

            if ancestors.len() > 1 {
                self.bump_and_cascade(
                    src,
                    &ancestors[..ancestors.len() - 1],
                    buf_size as u32,
                    stats,
                )
                .await?;
            }

            stats.parent_rewrites += 1;
            stats.bytes_appended += buf_size;
        }

        Ok(())
    }

    /// Batch-insert multiple new children into a collection at once.
    pub(crate) async fn batch_insert_into_collection(
        &mut self,
        src: &IO,
        ancestors: &[TargetInfo],
        new_children: &[(&str, &ArcValue)],
        stats: &mut IncrementalStats,
    ) -> Result<()> {
        if new_children.is_empty() {
            return Ok(());
        }

        let parent = &ancestors[ancestors.len() - 1];
        let parent_offset = parent.resolved_offset;

        let hint = if parent.subtree_size > 0 {
            Some(parent.subtree_size)
        } else {
            None
        };
        let mut parsed = read_container(&self.io, parent.resolved_offset, hint).await?;

        struct SerializedChild<'a> {
            key: &'a str,
            key_hash: u64,
            bytes: Vec<u8>,
            abs_offset: u64,
        }
        let mut serialized: Vec<SerializedChild<'_>> = Vec::with_capacity(new_children.len());
        let mut total_new_bytes: u64 = 0;
        let mut items_since_yield: u32 = 0;

        for &(key, value) in new_children {
            let (child_bytes, _) = self.serialize_value_to_bytes(value)?;
            total_new_bytes += child_bytes.len() as u64;
            serialized.push(SerializedChild {
                key,
                key_hash: hash_field_name(key),
                bytes: child_bytes,
                abs_offset: 0,
            });
            items_since_yield += 1;
            if items_since_yield >= 256 {
                self.io.yield_point().await;
                items_since_yield = 0;
            }
        }

        let mut batch_buf = Vec::with_capacity(total_new_bytes as usize);
        let mut offsets = Vec::with_capacity(serialized.len());
        for sc in &serialized {
            offsets.push(batch_buf.len() as u64);
            batch_buf.extend_from_slice(&sc.bytes);
        }
        let batch_start = self
            .free_list
            .write_or_append(&self.io, &batch_buf)
            .await
            .map_err(BlobError::Io)?;
        for (i, sc) in serialized.iter_mut().enumerate() {
            sc.abs_offset = batch_start + offsets[i];
        }

        // Count existing forwarded children + the new batch (all forwarded).
        // Compact proactively if >50% will be forwarded after insertion.
        let existing_forwarded = {
            let mut fwd = 0u32;
            for i in 0..parsed.child_count as usize {
                let eo = i * COLLECTION_INDEX_ENTRY_SIZE;
                let tf = parsed.child_index[eo + 8];
                if is_forwarded_flag(tf) {
                    fwd += 1;
                }
            }
            fwd
        };
        let after_insert_forwarded = existing_forwarded + serialized.len() as u32;
        let after_insert_total = parsed.child_count + serialized.len() as u32;
        let should_compact = after_insert_total > 0
            && after_insert_forwarded * 2 > after_insert_total
            && total_new_bytes >= 4096;

        if should_compact {
            if parent.is_forwarded {
                self.free_list
                    .free(parent.resolved_offset, parsed.subtree_size);
            }

            let entries: Vec<(&str, &[u8])> = serialized
                .iter()
                .map(|sc| (sc.key, sc.bytes.as_slice()))
                .collect();
            let op = crate::compact::CompactOp::InsertCollectionBatch { entries: &entries };
            let (new_offset, copy_stats) = crate::compact::compact_container(
                src,
                &self.io,
                &parsed,
                self.field_id_size,
                &op,
                &mut self.free_list,
                &self.dict,
            )
            .await?;
            let buf_size = copy_stats.bytes_written;

            let grandparent_offset = if ancestors.len() > 1 {
                ancestors[ancestors.len() - 2].original_offset
            } else {
                self.header.root_offset
            };
            self.forward_via_parent_index(
                parent,
                new_offset,
                buf_size,
                TYPE_COLLECTION,
                grandparent_offset,
            )
            .await?;

            if ancestors.len() > 1 {
                self.bump_and_cascade(
                    src,
                    &ancestors[..ancestors.len() - 1],
                    buf_size as u32,
                    stats,
                )
                .await?;
            }

            stats.parent_rewrites += 1;
            stats.bytes_appended += buf_size;
            stats.collection_inserts += new_children.len() as u32;
            return Ok(());
        }

        let total_key_data: u32 = serialized.iter().map(|sc| 2 + sc.key.len() as u32).sum();
        let can_insert_in_place = parsed.reserved_count >= serialized.len() as u32
            && parsed.key_data_used + total_key_data <= parsed.key_data_reserved;

        if can_insert_in_place {
            for sc in &serialized {
                let child_type_tag = sc.bytes[0];
                let type_flags = make_type_flags(child_type_tag, true);
                let child_size = sc.bytes.len() as u64;

                parsed.insert_child(
                    sc.key,
                    sc.key_hash,
                    type_flags,
                    sc.abs_offset,
                    child_size,
                    &self.dict,
                );

                // Track inline keys for future dictionary absorption
                if self.dict.lookup(sc.key).is_none()
                    && !crate::dictionary::is_collection_key(sc.key)
                {
                    self.pending_keys.insert(sc.key.to_string());
                }
            }

            let buf = parsed.to_bytes();
            self.io.sync().await?;
            self.io.pwrite_deferred(parent_offset, &buf).await?;

            self.bump_and_cascade(src, ancestors, total_new_bytes as u32, stats)
                .await?;

            stats.collection_inserts += new_children.len() as u32;
            stats.bytes_appended += total_new_bytes;
        } else {
            let entries: Vec<(&str, &[u8])> = serialized
                .iter()
                .map(|sc| (sc.key, sc.bytes.as_slice()))
                .collect();

            let hint = if parent.subtree_size > 0 {
                Some(parent.subtree_size)
            } else {
                None
            };
            let parsed = read_container(&self.io, parent.resolved_offset, hint).await?;

            if parent.is_forwarded {
                self.free_list
                    .free(parent.resolved_offset, parsed.subtree_size);
            }

            let op = crate::compact::CompactOp::InsertCollectionBatch { entries: &entries };
            let (new_offset, copy_stats) = crate::compact::compact_container(
                src,
                &self.io,
                &parsed,
                self.field_id_size,
                &op,
                &mut self.free_list,
                &self.dict,
            )
            .await?;
            let buf_size = copy_stats.bytes_written;

            let grandparent_offset = if ancestors.len() > 1 {
                ancestors[ancestors.len() - 2].original_offset
            } else {
                self.header.root_offset
            };
            self.forward_via_parent_index(
                parent,
                new_offset,
                buf_size,
                TYPE_COLLECTION,
                grandparent_offset,
            )
            .await?;

            if ancestors.len() > 1 {
                self.bump_and_cascade(
                    src,
                    &ancestors[..ancestors.len() - 1],
                    buf_size as u32,
                    stats,
                )
                .await?;
            }

            stats.parent_rewrites += 1;
            stats.bytes_appended += buf_size;
            stats.collection_inserts += new_children.len() as u32;
        }

        Ok(())
    }

    /// Update the parent's index entry to forward to a new location.
    pub(crate) async fn forward_via_parent_index(
        &mut self,
        target: &TargetInfo,
        new_offset: u64,
        new_size: u64,
        new_type_tag: u8,
        _parent_offset: u64,
    ) -> Result<()> {
        self.io.sync().await?;

        if target.parent_index_entry_pos.is_none() {
            self.io
                .pwrite_deferred(16, &new_offset.to_le_bytes())
                .await?;
            self.header.root_offset = new_offset;
            return Ok(());
        }

        let parent_index_pos = target.parent_index_entry_pos.unwrap();
        let _parent_tag = target
            .parent_tag
            .ok_or_else(|| BlobError::InternalError("parent_tag not set".to_string()))?;

        let type_flags_pos = parent_index_pos + 8;

        let new_type_flags = make_type_flags(new_type_tag, true);

        let mut entry_buf = [0u8; 17];
        entry_buf[0] = new_type_flags;
        entry_buf[1..9].copy_from_slice(&new_offset.to_le_bytes());
        entry_buf[9..17].copy_from_slice(&new_size.to_le_bytes());
        self.io.pwrite_deferred(type_flags_pos, &entry_buf).await?;

        Ok(())
    }

    /// Check if a container's fragmentation exceeds the 50% threshold.
    /// If so, compact it to EOF and update the parent's index entry.
    pub(crate) async fn check_and_compact_container(
        &mut self,
        src: &IO,
        container: &TargetInfo,
        parent_offset: u64,
    ) -> Result<Option<u64>> {
        let hint = if container.subtree_size > 0 {
            Some(container.subtree_size)
        } else {
            None
        };
        let parsed = match read_container(&self.io, container.resolved_offset, hint).await {
            Ok(c) => c,
            Err(BlobError::NotAContainer(_, _)) => return Ok(None),
            Err(e) => return Err(e),
        };

        let tag = parsed.tag;
        if tag != TYPE_COLLECTION {
            return Ok(None);
        }

        // Scan the child index: count forwarded children and sum all child sizes.
        let (forwarded_count, child_count, total_children_size) = {
            let mut fwd = 0u32;
            let mut total_size = 0u64;
            for i in 0..parsed.child_count as usize {
                let eo = i * COLLECTION_INDEX_ENTRY_SIZE;
                let tf = parsed.child_index[eo + 8];
                let sz =
                    u64::from_le_bytes(parsed.child_index[eo + 17..eo + 25].try_into().unwrap());
                total_size += sz;
                if is_forwarded_flag(tf) {
                    fwd += 1;
                }
            }
            (fwd, parsed.child_count, total_size)
        };

        // Compact when >50% of children are forwarded.
        let fragmented = child_count > 0 && forwarded_count * 2 > child_count;

        if !fragmented {
            return Ok(None);
        }

        const MIN_COMPACT_BYTES: u64 = 4096;
        if total_children_size < MIN_COMPACT_BYTES {
            return Ok(None);
        }

        if container.is_forwarded {
            self.free_list
                .free(container.resolved_offset, parsed.subtree_size);
        } else {
            self.free_list.waste(parsed.subtree_size);
        }

        let (new_offset, copy_stats) = crate::compact::compact_container(
            src,
            &self.io,
            &parsed,
            self.field_id_size,
            &crate::compact::CompactOp::Defrag,
            &mut self.free_list,
            &self.dict,
        )
        .await?;

        self.forward_via_parent_index(
            container,
            new_offset,
            copy_stats.bytes_written,
            tag,
            parent_offset,
        )
        .await?;

        Ok(Some(copy_stats.bytes_written))
    }

    /// Cascade compaction checks up the ancestor chain.
    ///
    /// Walks ancestors from deepest to shallowest (skipping the outermost,
    /// which is the root). At each level, checks if the container exceeds
    /// compaction thresholds. If compaction fires, the cascade continues
    /// upward with the compacted size as the new delta.
    pub(crate) async fn bump_and_cascade(
        &mut self,
        src: &IO,
        ancestors: &[TargetInfo],
        _delta: u32,
        _stats: &mut IncrementalStats,
    ) -> Result<()> {
        let mut remaining = ancestors;

        while remaining.len() > 1 {
            let container = &remaining[remaining.len() - 1];
            let parent_offset = remaining[remaining.len() - 2].original_offset;
            self.check_and_compact_container(src, container, parent_offset)
                .await?;
            remaining = &remaining[..remaining.len() - 1];
        }

        Ok(())
    }
}
