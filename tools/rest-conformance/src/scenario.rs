//! A fixed scenario for what random scripts rarely line up.

use crate::script::bulk_compact;
use crate::stack::Stack;
use serde_json::{json, Map, Value};

const COLLECTIONS: usize = 20;
const CHILDREN: usize = 30;

fn put(stack: &Stack, url: &str, value: &Value) -> Result<(), String> {
    let response = stack.request("PUT", url, &[], Some(value.to_string().as_bytes()))?;
    if response.status != 200 {
        return Err(format!(
            "PUT {url}: {} {}",
            response.status,
            response.text()
        ));
    }
    Ok(())
}

fn check(stack: &Stack, wanted: &Value, when: &str) -> Result<(), String> {
    let response = stack.request("GET", "/live-compact.json", &[], None)?;
    if response.status != 200 || response.json()? != *wanted {
        return Err(format!(
            "{when}: data differs ({} bytes)",
            response.body.len()
        ));
    }
    Ok(())
}

/// A full compaction with the server running moves everything in the blob, while
/// the database still has the old layout in its cache. The next storage worker
/// batch has to start from the new one.
pub fn live_compaction(stack: &mut Stack) -> Result<(), String> {
    let failures_before = stack.compaction_failures();

    // Put collections in the blob, with dead space, so compaction moves them.
    let mut wanted = Map::new();
    for c in 0..COLLECTIONS {
        let mut children = Map::new();
        for k in 0..CHILDREN {
            children.insert(
                format!("k{k}"),
                json!({"v": format!("{c}-{k}"), "padding": "y".repeat(200)}),
            );
        }
        let value = Value::Object(children);
        put(stack, &format!("/live-compact/c{c}.json"), &value)?;
        wanted.insert(format!("c{c}"), value);
    }
    bulk_compact(stack)?;
    for (c, children) in wanted.iter_mut() {
        for k in (0..CHILDREN).step_by(2) {
            let url = format!("/live-compact/{c}/k{k}.json");
            stack.request("DELETE", &url, &[], None)?;
            children.as_object_mut().unwrap().remove(&format!("k{k}"));
        }
    }
    bulk_compact(stack)?;

    // A new database and worker. Reading child by child fills the database's cache
    // with this blob's container headers.
    stack.restart()?;
    for (c, children) in &wanted {
        for k in children.as_object().unwrap().keys() {
            stack.request("GET", &format!("/live-compact/{c}/{k}.json"), &[], None)?;
        }
    }

    stack.live_compact()?;

    for (c, children) in wanted.iter_mut() {
        for k in 100..110 {
            let value = json!({"v": format!("new-{k}")});
            put(stack, &format!("/live-compact/{c}/k{k}.json"), &value)?;
            children
                .as_object_mut()
                .unwrap()
                .insert(format!("k{k}"), value);
        }
    }
    bulk_compact(stack)?;

    let wanted = Value::Object(wanted);
    check(stack, &wanted, "after the worker's batch")?;
    stack.evict()?;
    check(stack, &wanted, "after eviction")?;
    stack.restart()?;
    check(stack, &wanted, "after restart")?;

    let failures = stack.compaction_failures() - failures_before;
    if failures > 0 {
        return Err(format!("{failures} storage worker batches failed"));
    }
    Ok(())
}
