//! Quick blob inspector: shows top-level structure with sizes.
//!
//! Usage: inspect <blob.lark> [path]
//! Examples:
//!   inspect ~/Downloads/blob.lark           # show root children
//!   inspect ~/Downloads/blob.lark /burst    # show children of /burst

use lark_blob::io::{BlobIO, StdBlobIO};
use lark_blob::session::BlobSession;

/// Minimal single-poll block_on for sync-returning async fns (StdBlobIO).
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    fn noop_raw_waker() -> RawWaker {
        fn no_op(_: *const ()) {}
        fn clone(_: *const ()) -> RawWaker {
            noop_raw_waker()
        }
        RawWaker::new(
            std::ptr::null(),
            &RawWakerVTable::new(clone, no_op, no_op, no_op),
        )
    }
    let waker = unsafe { Waker::from_raw(noop_raw_waker()) };
    let mut cx = Context::from_waker(&waker);
    let mut fut = std::pin::pin!(fut);
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(val) => val,
        Poll::Pending => panic!("block_on: unexpected Pending from sync BlobIO"),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: inspect <blob.lark> [path]");
        std::process::exit(1);
    }

    let path = std::path::Path::new(&args[1]);
    let sub_path = args.get(2);

    block_on(async {
        let io = StdBlobIO::open(path).expect("open blob");
        let file_size = io.size().await.expect("size");
        let session = BlobSession::open(io).await.expect("open session");

        let header = session.header();
        println!("=== Blob Header ===");
        println!(
            "  file size:       {} ({:.2} GB)",
            file_size,
            file_size as f64 / (1024.0 * 1024.0 * 1024.0)
        );
        println!("  root_offset:     {}", header.root_offset);
        println!("  dict_offset:     {}", header.dict_offset);
        println!("  dict_fields:     {}", header.dict_field_count);
        println!();

        let path_parts: Vec<&str> = match &sub_path {
            Some(p) => p.split('/').filter(|s| !s.is_empty()).collect(),
            None => vec![],
        };

        inspect_path(&session, &path_parts).await;

        // If --read-subtree flag, try read_subtree instead of read_shallow
        if std::env::args().any(|a| a == "--read-subtree") {
            println!();
            println!("=== read_subtree test ===");
            let refs: Vec<&str> = path_parts.iter().map(|s| &**s).collect();
            match session.read_subtree(&refs).await {
                Ok(val) => {
                    let s = format!("{:?}", val);
                    if s.len() > 500 {
                        println!(
                            "OK — got ArcValue, debug repr {}... ({} chars total)",
                            &s[..500],
                            s.len()
                        );
                    } else {
                        println!("OK — {:?}", val);
                    }
                }
                Err(e) => println!("FAILED — {:?}", e),
            }
        }

        // If at root, also drill one level into each container child
        if path_parts.is_empty()
            && let Ok(lark_blob::ShallowValue::Children(children)) = session.read_shallow(&[]).await
        {
            for child in &children {
                if child.value.is_none() {
                    // It's a container — drill in
                    println!();
                    let child_path = [child.key.as_str()];
                    inspect_path(&session, &child_path).await;
                }
            }
        }
    });
}

async fn inspect_path<IO: BlobIO>(session: &BlobSession<IO>, path_parts: &[&str]) {
    match session.read_shallow(path_parts).await {
        Ok(lark_blob::ShallowValue::Primitive(v)) => {
            println!("Value at /{}:", path_parts.join("/"));
            println!("  {:?}", v);
        }
        Ok(lark_blob::ShallowValue::Children(children)) => {
            let display_path = if path_parts.is_empty() {
                "/".to_string()
            } else {
                format!("/{}/", path_parts.join("/"))
            };
            println!(
                "=== Children at {} ({} children) ===",
                display_path,
                children.len()
            );
            println!(
                "{:<50} {:>14}  {:>12}  TYPE",
                "KEY", "SIZE (bytes)", "HUMAN"
            );
            println!("{}", "-".repeat(95));

            let mut sorted: Vec<_> = children.iter().collect();
            sorted.sort_by_key(|c| std::cmp::Reverse(c.size));

            for child in &sorted {
                let human = human_size(child.size);
                let typ = match &child.value {
                    Some(v) => {
                        let s = format!("{:?}", v);
                        if s.len() > 40 {
                            format!("{}...", &s[..40])
                        } else {
                            s
                        }
                    }
                    None => "container".to_string(),
                };
                println!(
                    "{:<50} {:>14}  {:>12}  {}",
                    child.key, child.size, human, typ
                );
            }

            let total: u64 = children.iter().map(|c| c.size).sum();
            println!("{}", "-".repeat(95));
            println!(
                "{:<50} {:>14}  {:>12}",
                "TOTAL (children)",
                total,
                human_size(total)
            );
        }
        Err(e) => {
            eprintln!("Error reading /{}: {:?}", path_parts.join("/"), e);
        }
    }
}

fn human_size(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}
