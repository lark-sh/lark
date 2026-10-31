//! Fixed checks of the behavior in Firebase's public REST documentation.
//!
//! Each check cites the page it comes from. Where the docs don't commit to
//! something, the check doesn't either.

use crate::script::parse;
use crate::stack::{Response, Stack};
use serde_json::{json, Value};
use std::cell::Cell;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

const REF: &str = "https://firebase.google.com/docs/reference/rest/database";
const SAVE: &str = "https://firebase.google.com/docs/database/rest/save-data";
const RETRIEVE: &str = "https://firebase.google.com/docs/database/rest/retrieve-data";
const STRUCTURE: &str = "https://firebase.google.com/docs/database/rest/structure-data";
const LIMITS: &str = "https://firebase.google.com/docs/database/usage/limits";

/// Checks that fail today. Remove a line when its check starts passing: the run
/// fails until you do, so this list can't go stale.
const KNOWN_DIFFERENCES: &[&str] = &[
    // ETags are only returned by GET, and if-match isn't refused where it doesn't apply.
    "GET with if-match is rejected",
    "PUT with X-Firebase-ETag returns its ETag",
    "POST with X-Firebase-ETag returns an ETag",
    "POST with if-match is rejected",
    "PATCH with X-Firebase-ETag is rejected",
    "PATCH with if-match is rejected",
    "DELETE with X-Firebase-ETag returns null's ETag",
    "null_etag writes to an empty location",
];

type Outcome = Result<(), String>;

struct Spec<'a> {
    stack: &'a Stack,
    next: Cell<u32>,
    passed: Cell<u32>,
    known: Cell<u32>,
    failures: std::cell::RefCell<Vec<String>>,
}

fn ensure(condition: bool, message: impl FnOnce() -> String) -> Outcome {
    if condition {
        Ok(())
    } else {
        Err(message())
    }
}

fn show(response: &Response) -> String {
    format!("HTTP {}: {}", response.status, response.text())
}

fn status(response: &Response, wanted: u16) -> Outcome {
    ensure(response.status == wanted, || {
        format!("wanted HTTP {wanted}, got {}", show(response))
    })
}

fn body(response: &Response, wanted: &Value) -> Outcome {
    let actual = parse(response, 200)?;
    ensure(actual == *wanted, || {
        format!("wanted {wanted}, got {actual}")
    })
}

/// A path of its own for each check.
struct Node<'a> {
    stack: &'a Stack,
    path: String,
}

impl Node<'_> {
    fn url(&self, suffix: &str) -> String {
        format!("/spec/{}{suffix}", self.path)
    }

    fn get(&self, suffix: &str) -> Result<Response, String> {
        self.stack.request("GET", &self.url(suffix), &[], None)
    }

    fn get_with(&self, suffix: &str, headers: &[(&str, &str)]) -> Result<Response, String> {
        self.stack.request("GET", &self.url(suffix), headers, None)
    }

    fn send(&self, method: &str, suffix: &str, data: &str) -> Result<Response, String> {
        self.stack
            .request(method, &self.url(suffix), &[], Some(data.as_bytes()))
    }

    fn send_with(
        &self,
        method: &str,
        suffix: &str,
        headers: &[(&str, &str)],
        data: &str,
    ) -> Result<Response, String> {
        self.stack
            .request(method, &self.url(suffix), headers, Some(data.as_bytes()))
    }

    fn put(&self, data: &str) -> Result<(), String> {
        status(&self.send("PUT", ".json", data)?, 200)
    }

    fn etag(&self, suffix: &str) -> Result<String, String> {
        let response = self.get_with(suffix, &[("X-Firebase-ETag", "true")])?;
        response
            .header("etag")
            .map(str::to_string)
            .ok_or_else(|| format!("no ETag in {}", show(&response)))
    }
}

impl<'a> Spec<'a> {
    fn check(&self, name: &str, doc: &str, f: impl FnOnce(Node<'a>) -> Outcome) {
        let n = self.next.get() + 1;
        self.next.set(n);
        let node = Node {
            stack: self.stack,
            path: format!("n{n}"),
        };
        let known = KNOWN_DIFFERENCES.contains(&name);
        match f(node) {
            Ok(()) if known => self.failures.borrow_mut().push(format!(
                "{name}\n    passes now, so remove it from KNOWN_DIFFERENCES"
            )),
            Ok(()) => self.passed.set(self.passed.get() + 1),
            Err(_) if known => self.known.set(self.known.get() + 1),
            Err(e) => self
                .failures
                .borrow_mut()
                .push(format!("{name}\n    {e}\n    {doc}")),
        }
    }
}

fn methods(s: &Spec) {
    s.check("GET returns the data", REF, |n| {
        n.put(r#"{"first":"Jack","last":"Sparrow"}"#)?;
        body(
            &n.get(".json")?,
            &json!({"first": "Jack", "last": "Sparrow"}),
        )
    });
    s.check("GET of a missing location returns null", RETRIEVE, |n| {
        let response = n.get(".json")?;
        status(&response, 200)?;
        ensure(response.body == b"null", || show(&response))
    });
    s.check("PUT responds with the data", REF, |n| {
        body(&n.send("PUT", ".json", r#"{"a":1}"#)?, &json!({"a": 1}))
    });
    s.check("PUT replaces child nodes", SAVE, |n| {
        n.put(r#"{"a":1,"b":2}"#)?;
        n.put(r#"{"c":3}"#)?;
        body(&n.get(".json")?, &json!({"c": 3}))
    });
    s.check("POST responds with the new child's name", REF, |n| {
        let response = n.send("POST", ".json", r#"{"user":"jack"}"#)?;
        let name = parse(&response, 200)?["name"]
            .as_str()
            .ok_or_else(|| show(&response))?
            .to_string();
        body(&n.get(&format!("/{name}.json"))?, &json!({"user": "jack"}))
    });
    s.check("PATCH keeps omitted children", REF, |n| {
        n.put(r#"{"first":"Jack","last":"Sparrow"}"#)?;
        body(
            &n.send("PATCH", ".json", r#"{"last":"Jones"}"#)?,
            &json!({"last": "Jones"}),
        )?;
        body(&n.get(".json")?, &json!({"first": "Jack", "last": "Jones"}))
    });
    s.check(
        "PATCH with a nested object replaces that child",
        SAVE,
        |n| {
            n.put(r#"{"users":{"a":{"x":1,"y":2}}}"#)?;
            n.send("PATCH", ".json", r#"{"users":{"a":{"x":9}}}"#)?;
            body(&n.get(".json")?, &json!({"users": {"a": {"x": 9}}}))
        },
    );
    s.check("PATCH with slash keys writes each path", SAVE, |n| {
        n.put(r#"{"a":{"x":1,"y":2},"b":{"x":1,"y":2}}"#)?;
        n.send("PATCH", ".json", r#"{"a/x":9,"b/y":8}"#)?;
        body(
            &n.get(".json")?,
            &json!({"a": {"x": 9, "y": 2}, "b": {"x": 1, "y": 8}}),
        )
    });
    s.check("DELETE responds with null", REF, |n| {
        n.put("1")?;
        let response = n.stack.request("DELETE", &n.url(".json"), &[], None)?;
        status(&response, 200)?;
        ensure(response.body == b"null", || show(&response))?;
        body(&n.get(".json")?, &Value::Null)
    });
    s.check("Method override header", REF, |n| {
        n.put("1")?;
        let headers = [("X-HTTP-Method-Override", "DELETE")];
        status(&n.send_with("POST", ".json", &headers, "")?, 200)?;
        body(&n.get(".json")?, &Value::Null)
    });
    s.check("Method override query parameter", REF, |n| {
        n.put("1")?;
        status(
            &n.send("POST", ".json?x-http-method-override=DELETE", "")?,
            200,
        )?;
        body(&n.get(".json")?, &Value::Null)
    });
}

fn conditional(s: &Spec) {
    let want_etag = [("X-Firebase-ETag", "true")];
    s.check("GET with X-Firebase-ETag returns an ETag", REF, |n| {
        n.put("1")?;
        n.etag(".json").map(|_| ())
    });
    s.check("ETag changes with the data", REF, |n| {
        n.put("1")?;
        let before = n.etag(".json")?;
        n.put("2")?;
        ensure(before != n.etag(".json")?, || "ETag unchanged".to_string())
    });
    s.check("GET with if-match is rejected", REF, |n| {
        status(&n.get_with(".json", &[("if-match", "x")])?, 400)
    });
    s.check("PUT with X-Firebase-ETag returns its ETag", REF, |n| {
        let response = n.send_with("PUT", ".json", &want_etag, "1")?;
        status(&response, 200)?;
        let etag = response.header("etag").ok_or_else(|| show(&response))?;
        ensure(etag == n.etag(".json")?, || format!("ETag {etag}"))
    });
    s.check("POST with X-Firebase-ETag returns an ETag", REF, |n| {
        let response = n.send_with("POST", ".json", &want_etag, "1")?;
        status(&response, 200)?;
        ensure(response.header("etag").is_some(), || show(&response))
    });
    s.check("POST with if-match is rejected", REF, |n| {
        status(
            &n.send_with("POST", ".json", &[("if-match", "x")], "1")?,
            400,
        )
    });
    s.check("PATCH with X-Firebase-ETag is rejected", REF, |n| {
        status(
            &n.send_with("PATCH", ".json", &want_etag, r#"{"a":1}"#)?,
            400,
        )
    });
    s.check("PATCH with if-match is rejected", REF, |n| {
        status(
            &n.send_with("PATCH", ".json", &[("if-match", "x")], r#"{"a":1}"#)?,
            400,
        )
    });
    s.check(
        "DELETE with X-Firebase-ETag returns null's ETag",
        REF,
        |n| {
            n.put("1")?;
            let response = n.send_with("DELETE", ".json", &want_etag, "")?;
            body(&response, &Value::Null)?;
            let etag = response.header("etag").ok_or_else(|| show(&response))?;
            ensure(etag == n.etag(".json")?, || format!("ETag {etag}"))
        },
    );
    s.check("PUT with a matching if-match writes", SAVE, |n| {
        n.put("1")?;
        let etag = n.etag(".json")?;
        body(
            &n.send_with("PUT", ".json", &[("if-match", &etag)], "2")?,
            &json!(2),
        )?;
        body(&n.get(".json")?, &json!(2))
    });
    s.check(
        "PUT with a stale if-match is a 412 and doesn't write",
        SAVE,
        |n| {
            n.put("1")?;
            let etag = n.etag(".json")?;
            let response = n.send_with("PUT", ".json", &[("if-match", "stale")], "2")?;
            status(&response, 412)?;
            ensure(response.header("etag") == Some(&etag), || {
                format!("ETag {:?}", response.header("etag"))
            })?;
            ensure(response.json()? == json!(1), || show(&response))?;
            body(&n.get(".json")?, &json!(1))
        },
    );
    s.check("DELETE with a matching if-match deletes", REF, |n| {
        n.put("1")?;
        let etag = n.etag(".json")?;
        status(
            &n.send_with("DELETE", ".json", &[("if-match", &etag)], "")?,
            200,
        )?;
        body(&n.get(".json")?, &Value::Null)
    });
    s.check("DELETE with a stale if-match is a 412", REF, |n| {
        n.put("1")?;
        status(
            &n.send_with("DELETE", ".json", &[("if-match", "stale")], "")?,
            412,
        )?;
        body(&n.get(".json")?, &json!(1))
    });
    s.check("null_etag writes to an empty location", REF, |n| {
        status(
            &n.send_with("PUT", ".json", &[("if-match", "null_etag")], "1")?,
            200,
        )?;
        body(&n.get(".json")?, &json!(1))
    });
    s.check("null_etag fails on a location with data", REF, |n| {
        n.put("1")?;
        status(
            &n.send_with("PUT", ".json", &[("if-match", "null_etag")], "2")?,
            412,
        )
    });
}

fn parameters(s: &Spec) {
    s.check("shallow returns a primitive as it is", REF, |n| {
        n.put(r#""text""#)?;
        body(&n.get(".json?shallow=true")?, &json!("text"))
    });
    s.check("shallow truncates object values to true", REF, |n| {
        n.put(r#"{"a":{"b":1},"c":2,"d":"x"}"#)?;
        body(
            &n.get(".json?shallow=true")?,
            &json!({"a": true, "c": true, "d": true}),
        )
    });
    s.check(
        "shallow can't be mixed with filtering parameters",
        REF,
        |n| status(&n.get(r#".json?shallow=true&orderBy="$key""#)?, 400),
    );
    s.check("print=pretty works on GET", REF, |n| {
        n.put(r#"{"a":1}"#)?;
        let response = n.get(".json?print=pretty")?;
        body(&response, &json!({"a": 1}))?;
        ensure(response.text().contains('\n'), || show(&response))
    });
    for method in ["PUT", "PATCH", "POST"] {
        s.check(&format!("print=pretty works on {method}"), REF, |n| {
            let response = n.send(method, ".json?print=pretty", r#"{"a":1}"#)?;
            status(&response, 200)?;
            ensure(response.text().contains('\n'), || show(&response))
        });
        s.check(
            &format!("print=silent on {method} is an empty 204"),
            REF,
            |n| {
                let response = n.send(method, ".json?print=silent", r#"{"a":1}"#)?;
                status(&response, 204)?;
                ensure(response.body.is_empty(), || show(&response))
            },
        );
    }
    s.check("print=pretty works on DELETE", REF, |n| {
        let response = n
            .stack
            .request("DELETE", &n.url(".json?print=pretty"), &[], None)?;
        body(&response, &Value::Null)
    });
    s.check("print=silent on GET is an empty 204", REF, |n| {
        n.put("1")?;
        let response = n.get(".json?print=silent")?;
        status(&response, 204)?;
        ensure(response.body.is_empty(), || show(&response))
    });
    s.check("callback wraps the response", REF, |n| {
        n.put(r#"{"a":1}"#)?;
        let response = n.get(".json?callback=gotData")?;
        status(&response, 200)?;
        let text = response.text();
        let inner = text
            .trim()
            .trim_end_matches(';')
            .strip_prefix("gotData(")
            .and_then(|t| t.strip_suffix(')'))
            .ok_or_else(|| show(&response))?;
        let inner: Value = serde_json::from_str(inner).map_err(|e| e.to_string())?;
        ensure(inner == json!({"a": 1}), || show(&response))
    });
    s.check("callback is rejected on writes", REF, |n| {
        status(&n.send("PUT", ".json?callback=gotData", "1")?, 400)
    });
    s.check("download sets an attachment header", REF, |n| {
        n.put("1")?;
        let response = n.get(".json?download=my.txt")?;
        let header = response.header("content-disposition").unwrap_or("");
        ensure(
            header.contains("attachment") && header.contains("my.txt"),
            || format!("Content-Disposition {header:?}"),
        )
    });
    s.check("download is rejected on writes", REF, |n| {
        status(&n.send("PUT", ".json?download=my.txt", "1")?, 400)
    });
    s.check("timeout accepts ms, s and min", REF, |n| {
        for value in ["300ms", "3s", "3min", "15min"] {
            status(&n.get(&format!(".json?timeout={value}"))?, 200)?;
        }
        Ok(())
    });
    s.check("timeout above 15min is rejected", REF, |n| {
        status(&n.get(".json?timeout=16min")?, 400)
    });
    s.check("timeout that isn't positive is rejected", REF, |n| {
        status(&n.get(".json?timeout=0s")?, 400)
    });
    s.check("writeSizeLimit is accepted", REF, |n| {
        for value in ["tiny", "small", "medium", "large", "unlimited"] {
            status(
                &n.send("PUT", &format!(".json?writeSizeLimit={value}"), "1")?,
                200,
            )?;
        }
        Ok(())
    });
}

fn queries(s: &Spec) {
    let dinosaurs = r#"{
        "bruhathkayosaurus":{"height":25,"length":44},
        "lambeosaurus":{"height":2.1,"length":12.5},
        "linhenykus":{"height":0.6,"length":1},
        "pterodactyl":{"height":0.6,"length":1.2},
        "stegosaurus":{"height":4,"length":9}
    }"#;
    s.check("orderBy $key with limitToFirst", RETRIEVE, |n| {
        n.put(r#"{"b":1,"a":2,"10":3,"9":4}"#)?;
        let actual = parse(&n.get(r#".json?orderBy="$key"&limitToFirst=2"#)?, 200)?;
        ensure(actual == json!({"9": 4, "10": 3}), || actual.to_string())
    });
    s.check("orderBy $value sorts by type, then value", RETRIEVE, |n| {
        n.put(r#"{"a":"s","b":3,"c":true,"d":false,"e":{"x":1}}"#)?;
        let actual = parse(&n.get(r#".json?orderBy="$value"&limitToFirst=3"#)?, 200)?;
        ensure(actual == json!({"d": false, "c": true, "b": 3}), || {
            actual.to_string()
        })
    });
    s.check(
        "orderBy child, missing values first, ties by key",
        RETRIEVE,
        |n| {
            n.put(r#"{"a":{"h":2},"b":{"h":1},"c":{"h":1},"d":{"x":1}}"#)?;
            let actual = parse(&n.get(r#".json?orderBy="h"&limitToFirst=2"#)?, 200)?;
            ensure(actual == json!({"d": {"x": 1}, "b": {"h": 1}}), || {
                actual.to_string()
            })
        },
    );
    s.check("orderBy deep child path", RETRIEVE, |n| {
        n.put(r#"{"a":{"d":{"h":2}},"b":{"d":{"h":1}}}"#)?;
        let actual = parse(&n.get(r#".json?orderBy="d/h"&limitToFirst=1"#)?, 200)?;
        ensure(actual == json!({"b": {"d": {"h": 1}}}), || {
            actual.to_string()
        })
    });
    s.check("orderBy $priority", RETRIEVE, |n| {
        n.put(r#"{"a":{".value":1,".priority":2},"b":{".value":2,".priority":1},"c":3}"#)?;
        let actual = parse(&n.get(r#".json?orderBy="$priority"&limitToFirst=2"#)?, 200)?;
        ensure(actual == json!({"c": 3, "b": 2}), || actual.to_string())
    });
    s.check("limitToLast", RETRIEVE, |n| {
        n.put(dinosaurs)?;
        let actual = parse(&n.get(r#".json?orderBy="height"&limitToLast=1"#)?, 200)?;
        ensure(
            actual
                .as_object()
                .is_some_and(|m| m.contains_key("bruhathkayosaurus")),
            || actual.to_string(),
        )
    });
    s.check("startAt and endAt are inclusive", RETRIEVE, |n| {
        n.put(dinosaurs)?;
        let url = r#".json?orderBy="$key"&startAt="linhenykus"&endAt="pterodactyl""#;
        let actual = parse(&n.get(url)?, 200)?;
        let keys: Vec<&String> = actual
            .as_object()
            .ok_or_else(|| actual.to_string())?
            .keys()
            .collect();
        ensure(keys.len() == 2, || actual.to_string())
    });
    s.check("equalTo", RETRIEVE, |n| {
        n.put(dinosaurs)?;
        let actual = parse(&n.get(r#".json?orderBy="height"&equalTo=25"#)?, 200)?;
        let count = actual.as_object().map_or(0, |m| m.len());
        ensure(count == 1, || actual.to_string())
    });
    s.check("orderBy twice is rejected", RETRIEVE, |n| {
        status(&n.get(r#".json?orderBy="$key"&orderBy="$value""#)?, 400)
    });
}

fn priorities(s: &Spec) {
    s.check("Read a priority through the .priority child", REF, |n| {
        n.put(r#"{"name":"Tom",".priority":1}"#)?;
        body(&n.get("/.priority.json")?, &json!(1))
    });
    s.check("Write a priority through the .priority child", REF, |n| {
        n.put(r#"{"name":"Tom"}"#)?;
        status(&n.send("PUT", "/.priority.json", "5")?, 200)?;
        body(&n.get("/.priority.json")?, &json!(5))
    });
    s.check("Write a primitive with .value and .priority", REF, |n| {
        n.put(r#"{".value":"Tom",".priority":1}"#)?;
        body(&n.get(".json")?, &json!("Tom"))?;
        body(&n.get("/.priority.json")?, &json!(1))
    });
    s.check("Priorities at any depth of a payload", REF, |n| {
        n.put(r#"{"a":{"b":{"x":1,".priority":2}}}"#)?;
        body(&n.get("/a/b/.priority.json")?, &json!(2))
    });
    s.check("format=export includes priorities", REF, |n| {
        n.put(r#"{"a":1,".priority":2}"#)?;
        let actual = parse(&n.get(".json?format=export")?, 200)?;
        ensure(actual[".priority"] == json!(2), || actual.to_string())
    });
    s.check("A shallow GET leaves priorities out", REF, |n| {
        n.put(r#"{"a":1,".priority":2}"#)?;
        body(&n.get(".json?shallow=true")?, &json!({"a": true}))
    });
    s.check("Query results leave priorities out", REF, |n| {
        n.put(r#"{"a":{"x":1,".priority":2}}"#)?;
        let actual = parse(&n.get(r#".json?orderBy="$key"&limitToFirst=1"#)?, 200)?;
        ensure(actual == json!({"a": {"x": 1}}), || actual.to_string())
    });
    s.check("format=export keeps .value and .priority", REF, |n| {
        n.put(r#"{".value":"Tom",".priority":1}"#)?;
        let actual = parse(&n.get(".json?format=export")?, 200)?;
        ensure(actual == json!({".value": "Tom", ".priority": 1}), || {
            actual.to_string()
        })
    });
    s.check("A plain GET leaves priorities out", REF, |n| {
        n.put(r#"{"a":1,".priority":2}"#)?;
        body(&n.get(".json")?, &json!({"a": 1}))
    });
}

fn server_values(s: &Spec) {
    s.check("timestamp is milliseconds since the epoch", REF, |n| {
        n.put(r#"{".sv":"timestamp"}"#)?;
        let actual = parse(&n.get(".json")?, 200)?;
        ensure(
            actual.as_i64().is_some_and(|t| t > 1_600_000_000_000),
            || actual.to_string(),
        )
    });
    s.check("increment adds to the value", REF, |n| {
        n.put("5")?;
        n.put(r#"{".sv":{"increment":2}}"#)?;
        body(&n.get(".json")?, &json!(7))
    });
    s.check("increment on missing data writes the delta", REF, |n| {
        n.put(r#"{".sv":{"increment":2}}"#)?;
        body(&n.get(".json")?, &json!(2))
    });
    s.check("increment with a float gives a double", REF, |n| {
        n.put("5")?;
        n.put(r#"{".sv":{"increment":0.5}}"#)?;
        body(&n.get(".json")?, &json!(5.5))
    });
    s.check("An unrecognized server value is rejected", SAVE, |n| {
        status(&n.send("PUT", ".json", r#"{".sv":"nonsense"}"#)?, 400)
    });
}

fn errors(s: &Spec) {
    s.check("Unparseable PUT data is a 400", REF, |n| {
        status(&n.send("PUT", ".json", "{not json")?, 400)
    });
    s.check("Missing PUT data is a 400 and doesn't write", REF, |n| {
        n.put("1")?;
        status(&n.send("PUT", ".json", "")?, 400)?;
        body(&n.get(".json")?, &json!(1))
    });
    s.check("Missing POST data is a 400", REF, |n| {
        status(&n.send("POST", ".json", "")?, 400)
    });
    for (name, key) in [
        ("dot", "a.b"),
        ("dollar", "a$b"),
        ("hash", "a#b"),
        ("open bracket", "a[b"),
        ("close bracket", "a]b"),
    ] {
        s.check(
            &format!("A key with a {name} is rejected"),
            STRUCTURE,
            |n| {
                let data = json!({key: 1}).to_string();
                status(&n.send("PUT", ".json", &data)?, 400)
            },
        );
    }
    s.check("A key with a control character is rejected", LIMITS, |n| {
        status(&n.send("PUT", ".json", r#"{"a\u0001b":1}"#)?, 400)
    });
    s.check("A path with an invalid child name is rejected", REF, |n| {
        status(&n.get("/a%23b.json")?, 400)
    });
    s.check("A key of 768 bytes is accepted", LIMITS, |n| {
        let data = json!({"k".repeat(768): 1}).to_string();
        status(&n.send("PUT", ".json", &data)?, 200)
    });
    s.check("A key over 768 bytes is rejected", LIMITS, |n| {
        let data = json!({"k".repeat(769): 1}).to_string();
        status(&n.send("PUT", ".json", &data)?, 400)
    });
    s.check("Data past 32 levels is rejected", LIMITS, |n| {
        let mut data = json!(1);
        for _ in 0..33 {
            data = json!({"d": data});
        }
        status(&n.send("PUT", ".json", &data.to_string())?, 400)
    });
    s.check("Data past 32 levels counts the path", LIMITS, |n| {
        let mut data = json!(1);
        for _ in 0..31 {
            data = json!({"d": data});
        }
        status(&n.send("PUT", ".json", &data.to_string())?, 400)
    });
    s.check("Data past 200 levels is rejected", LIMITS, |n| {
        let data = format!("{}1{}", r#"{"d":"#.repeat(200), "}".repeat(200));
        status(&n.send("PUT", ".json", &data)?, 400)
    });
    s.check("A string over 10 MB is rejected", LIMITS, |n| {
        let data = json!("x".repeat(10 * 1024 * 1024 + 1)).to_string();
        status(&n.send("PUT", ".json", &data)?, 400)
    });
    s.check("A literal null deletes", SAVE, |n| {
        n.put("1")?;
        status(&n.send("PUT", ".json", "null")?, 200)?;
        body(&n.get(".json")?, &Value::Null)
    });
    s.check("A rules violation is a 401", REF, |n| {
        status(&n.stack.request("GET", "/private/x.json", &[], None)?, 401)
    });
    s.check("A write that violates rules is a 401", REF, |n| {
        status(
            &n.stack.request("PUT", "/private/x.json", &[], Some(b"1"))?,
            401,
        )
    });
    s.check("An invalid auth token is a 401", REF, |n| {
        status(&n.get(".json?auth=not-a-token")?, 401)
    });
}

/// Reads server-sent events until `count` have arrived, skipping keep-alives.
fn read_events(stream: TcpStream, count: usize) -> Result<Vec<(String, Value)>, String> {
    let mut lines = BufReader::new(stream).lines();
    let mut events = Vec::new();
    let mut name = String::new();
    while events.len() < count {
        let line = lines
            .next()
            .ok_or("stream ended")?
            .map_err(|e| format!("reading stream: {e}, after {events:?}"))?;
        if let Some(rest) = line.strip_prefix("event:") {
            name = rest.trim().to_string();
        } else if let Some(rest) = line.strip_prefix("data:") {
            if name != "keep-alive" {
                let data = serde_json::from_str(rest.trim()).map_err(|e| e.to_string())?;
                events.push((name.clone(), data));
            }
        }
    }
    Ok(events)
}

fn streaming(s: &Spec) {
    s.check("Streaming sends put and patch events", REF, |n| {
        n.put(r#"{"a":1,"b":2}"#)?;
        let mut stream =
            TcpStream::connect(("127.0.0.1", n.stack.edge_port())).map_err(|e| e.to_string())?;
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .map_err(|e| e.to_string())?;
        write!(
            stream,
            "GET {} HTTP/1.1\r\nHost: 127.0.0.1\r\nAccept: text/event-stream\r\n\r\n",
            n.url(".json")
        )
        .map_err(|e| e.to_string())?;
        let reader = stream.try_clone().map_err(|e| e.to_string())?;
        let handle = std::thread::spawn(move || read_events(reader, 3));
        std::thread::sleep(Duration::from_millis(200));
        n.send("PUT", "/c.json", r#"{"foo":true}"#)?;
        n.send("PATCH", "/c.json", r#"{"foo":3,"baz":4}"#)?;
        let events = handle.join().map_err(|_| "reader panicked")??;
        let wanted = vec![
            (
                "put".to_string(),
                json!({"path": "/", "data": {"a": 1, "b": 2}}),
            ),
            (
                "put".to_string(),
                json!({"path": "/c", "data": {"foo": true}}),
            ),
            (
                "patch".to_string(),
                json!({"path": "/c", "data": {"foo": 3, "baz": 4}}),
            ),
        ];
        ensure(events == wanted, || format!("{events:?}"))
    });
}

fn streaming_patches(s: &Spec) {
    s.check("A patch's keys are children of its path", REF, |n| {
        n.put(r#"{"a":{"x":1},"b":{"y":2}}"#)?;
        let mut stream =
            TcpStream::connect(("127.0.0.1", n.stack.edge_port())).map_err(|e| e.to_string())?;
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .map_err(|e| e.to_string())?;
        write!(
            stream,
            "GET {} HTTP/1.1\r\nHost: 127.0.0.1\r\nAccept: text/event-stream\r\n\r\n",
            n.url(".json")
        )
        .map_err(|e| e.to_string())?;
        let reader = stream.try_clone().map_err(|e| e.to_string())?;
        let handle = std::thread::spawn(move || read_events(reader, 3));
        std::thread::sleep(Duration::from_millis(200));
        n.send("PATCH", ".json", r#"{"a/x":9,"b/y":8}"#)?;
        let events = handle.join().map_err(|_| "reader panicked")??;
        let wanted = vec![
            (
                "put".to_string(),
                json!({"path": "/", "data": {"a": {"x": 1}, "b": {"y": 2}}}),
            ),
            ("patch".to_string(), json!({"path": "/a", "data": {"x": 9}})),
            ("patch".to_string(), json!({"path": "/b", "data": {"y": 8}})),
        ];
        ensure(events == wanted, || format!("{events:?}"))
    });
}

/// Runs every check and prints the failures. Returns whether all passed.
pub fn run(stack: &Stack) -> bool {
    let spec = Spec {
        stack,
        next: Cell::new(0),
        passed: Cell::new(0),
        known: Cell::new(0),
        failures: Default::default(),
    };
    methods(&spec);
    conditional(&spec);
    parameters(&spec);
    queries(&spec);
    priorities(&spec);
    server_values(&spec);
    errors(&spec);
    streaming(&spec);
    streaming_patches(&spec);
    let failures = spec.failures.borrow();
    println!(
        "Documented behavior: {} passed, {} known differences, {} failed",
        spec.passed.get(),
        spec.known.get(),
        failures.len()
    );
    for failure in failures.iter() {
        println!("  FAILED {failure}");
    }
    failures.is_empty()
}
