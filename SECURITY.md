# Security Policy

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub issues,
discussions, or pull requests.**

Report privately through GitHub's **private vulnerability reporting**: open the
[**Security** tab](https://github.com/lark-sh/lark/security/advisories/new) of the
repository and click **Report a vulnerability**. This opens a channel visible only
to maintainers.

If you can't use GitHub's private reporting, email **team@lark.sh** with
`SECURITY` in the subject line.

Please include as much as you can:

- the affected component (`lark-server`, `lark-edge`, `lark-blob`, …) and the
  version or commit (both binaries report it via `--version` and in their first
  startup log line),
- a description of the issue and its impact,
- steps to reproduce or a proof of concept,
- any suggested mitigation.

## What to expect

- We'll acknowledge your report within **3 business days**.
- We'll confirm the issue, determine impact and affected versions, and keep you
  updated as we work on a fix.
- We'll credit you in the published advisory unless you ask to remain anonymous.
- Please give us a reasonable window to ship a fix before public disclosure — we
  aim for **90 days**.

## Supported versions

Lark is pre-1.0 and under active development. Security fixes land on the latest
release and on `main`. Once we reach 1.0 this section will list the supported
release lines.

## Scope

Lark is the storage and realtime layer for other people's data, so we treat these
as high priority:

- authentication/authorization bypass at the gateway (`lark-edge`),
- security-rules evaluation bypass (`server/src/rules`),
- data exposure across project or database boundaries,
- memory-safety issues in the Rust engine or the blob format.

Denial-of-service via pathological input (a single request that wedges a database)
is in scope; volumetric/network DoS generally is not.
