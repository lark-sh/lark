# Lark

Lark is a real-time database server that syncs a JSON tree between multiple clients with optional data persistence. It's written primarily in Rust on top of Glommio, with an edge component written in Go for terminating WebSocket, WebTransport, and REST HTTP connections.

You can use it alongside [`@lark-sh/client`](https://www.npmjs.com/package/@lark-sh/client) to easily create web applications which are realtime.

In addition, Lark is designed to be drop-in compatible with Firebase Realtime Database SDKs, including the Firebase JS SDK. You can use Lark as a replacement for the realtime database, while continuing to use other Firebase features (such as Firebase Auth, Hosting, and Storage).

Lark is proudly open-source, and we believe that every developer should have the option to own their data stack without vendor lock-in.

In addition, [Lark Cloud](https://lark.sh) is available for those who want us to handle the devops so you can focus on building your application, while knowing that you always have the option to move onto your own stack in the future.

Full documentation for building on Lark (the client SDK, security rules, the REST API, and Firebase SDK compatibility) lives at [docs.larksh.com](https://docs.larksh.com). The [`docs/`](docs/) folder in this repo covers running Lark itself: deployment, backups, observability, and internals.

## Quick start

If you're exploring Lark for the first time, you probably have two goals: firstly, get Lark running on your local machine so you can explore the platform; and secondly, see something neat running on top of it so you can get a feel for what's possible.

First, you'll need to have `Docker` as well as `make` installed. Then on your local machine:

```bash
make up
```

That brings up `lark-server` and `lark-edge`. The dashboard is at http://localhost:8080/admin/, and the admin email and one-time password are
printed in the log on first start.

**Want it on the public internet instead?** `deploy/fly/quickstart.sh` stands up a real, TLS-terminated Lark deployment on [Fly.io](https://fly.io) in a few minutes (you'll need a domain). See [`deploy/fly/README.md`](deploy/fly/README.md).

### Try an example app

If you'd like to see something running on top of Lark, check out the [`examples/`](examples/) folder:

- [Sticky notes](examples/sticky-notes/README.md) is a multiplayer whiteboard built directly on [`@lark-sh/client`](https://www.npmjs.com/package/@lark-sh/client). Live cursors, draggable notes, snapshot-interpolated motion, and volatile-path presence in ~300 lines of vanilla TypeScript. Start here if you're building a new app on Lark.
- [Firebase quickstart](examples/firebase-quickstart/README.md) runs Firebase's own `quickstart-js/database` social-blogging app against Lark with a handful of lines changed. Start here if you'd like to see how easy it is to migrate an existing Firebase SDK-based app to Lark.

## Development

If you're looking for details on the technical side of how Lark works, or are interested in coding on the actual Lark service itself instead of just
running an app on top of it, check out [CONTRIBUTING](CONTRIBUTING.md) for an overview of the codebase as well as how to setup a full dev environment.

## License

AGPL v3. See [LICENSE](LICENSE).
