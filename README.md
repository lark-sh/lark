# Lark

Lark is a real-time database server that syncs a JSON tree between multiple clients with optional data persistence. It's written primarily in Rust on top of Glommio, with an edge component written in Go for terminating WebSocket, WebTransport, and REST HTTP connections.

You can use it alongside [`@lark-sh/client`](https://www.npmjs.com/package/@lark-sh/client) to easily create web applications which are realtime.

In addition, Lark is designed to be drop-in compatible with Firebase Realtime Database SDKs, including the Firebase JS SDK. You can use Lark as a replacement for the realtime database, while continuing to use other Firebase features (such as Firebase Auth, Hosting, and Storage).

Lark is proudly open-source, and we believe that every developer should have the option to own their data stack without vendor lock-in.

In addition, [Lark Cloud](https://lark.sh) is available for those who want us to handle the devops so you can focus on building your application, while knowing that you always have the option to move onto your own stack in the future.

Full documentation for building on Lark (the client SDK, security rules, the REST API, and Firebase SDK compatibility) lives at [docs.larksh.com](https://docs.larksh.com). The [`docs/`](docs/) folder in this repo covers running Lark itself: deployment, backups, observability, and internals. For how Lark is tested, including running Firebase's own SDK test suite against it and crash-testing the durability contract, see [TESTING.md](TESTING.md).

## Project status

Lark is a new open-source project backed by a production service: [Lark Cloud](https://lark.sh) runs this same codebase and hosts real customer data on it today. The engine is continuously tested against an explicit durability contract and verified against Firebase's own SDK test suite; [TESTING.md](TESTING.md) describes both and shows how to run everything yourself.

The `0.x` version number reflects the project's age, not known instability. The on-disk format is already something we won't break without providing a migration path, and we don't expect churn in the configuration or wire surfaces beyond new features. We'd rather let Lark earn its 1.0 through public production mileage than declare it on our own confidence. Until then, breaking changes in `0.x` releases are rare, documented in the [CHANGELOG](CHANGELOG.md), and accompanied by a migration path whenever stored data is affected.

## Quick start

If you're exploring Lark for the first time, you probably have two goals: firstly, get Lark running on your local machine so you can explore the platform; and secondly, see something neat running on top of it so you can get a feel for what's possible.

You don't need to clone this repo to run Lark — all you need is `Docker`. Download the compose file, give it a secret, and start it:

```bash
mkdir lark && cd lark
curl -fsSL https://raw.githubusercontent.com/lark-sh/lark/main/docker-compose.prod.yml -o docker-compose.yml
echo "SERVER_SECRET=$(openssl rand -hex 32)" > .env
docker compose up
```

That pulls the published `lark-server` and `lark-edge` images and brings up the stack, so there's no Rust or Go toolchain involved. The dashboard is at http://localhost:8080/admin/, and the admin email and one-time password are
printed in the log on first start.

The images track `latest` by default. To pin a version, add it to the same file: `echo "LARK_VERSION=0.2.0" >> .env`.

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
