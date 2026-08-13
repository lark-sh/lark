# Sticky Notes — a Lark Quickstart

A tiny multiplayer whiteboard built on [`@lark-sh/client`](https://www.npmjs.com/package/@lark-sh/client). Open it in two tabs and you'll see:

- **Sticky notes** that anyone can place, edit, drag, and delete, synced in real time.
- **Live cursors** for everyone connected, with a name and color.
- A little **plop animation** when notes appear, and a tilt while they're being dragged.

Notes persist (closing all tabs and reopening shows them again). Cursors don't; they disappear the moment you leave.

---

## What you'll need

- **Node 20+** and **npm**
- **Docker** + `make` (for the local Lark stack)
- ~3 minutes

## Step 1 — Start Lark locally

From the root of the `lark` repo:

```sh
make up-release
```

Wait ~30 seconds, then open the admin UI:

```
http://localhost:8080/admin/
```

If this is your first time, the admin temporary password is printed in the `make up-release` output. Sign in with that.

## Step 2 — Create the project

In the admin UI:

1. Click **Create Project**. Name it **`Sticky Notes`** (which should create the slug `sticky-notes`).
2. Leave **Auto Create** enabled. Uncheck **Ephemeral** if you'd like notes to survive a restart of the stack.
3. Open the project, then click the **Project Settings** button in the top-right corner, and replace the default rules with:

   ```json
   {
     "rules": {
       ".read": true,
       ".write": true,
       "cursors": {
         "$uid": {
           ".volatile": true
         }
       }
     }
   }
   ```

Setting `volatile: true` on the cursors path tells Lark that this will be frequently-written data that doesn't need to be persisted to disk. It means one server can carry both your fire-and-forget presence data and your persisted data. If you don't set this flag, every cursor position is written to disk, which adds up fast, so don't forget it!

That's it on the Lark side.

## Step 3 — Run the example

From this directory (`examples/sticky-notes`):

```sh
npm install
npm run dev
```

Vite serves the app at <http://localhost:5173>. Open it in two browser windows side by side. Double-click anywhere on either window to drop a sticky note.

---

## What's happening under the hood

The whole app touches three Lark APIs. The relevant lines are all in `src/main.ts`:

| Behavior | Lark API |
|---|---|
| Connect to your local Lark instance | `new LarkDatabase("sticky-notes/sticky-notes", { anonymous: true, domain: "lark.localhost:8080" })` |
| Spawn a new note with a server-assigned key | `notesRef.push(note)` |
| Live-sync notes across all clients | `notesRef.on("child_added" \| "child_changed" \| "child_removed", …)` |
| Edit / move a note | `notesRef.child(id).update({ text })` / `update({ x, y })` |
| Delete a note | `notesRef.child(id).remove()` |
| Publish your cursor position | `db.ref("cursors/<uid>").set({ x, y, name, color })` (throttled to match the server rate) |
| Auto-clean your cursor when you leave | `db.ref("cursors/<uid>").onDisconnect().remove()` |

### Smoothing cursor + drag motion

Lark broadcasts volatile updates at **~20Hz over WebTransport** and **~7Hz over WebSocket**; fast, but visibly chunky if you paint each update directly. Two different smoothing strategies in this example, picked to match how data arrives:

- **Cursors (volatile, 7–20Hz): snapshot interpolation.** Each cursor keeps a small ring buffer of recent samples with timestamps. On every animation frame, we render the cursor at `now - renderDelay` (~80ms on WebTransport, ~160ms on WebSocket), linearly interpolating between the two samples that bracket that time. Motion is constant-velocity between known points, the same technique multiplayer game engines use to hide latency. The visible cost is a small lag behind real time, which for presence is invisible.
- **Notes during remote drag (durable, 25Hz): plain exponential lerp.** Drag updates arrive often enough that a simple `current += (target - current) * 0.25` per frame looks smooth without needing a delay budget.

See `sampleCursorAt()` and `stepNote()` in `src/main.ts`.

We also write cursor positions at ~20Hz over WebSocket even though the server only rebroadcasts at ~7Hz. That's intentional: volatile writes are batched and coalesced server-side, so writing faster just means the server always has a fresh sample to send on its next tick. The cost on the wire is tiny.

The data layout in Lark looks like:

```
sticky-notes/sticky-notes
├── notes
│   ├── -N9aB1c…  { x, y, text, color, tilt, author }
│   └── -N9aB1d…  { x, y, text, color, tilt, author }
└── cursors
    ├── 5f2e…    { x, y, name, color }
    └── b1a4…    { x, y, name, color }
```

You can watch it update live in the admin UI's **Database Editor** while you place and drag notes.

---

## Things to try

- Open the admin Database Editor in a third tab while the app runs, and watch the JSON tree mutate as notes are placed and dragged.
- Disconnect a tab (close it, or stop and start `make` underneath it) and watch its cursor vanish from the other tab. That's `onDisconnect().remove()` firing.
- Confirm cursors are flowing volatile. Open the browser devtools console: the connect log prints `connected as Otter · webtransport` (or `· websocket`), and you'll get a loud warning if the volatile rule didn't take. You can also `console.log(db.volatilePaths)` to inspect.

## Customizing

- The connection config is at the top of `src/main.ts` (`PROJECT`, `DATABASE`, `DOMAIN`). Change `DOMAIN` to point at a remote Lark instance (e.g. `larkdb.net`) and your project's name to take it beyond local.
- All the look-and-feel is in `src/style.css`: note colors, the plop keyframes, drag rotation, and the cursor SVG label.
