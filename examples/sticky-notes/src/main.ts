import { LarkDatabase } from "@lark-sh/client";

// ---------- Config ----------
// Change PROJECT to match the project you created in the Lark admin UI.
// For local development the SDK talks to <project>.lark.localhost:8080.
const PROJECT = "sticky-notes";
const DATABASE = "sticky-notes";
const DOMAIN = "lark.localhost:8080";

// Lerp factor per frame for note drags. Notes write at 25Hz so 0.25/frame
// (~94% in 10 frames) smooths the chunkiness without lagging visibly.
const LERP_ALPHA = 0.25;

// Cursors get the real treatment: snapshot interpolation. We render every
// cursor `RENDER_DELAY_MS` behind real time, with constant velocity between
// the two samples that bracket the render time. That's what real multiplayer
// games do and it's what eliminates the "boil and stall" feel of plain lerp
// when samples only arrive at 7Hz (WebSocket) or 20Hz (WebTransport).
//
// Delay should be a bit more than one inter-sample interval so we always have
// a future sample to interpolate toward.
function renderDelayMs() {
  return db.transportType === "webtransport" ? 80 : 160;
}

// ---------- Types ----------
interface Note {
  x: number;        // 0..1, fraction of board width
  y: number;        // 0..1, fraction of board height
  text: string;
  color: string;    // hex
  tilt: number;     // degrees, baked in so every client sees the same tilt
  author: string;   // uid that created the note (just for fun)
}

interface CursorState {
  x: number;
  y: number;
  name: string;
  color: string;
}

// Animated render state. We track fractional (fx,fy) so window resizes can
// recompute pixel targets, and (cx,cy)/(tx,ty) so the rAF loop can lerp.
interface NoteAnim {
  el: HTMLElement;
  body: HTMLElement;
  fx: number; fy: number;
  cx: number; cy: number;
  tx: number; ty: number;
}

// One sample of a cursor's position, with the time we received it.
// Fractional coords so window resizes don't strand the buffer.
interface CursorSample {
  fx: number;
  fy: number;
  t: number;  // performance.now() at receive
}

interface CursorAnim {
  el: HTMLElement;
  samples: CursorSample[];  // ordered oldest → newest
}

// How many samples to keep per cursor. Enough to cover the render delay even
// when input is bursty; older ones are trimmed each tick.
const CURSOR_BUFFER = 8;

// ---------- Identity ----------
const NAMES = [
  "Otter", "Heron", "Fox", "Sparrow", "Bear", "Lynx",
  "Wren", "Crane", "Hare", "Marten", "Finch", "Stoat",
];
const COLORS = [
  "#5b8def", "#ef6f6c", "#56b870", "#d4a017",
  "#9b6dff", "#e16eb3", "#39b3a7", "#f0883e",
];
const NOTE_COLORS = [
  "#ffe27a", "#ffbb6e", "#ff9ec7", "#a9e1ff",
  "#b8f2c9", "#dcc6ff", "#fff1a8",
];

function pick<T>(arr: readonly T[]): T {
  return arr[Math.floor(Math.random() * arr.length)]!;
}

const me = {
  uid: crypto.randomUUID(),
  name: pick(NAMES),
  color: pick(COLORS),
};

// ---------- DOM ----------
const board = document.getElementById("board")!;
const hint = document.getElementById("hint")!;
const statusEl = document.getElementById("status")!;

function boardSize() {
  return { w: board.clientWidth, h: board.clientHeight };
}

function setStatus(state: "connecting" | "connected" | "disconnected", text: string) {
  statusEl.dataset.state = state;
  statusEl.textContent = text;
}

// ---------- Lark ----------
const db = new LarkDatabase(`${PROJECT}/${DATABASE}`, {
  anonymous: true,
  domain: DOMAIN,
  transport: "websocket", //Use 'auto' if this is deployed publicly to take advantage of WebTransport
});

// Cursor write rate. 50ms (~20Hz) for both transports — the server batches
// and coalesces volatile updates anyway, so writing faster than its broadcast
// rate just means it always has a fresh sample to send when the next tick
// fires. The cost is negligible.
const CURSOR_WRITE_INTERVAL_MS = 50;

db.onConnect(() => {
  setStatus("connected", `connected as ${me.name} · ${db.transportType}`);

  // Sanity check that `cursors/$uid` is configured as volatile in the project's
  // security rules. If it isn't, every cursor move becomes a durable write —
  // fine locally, painful on the real internet.
  if (!db.volatilePaths.some((p) => p.startsWith("cursors"))) {
    console.warn(
      "[lark] cursors are NOT volatile. Add a `.volatile: true` rule under " +
        "`cursors/$uid` in this project's rules — see this example's README."
    );
  }
});
db.onDisconnect(() => setStatus("disconnected", "disconnected"));
db.onError((err) => {
  console.error("[lark]", err);
  const code = (err as { code?: string }).code ?? "error";
  setStatus("disconnected", `error: ${code}`);
});

// Remove our cursor when this client goes away (tab close, network drop, etc.)
db.ref(`cursors/${me.uid}`).onDisconnect().remove();

// ---------- Animation loop ----------
//
// One rAF loop drives every interpolating element. We lerp (cx,cy) toward
// (tx,ty) at LERP_ALPHA per frame. When updates arrive at 20Hz or 7Hz, this
// is what makes them look smooth.

const notes = new Map<string, NoteAnim>();
const cursors = new Map<string, CursorAnim>();
// Notes the local user is currently dragging — skip interpolation for these
// so the drag follows the pointer exactly.
const localDragging = new Set<string>();

function stepNote(n: NoteAnim) {
  n.cx += (n.tx - n.cx) * LERP_ALPHA;
  n.cy += (n.ty - n.cy) * LERP_ALPHA;
  if (Math.abs(n.tx - n.cx) < 0.5) n.cx = n.tx;
  if (Math.abs(n.ty - n.cy) < 0.5) n.cy = n.ty;
}

// Snapshot interpolation: given the cursor's recent samples and the render
// time (slightly in the past), find the two samples that bracket render time
// and lerp between them with constant velocity. If render time falls outside
// the buffer, clamp to the nearest sample so paused or brand-new cursors
// don't jump.
function sampleCursorAt(samples: CursorSample[], renderTime: number) {
  if (samples.length === 0) return null;
  if (samples.length === 1 || renderTime <= samples[0]!.t) {
    return { fx: samples[0]!.fx, fy: samples[0]!.fy };
  }
  const last = samples[samples.length - 1]!;
  if (renderTime >= last.t) return { fx: last.fx, fy: last.fy };

  for (let i = 0; i < samples.length - 1; i++) {
    const a = samples[i]!;
    const b = samples[i + 1]!;
    if (renderTime >= a.t && renderTime < b.t) {
      const t = (renderTime - a.t) / (b.t - a.t);
      return {
        fx: a.fx + (b.fx - a.fx) * t,
        fy: a.fy + (b.fy - a.fy) * t,
      };
    }
  }
  return { fx: last.fx, fy: last.fy };
}

function tickAnim() {
  const now = performance.now();
  const { w, h } = boardSize();

  for (const [id, n] of notes) {
    if (localDragging.has(id)) continue;
    if (n.cx === n.tx && n.cy === n.ty) continue;
    stepNote(n);
    n.el.style.left = `${n.cx}px`;
    n.el.style.top = `${n.cy}px`;
  }

  const renderTime = now - renderDelayMs();
  for (const [, c] of cursors) {
    const pos = sampleCursorAt(c.samples, renderTime);
    if (!pos) continue;
    c.el.style.transform = `translate(${pos.fx * w}px, ${pos.fy * h}px)`;
  }

  requestAnimationFrame(tickAnim);
}
requestAnimationFrame(tickAnim);

// ---------- Notes ----------
const notesRef = db.ref("notes");

function renderNote(id: string, note: Note, opts: { plop: boolean }) {
  const { w, h } = boardSize();
  const tx = note.x * w;
  const ty = note.y * h;

  let n = notes.get(id);
  const isNew = !n;

  if (!n) {
    const el = document.createElement("div");
    el.className = "note";
    el.dataset.id = id;

    const body = document.createElement("div");
    body.className = "note-body";
    body.contentEditable = "false";
    el.appendChild(body);

    const del = document.createElement("button");
    del.className = "note-delete";
    del.textContent = "×";
    del.title = "Delete";
    del.addEventListener("click", (e) => {
      e.stopPropagation();
      notesRef.child(id).remove();
    });
    el.appendChild(del);

    // Snap initial position so the plop happens at the right place — we
    // don't want the new note to slide in from (0,0).
    el.style.left = `${tx}px`;
    el.style.top = `${ty}px`;
    board.appendChild(el);

    n = { el, body, fx: note.x, fy: note.y, cx: tx, cy: ty, tx, ty };
    notes.set(id, n);
    wireNoteInteractions(id, n);
  }

  n.el.style.setProperty("--note-color", note.color);
  n.el.style.setProperty("--note-tilt", `${note.tilt}deg`);

  // Update target — the rAF loop will lerp current toward it.
  n.fx = note.x;
  n.fy = note.y;
  n.tx = tx;
  n.ty = ty;

  // Only overwrite text when the user isn't actively editing — otherwise
  // remote echoes of your own keystrokes would jump the caret around.
  if (document.activeElement !== n.body && n.body.innerText !== note.text) {
    n.body.innerText = note.text;
  }

  if (isNew && opts.plop) {
    n.el.classList.add("plop-in");
    n.el.addEventListener(
      "animationend",
      () => n!.el.classList.remove("plop-in"),
      { once: true }
    );
  }
}

function removeNote(id: string) {
  const n = notes.get(id);
  if (!n) return;
  notes.delete(id);
  localDragging.delete(id);
  n.el.classList.add("plop-out");
  n.el.addEventListener("animationend", () => n.el.remove(), { once: true });
}

notesRef.on("child_added", (snap) => {
  renderNote(snap.key!, snap.val() as Note, { plop: true });
});

notesRef.on("child_changed", (snap) => {
  renderNote(snap.key!, snap.val() as Note, { plop: false });
});

notesRef.on("child_removed", (snap) => {
  removeNote(snap.key!);
});

// ---------- Note interactions: drag, edit ----------

function wireNoteInteractions(id: string, n: NoteAnim) {
  const { el, body } = n;

  // Double-click to edit. Pointerdown to drag.
  el.addEventListener("dblclick", (e) => {
    e.stopPropagation();
    startEditing();
  });

  function startEditing() {
    el.classList.add("editing");
    body.contentEditable = "true";
    body.focus();
    const range = document.createRange();
    range.selectNodeContents(body);
    const sel = window.getSelection();
    sel?.removeAllRanges();
    sel?.addRange(range);
  }

  function stopEditing() {
    el.classList.remove("editing");
    body.contentEditable = "false";
    notesRef.child(id).update({ text: body.innerText });
  }

  body.addEventListener("blur", stopEditing);

  let textWriteTimer: number | undefined;
  body.addEventListener("input", () => {
    if (textWriteTimer) return;
    textWriteTimer = window.setTimeout(() => {
      textWriteTimer = undefined;
      notesRef.child(id).update({ text: body.innerText });
    }, 120);
  });

  // Drag
  let dragging = false;
  let dragOffsetX = 0;
  let dragOffsetY = 0;
  let dragWriteTimer: number | undefined;

  el.addEventListener("pointerdown", (e) => {
    if (el.classList.contains("editing")) return;
    if ((e.target as HTMLElement).closest(".note-delete")) return;

    dragging = true;
    localDragging.add(id);
    el.setPointerCapture(e.pointerId);
    el.classList.add("dragging");

    const rect = el.getBoundingClientRect();
    dragOffsetX = e.clientX - (rect.left + rect.width / 2);
    dragOffsetY = e.clientY - (rect.top + rect.height / 2);
    e.preventDefault();
  });

  el.addEventListener("pointermove", (e) => {
    if (!dragging) return;
    const { w, h } = boardSize();
    const px = e.clientX - dragOffsetX;
    const py = e.clientY - dragOffsetY;

    // Drive the local element directly while dragging — no lerp, no jitter.
    el.style.left = `${px}px`;
    el.style.top = `${py}px`;
    // Keep our anim state in sync so it doesn't snap when drag ends.
    n.cx = n.tx = px;
    n.cy = n.ty = py;
    n.fx = px / w;
    n.fy = py / h;

    if (dragWriteTimer) return;
    dragWriteTimer = window.setTimeout(() => {
      dragWriteTimer = undefined;
      notesRef.child(id).update({ x: n.fx, y: n.fy });
    }, 40);
  });

  el.addEventListener("pointerup", (e) => {
    if (!dragging) return;
    dragging = false;
    localDragging.delete(id);
    el.releasePointerCapture(e.pointerId);
    el.classList.remove("dragging");
    notesRef.child(id).update({ x: n.fx, y: n.fy });
  });
}

// ---------- Create a note by double-clicking the board ----------

board.addEventListener("dblclick", async (e) => {
  if ((e.target as HTMLElement).closest(".note")) return;

  hint.classList.add("dismissed");

  const { w, h } = boardSize();
  const note: Note = {
    x: e.clientX / w,
    y: e.clientY / h,
    text: "",
    color: pick(NOTE_COLORS),
    tilt: (Math.random() - 0.5) * 6, // -3deg .. +3deg
    author: me.uid,
  };
  const newRef = await notesRef.push(note);

  // Open the new note for editing immediately on the creating client.
  const id = newRef.key!;
  setTimeout(() => {
    const n = notes.get(id);
    if (!n) return;
    n.el.dispatchEvent(new MouseEvent("dblclick", { bubbles: true }));
  }, 0);
});

// ---------- Cursors ----------

const cursorsRef = db.ref("cursors");

function renderCursor(uid: string, c: CursorState) {
  if (uid === me.uid) return;
  const { w, h } = boardSize();
  const now = performance.now();

  let anim = cursors.get(uid);
  if (!anim) {
    const el = document.createElement("div");
    el.className = "cursor";
    el.innerHTML = `
      <svg width="22" height="22" viewBox="0 0 22 22">
        <path d="M3 2 L19 11 L11 12 L8 19 Z" fill="${c.color}" stroke="white" stroke-width="1.5" stroke-linejoin="round"/>
      </svg>
      <span class="cursor-label" style="--cursor-color:${c.color}">${escapeHtml(c.name)}</span>
    `;
    // Snap initial position so a brand new cursor doesn't fly in from (0,0).
    el.style.transform = `translate(${c.x * w}px, ${c.y * h}px)`;
    board.appendChild(el);
    anim = { el, samples: [] };
    cursors.set(uid, anim);
  }

  // Append the new sample and trim. Keep only what's relevant to the render
  // window — anything older than ~2x the render delay is dead weight.
  anim.samples.push({ fx: c.x, fy: c.y, t: now });
  const minT = now - renderDelayMs() * 2;
  while (anim.samples.length > CURSOR_BUFFER || (anim.samples.length > 2 && anim.samples[0]!.t < minT)) {
    anim.samples.shift();
  }
}

function removeCursor(uid: string) {
  const anim = cursors.get(uid);
  if (!anim) return;
  anim.el.remove();
  cursors.delete(uid);
}

cursorsRef.on("child_added", (snap) => renderCursor(snap.key!, snap.val() as CursorState));
cursorsRef.on("child_changed", (snap) => renderCursor(snap.key!, snap.val() as CursorState));
cursorsRef.on("child_removed", (snap) => removeCursor(snap.key!));

// Publish our cursor position. See CURSOR_WRITE_INTERVAL_MS above for why
// we write at 20Hz even on WebSocket (server coalesces; freshness matters).
let lastSent = 0;
const myCursorRef = db.ref(`cursors/${me.uid}`);

window.addEventListener("pointermove", (e) => {
  const now = performance.now();
  if (now - lastSent < CURSOR_WRITE_INTERVAL_MS) return;
  lastSent = now;
  const { w, h } = boardSize();
  myCursorRef.set({
    x: e.clientX / w,
    y: e.clientY / h,
    name: me.name,
    color: me.color,
  });
});

window.addEventListener("beforeunload", () => {
  // Best-effort cleanup. onDisconnect() is the real guarantee.
  myCursorRef.remove();
});

// ---------- Helpers ----------

function escapeHtml(s: string) {
  return s.replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]!
  );
}

// ---------- Reposition everything on resize so fractional coords still match ----------
// Cursors store fractional coords in each sample, so the rAF loop picks up the
// new board size automatically — nothing to do for them here.
window.addEventListener("resize", () => {
  const { w, h } = boardSize();
  for (const [, n] of notes) {
    n.tx = n.fx * w;
    n.ty = n.fy * h;
    n.cx = n.tx;
    n.cy = n.ty;
    n.el.style.left = `${n.cx}px`;
    n.el.style.top = `${n.cy}px`;
  }
});
