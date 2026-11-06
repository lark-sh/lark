/**
 * A write that leaves the data as it was sends no data events, only an ack,
 * even to the writer's own subscriptions. An update sends only the keys that
 * changed. Acking a write without first sending its data event once made the
 * SDK drop its optimistic copy and fire child_removed (see
 * lark-multi-path-update.test.ts). These tests check, through the real SDK,
 * that an unchanged write can't do that: the writer's and another client's
 * listeners, plain and limited, see no events and keep their data.
 */

import { expect } from 'chai';

import { Query, Reference } from '../src/api/Reference';
import { getRandomNode, pause } from './helpers/util';

const EVENTS = [
  'value',
  'child_added',
  'child_changed',
  'child_removed',
  'child_moved'
] as const;

interface Seen {
  listener: string;
  event: string;
  key: string | null;
  val: unknown;
}

const ITEMS = {
  a: { name: 'A', score: 1 },
  b: { name: 'B', score: 2 },
  c: { name: 'C', score: 3 }
};

/** Listens for every event type on each query and records what fires. */
function listen(queries: Record<string, Query>, seen: Seen[]): () => void {
  const offs: Array<() => void> = [];
  for (const [listener, query] of Object.entries(queries)) {
    for (const event of EVENTS) {
      const cb = query.on(event, snap => {
        seen.push({ listener, event, key: snap.key, val: snap.val() });
      });
      offs.push(() => query.off(event, cb));
    }
  }
  return () => offs.forEach(off => off());
}

/** The last value event each listener saw. */
function lastValues(seen: Seen[]): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const e of seen) {
    if (e.event === 'value') {
      out[e.listener] = e.val;
    }
  }
  return out;
}

describe('Lark: writes that change nothing send no events through the SDK', function () {
  // A stray event is the failure, so don't let a retry hide one.
  this.retries(0);

  let writerRoot: Reference;
  let observerRoot: Reference;
  let seen: Seen[];
  let stop: () => void;
  let settled: Record<string, unknown>;

  // The plain listener and two limited queries, on the writer's connection
  // and on another client's.
  function queries(root: Reference, who: string): Record<string, Query> {
    const items = root.child('items');
    return {
      [`${who} items`]: items,
      [`${who} limitToLast(2)`]: items.limitToLast(2),
      [`${who} orderByChild(score).limitToLast(2)`]: items
        .orderByChild('score')
        .limitToLast(2),
      [`${who} items/b`]: items.child('b')
    };
  }

  beforeEach(async () => {
    [writerRoot, observerRoot] = getRandomNode(2) as Reference[];
    await writerRoot.set({ items: ITEMS, other: { x: 1 } });
    seen = [];
    stop = listen(
      {
        ...queries(writerRoot, 'writer'),
        ...queries(observerRoot, 'observer')
      },
      seen
    );
    await pause(500);
    settled = lastValues(seen);
    expect(Object.keys(settled)).to.have.length(8);
    seen.length = 0;
  });

  afterEach(() => stop());

  /** Waits for anything the write sends, then checks nothing fired. */
  async function expectNoEvents() {
    await pause(500);
    expect(seen, JSON.stringify(seen, null, 2)).to.deep.equal([]);
    // Each listener still holds what it had, read through the SDK's cache.
    for (const [listener, query] of Object.entries({
      ...queries(writerRoot, 'writer'),
      ...queries(observerRoot, 'observer')
    })) {
      const snap = await query.once('value');
      expect(snap.val(), listener).to.deep.equal(settled[listener]);
    }
  }

  it('set() of the value already at a child in the limited window', async () => {
    await writerRoot.child('items/b').set({ name: 'B', score: 2 });
    await expectNoEvents();
  });

  it('set() of the value already at a child outside the limited window', async () => {
    await writerRoot.child('items/a').set({ name: 'A', score: 1 });
    await expectNoEvents();
  });

  it('set() of the whole list as it is', async () => {
    await writerRoot.child('items').set(ITEMS);
    await expectNoEvents();
  });

  it('set() of a leaf as it is', async () => {
    await writerRoot.child('items/c/score').set(3);
    await expectNoEvents();
  });

  it('update() whose every key holds its value', async () => {
    await writerRoot.child('items').update({ a: ITEMS.a, 'b/score': 2 });
    await expectNoEvents();
  });

  it('multi-path update() at the root whose every key holds its value', async () => {
    await writerRoot.update({
      'items/b/name': 'B',
      'items/c': ITEMS.c,
      'other/x': 1
    });
    await expectNoEvents();
  });

  it('update() with one key changed sends only that key, and nothing is removed', async () => {
    await writerRoot.child('items').update({
      'a/name': 'A',
      'b/score': 2,
      'c/name': 'C2'
    });
    await pause(500);

    expect(
      seen.filter(e => e.event === 'child_removed'),
      JSON.stringify(seen, null, 2)
    ).to.deep.equal([]);
    // Only c changed, so child events name only c. b's own listener fires
    // nothing.
    const childEvents = seen.filter(e => e.event !== 'value');
    expect(childEvents.map(e => e.key)).to.satisfy((keys: string[]) =>
      keys.every(k => k === 'c')
    );
    expect(seen.filter(e => e.listener.endsWith('items/b'))).to.deep.equal([]);

    const expected = {
      ...ITEMS,
      c: { name: 'C2', score: 3 }
    };
    for (const who of ['writer', 'observer']) {
      const values = lastValues(seen);
      expect(values[`${who} items`], who).to.deep.equal(expected);
      expect(values[`${who} limitToLast(2)`], who).to.deep.equal({
        b: expected.b,
        c: expected.c
      });
      expect(
        values[`${who} orderByChild(score).limitToLast(2)`],
        who
      ).to.deep.equal({ b: expected.b, c: expected.c });
    }
  });
});
