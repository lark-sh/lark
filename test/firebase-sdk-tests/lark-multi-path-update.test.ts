/**
 * Lark-specific regression tests that run via the Firebase JS SDK's
 * mocha harness against a live Lark stack (see test/run-firebase-sdk.sh).
 *
 * These files are kept in this repo, copied into the SDK's
 * packages/database-compat/test directory at test time, and torn down
 * after the run. They cover wire-level Firebase-compat behavior the
 * upstream suite doesn't directly exercise.
 *
 * This file targets a real-world failure observed running Firebase's
 * own quickstart-js/database social-blog app against Lark: a signed-in
 * user's `writeNewPost` performs `update(ref(db), { "/posts/<id>": ..., "/user-posts/<uid>/<id>": ... })`
 * — a root-level multi-path update with paths-as-keys — while the same
 * client also holds an `orderByChild('starCount').limitToLast(100)` query
 * subscription on `/posts`. Firebase echoes the new child to the
 * subscription; Lark currently doesn't, so the SDK clears its optimistic
 * write on ack and the post disappears from the UI until reload.
 */

import { expect, use } from 'chai';
import chaiAsPromised from 'chai-as-promised';

import { Reference } from '../src/api/Reference';
import { getRandomNode, pause } from './helpers/util';

use(chaiAsPromised);

describe('Lark: root-level multi-path update echoes to writer\'s own query subscription', () => {
  it('the writer\'s /posts query observes the new child added via update({ "/posts/<id>": ... })', async () => {
    // Fresh isolated root so the test doesn't see noise from other tests.
    const root = getRandomNode() as Reference;
    const postsRef = root.child('posts');
    // Mirror the Firebase quickstart's `recentPostsRef`.
    const query = postsRef.orderByChild('starCount').limitToLast(100);

    // Capture every `value` snapshot. The latest one is what the UI
    // binds against — if the bug fires, the SDK rolls back its
    // optimistic write on ack and emits a final null snapshot.
    const snapshots: Array<unknown> = [];
    const cb = query.on('value', snap => {
      snapshots.push(snap.val());
    });

    // Let the initial (empty) snapshot land.
    await pause(100);

    const post = {
      author: 'Alice',
      uid: 'user1',
      title: 'Hello',
      body: 'First post',
      starCount: 0,
    };

    // The exact shape the quickstart's writeNewPost sends: keys with a
    // leading slash. This is the wire form that exposed the
    // path-normalization gap — firebase_adapter's translate_merge built
    // `format!("/{}", "/posts/X")` = "//posts/X" when base_path was "/",
    // and the doubled-slash event.path silently missed every subscriber
    // in find_affected_shared_views' raw string-prefix match.
    await root.update({
      '/posts/post1': post,
      '/user-posts/user1/post1': post,
    });

    // Give the server ack + any post-ack reconciliation time to settle.
    // If Lark were to ack without echoing the new child to the existing
    // /posts subscription, the SDK would clear the optimistic write here
    // and fire a final null snapshot — that's the failure mode.
    await pause(500);

    const finalSnapshot = snapshots[snapshots.length - 1];
    expect(
      finalSnapshot,
      `final /posts snapshot should contain post1 but was ${JSON.stringify(
        finalSnapshot,
      )}; full snapshot history: ${JSON.stringify(snapshots)}`,
    ).to.be.an('object').that.has.property('post1');

    query.off('value', cb);
  });

  it('reproduces the quickstart sequence verbatim: limitToLast-only query, child_added handler, prior /users write, once-read, multi-path update', async () => {
    // Mirror the Firebase quickstart's writeNewPost flow as exactly as
    // possible. Key fidelity points the previous attempt got wrong:
    //   - recentPostsRef uses `query(ref, limitToLast(100))` ONLY — there
    //     is no orderByChild. Wire format becomes `q:{l:100, vf:"r"}`,
    //     matching the user's captured browser trace.
    //   - The list UI listens via `on('child_added', ...)`, not `value`.
    //     If Lark's broadcast for multi-path updates fires `value` but
    //     not `child_added` (or vice versa), the previous test would have
    //     missed it.
    //   - Pre-write, the quickstart does:
    //       set('/users/<uid>', ...)
    //       once('/users/<uid>', ...)   // newPostForCurrentUser's onlyOnce
    //       update('/', { 'posts/<X>': ..., 'user-posts/<uid>/<X>': ... })
    const root = getRandomNode() as Reference;
    const uid = 'user1';

    const postsRef = root.child('posts');
    const userPostsRef = root.child(`user-posts/${uid}`);

    // (1) recentPostsRef — limitToLast(100), NO orderByChild.
    const postsQuery = postsRef.limitToLast(100);

    // What the quickstart's fetchPosts callback would see — every key the
    // child_added handler fires for. If the bug fires, our new post key
    // never lands here (or lands and gets immediately followed by a
    // child_removed for the same key — also captured).
    const childEvents: Array<{ event: string; key: string | null; val: unknown }> = [];
    const onAddedCb = postsQuery.on('child_added', snap => {
      childEvents.push({ event: 'child_added', key: snap.key, val: snap.val() });
    });
    const onRemovedCb = postsQuery.on('child_removed', snap => {
      childEvents.push({ event: 'child_removed', key: snap.key, val: snap.val() });
    });

    // (2) /user-posts/<uid> non-query child_added.
    const userPostsEvents: Array<{ event: string; key: string | null }> = [];
    const userPostsAddedCb = userPostsRef.on('child_added', snap => {
      userPostsEvents.push({ event: 'child_added', key: snap.key });
    });
    const userPostsRemovedCb = userPostsRef.on('child_removed', snap => {
      userPostsEvents.push({ event: 'child_removed', key: snap.key });
    });

    // Let initial subscriptions establish.
    await pause(100);

    // (3) writeUserData — runs on auth-change pre-post.
    await root.child(`users/${uid}`).set({
      username: 'Alice',
      email: 'alice@example.com',
      profile_picture: null,
    });

    // (4) newPostForCurrentUser's once-read on /users/<uid>.
    await new Promise<void>(resolve => {
      root.child(`users/${uid}`).once('value', () => resolve());
    });

    const post = {
      author: 'Alice',
      uid,
      title: 'Hello',
      body: 'First post',
      starCount: 0,
      authorPic: null,
    };

    // (5) writeNewPost's multi-path update — `/`-prefixed keys, the exact
    // shape the quickstart's `'/posts/' + newPostKey` produces. See the
    // first test's comment for why this matters.
    await root.update({
      [`/posts/post1`]: post,
      [`/user-posts/${uid}/post1`]: post,
    });

    // Long enough for any post-ack reconciliation to settle.
    await pause(500);

    // The /posts query must observe a child_added for post1, AND that add
    // must not be followed by a child_removed (which would mean the SDK
    // rolled back its optimistic write because the server didn't echo).
    const postsAdds = childEvents.filter(e => e.event === 'child_added' && e.key === 'post1');
    const postsRemoves = childEvents.filter(e => e.event === 'child_removed' && e.key === 'post1');
    expect(
      postsAdds.length,
      `expected one child_added for post1 on /posts query; events=${JSON.stringify(childEvents)}`,
    ).to.be.greaterThan(0);
    expect(
      postsRemoves.length,
      `post1 was added then removed from /posts query — SDK rolled back optimistic write; events=${JSON.stringify(childEvents)}`,
    ).to.equal(0);

    // Same shape check for the parallel /user-posts/<uid> subscription.
    const userAdds = userPostsEvents.filter(e => e.event === 'child_added' && e.key === 'post1');
    const userRemoves = userPostsEvents.filter(e => e.event === 'child_removed' && e.key === 'post1');
    expect(
      userAdds.length,
      `expected child_added for post1 on /user-posts/${uid}; events=${JSON.stringify(userPostsEvents)}`,
    ).to.be.greaterThan(0);
    expect(
      userRemoves.length,
      `post1 added then removed from /user-posts/${uid}; events=${JSON.stringify(userPostsEvents)}`,
    ).to.equal(0);

    postsQuery.off('child_added', onAddedCb);
    postsQuery.off('child_removed', onRemovedCb);
    userPostsRef.off('child_added', userPostsAddedCb);
    userPostsRef.off('child_removed', userPostsRemovedCb);
  });
});
