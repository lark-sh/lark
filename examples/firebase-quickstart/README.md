# Firebase Quickstart on Lark

A walkthrough showing how to run Firebase's own [`quickstart-js/database`](https://github.com/firebase/quickstart-js/tree/master/database) sample (a social-blogging app with Firebase Auth + Realtime Database) against a local Lark backend, with **only a handful of lines changed**.

If you're migrating an existing Firebase app to Lark, this is roughly the same changes you'll make; Firebase Auth keeps working as-is (Lark validates Firebase ID tokens server-side), and only the database endpoint changes.

---

## What you'll need

- **Node 18 or newer** and **npm ≥ 9** (e.g. `nvm install 22` and `nvm use 22`)
- **Docker** + `make` (for the local Lark stack)
- A **Firebase project** in the Firebase console (free tier, used only for Auth)
- ~5 minutes

---

## Step 1 — Set up Firebase Auth

In the [Firebase console](https://console.firebase.google.com/):

1. **Create a project** (or use an existing one).
2. **Authentication → Sign-in method**, enable **Google** (the upstream quickstart's default sign-in)
3. **Project settings → General → Your apps**, register a **Web app** and copy the `firebaseConfig` snippet; you'll paste it into `config.ts` in step 5.
4. **Note the Project ID.** It's in the config snippet (for example, `projectId: "quickstart-abc123"`); we'll use this to set up Lark.

You do **not** need to create a Realtime Database in Firebase. The DB lives in Lark; Firebase is only signing tokens.

## Step 2 — Clone Firebase's quickstart

In any directory (someplace **outside** of your `lark/` directory):

```sh
git clone https://github.com/firebase/quickstart-js.git
cd quickstart-js/database
npm install
```

## Step 3 — Start a local Lark stack

In your root `lark` directory:

```sh
make up-release
```

This brings up the Lark service. Wait around 30 seconds for the service to come up fully, then open the admin UI:

```
http://localhost:8080/admin/
```

**Note: If it's your first boot**, you'll need to make note of the admin temporary password that's printed to the console output, and login with that for the first time.

## Step 4 — Create the Lark project

In the admin UI (`http://localhost:8080/admin/`):

1. **Create a new project**: call it `quickstart`. Uncheck "Ephemeral" if you want the data to be saved. Leave "Auto Create" enabled.
2. **Click on the Settings button** in the top-right corner. Set `Firebase Auth project ID` to your Firebase project ID from step 1 (e.g. `quickstart-abc123`). Click on `Save Settings`. This is how Lark knows which Google-signed token issuer to trust for this project.
3. **Default rules are fine**. By default the Lark project will be created with open rules, allowing any read or write. This is fine for demo purposes. In a real app you would want rules that restrict what users can read and write.

## Step 5 — Point the quickstart at Lark

Two files to edit in the cloned `quickstart-js/database/scripts/`:

### `config.ts`

Paste your `firebaseConfig` from step 1, but **insert `databaseURL` to point at your local Lark instance**.

```ts
export const firebaseConfig = {
  apiKey: "<your apiKey>",
  authDomain: "<your project>.firebaseapp.com",
  projectId: "<your project>",
  databaseURL: "http://quickstart.lark.localhost:8080",  // your Lark project
  // ...other fields from your Firebase web-app config
};
```

### `main.ts`

Find the existing emulator block (around lines 60-65) and **delete it entirely**:

```ts
// DELETE THIS BLOCK — we want real Firebase Auth (no emulator), and the
// database is already pointed at Lark via firebaseConfig.databaseURL above.
if (window.location.hostname === 'localhost') {
  connectAuthEmulator(auth, 'http://127.0.0.1:9099');
  connectDatabaseEmulator(database, '127.0.0.1', 9000);
}
```

## Step 6 — Run

```sh
npm install
npm run dev
```

Vite serves the app at `http://localhost:5173/`. Open it, click **Sign in with Google**, write a post. You should see:

- The post appears in real-time without a refresh.
- Stars update across tabs.
- Comments stream in via `onChildAdded`.
- Feel free to open a second tab and see how it syncs between them.

All of that is the unmodified Firebase JS SDK talking to Lark. Note: The "My Top Posts" tab is broken due to a bug in the quickstart itself, and does not work currently with either Lark or Firebase RTDB.

## Step 7 - See the data in Lark

If you'd like, you can go back to your admin dashboard in Lark at `http://localhost:8080/admin` and open up the Project and the Database you created, and inspect the data in the Database Editor, and even watch it change live as you make additional posts.

---

## Troubleshooting

**The browser can't resolve `quickstart.lark.localhost`.** You're on an older browser or a system that doesn't honor RFC 6761. Add `127.0.0.1 quickstart.lark.localhost` to `/etc/hosts`.

**Sign-in works but Lark rejects the token.** The `Firebase Auth project ID` on the Lark project doesn't match the issuer of the ID token. Re-check step 4 against the Firebase console.

---

## Next steps

The same two edits work on your own Firebase app: point `databaseURL` at your Lark project and drop the emulator block. For security rules, the REST API, and the rest of the client surface, see [docs.larksh.com](https://docs.larksh.com).
