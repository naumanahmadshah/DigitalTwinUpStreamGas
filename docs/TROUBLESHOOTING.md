# Troubleshooting

The deploy scripts are **idempotent**: if a step fails, fix the cause and re-run the same command.
Existing resources are detected and reused; data generation and the medallion are safe to re-run.

## "Could not create catalog ..."

The deploy uses a catalog that already exists as is, and only tries to create one when it is missing.
This error therefore means the catalog does not exist yet and could not be created: your metastore may
have no default storage location, or you may lack the CREATE CATALOG privilege. Options:

- Create the catalog once in the UI (**Catalog > Create catalog**), then re-run the deploy.
- Or point at an existing catalog you can write to: --catalog &lt;name&gt; (-Catalog &lt;name&gt; on Windows).

Creating a catalog needs the CREATE CATALOG privilege (workspace-admin or metastore-admin typically
have it). Deploying into an existing catalog needs only USE CATALOG and CREATE SCHEMA on it.

## "Lakebase project did not become ready"

The app's only database is a Lakebase Autoscaling project, so Lakebase must be available in your
workspace region. Check:

- Open **Compute > Lakebase** in the workspace. If you cannot create a project there, Lakebase
  Autoscaling is not enabled in this workspace/region; enable it or use a region where it is available.
- If a project was half-created, delete it and re-run, or pass a different --lakebase (-Lakebase) name.

The app binds to Lakebase at deploy time, so this must succeed for the deploy to complete.

## "The Lakebase sync failed, so the app would have no data"

The "Lakebase setup + first sync" job creates one synced table per gold mart and syncs them. Open the job
run (**Jobs & Pipelines**) for the reason:

- **A synced table did not come online:**\
  Open its sync pipeline (named "Synced table: ...") and the
  failing table's message. The most common cause is a gold mart that has not been built yet; re-run the
  medallion, then re-run the deploy with --skip-data.
- **Primary-key errors:**\
  Every synced mart needs a unique, non-null key (listed in
  serving/setup_lakebase.py). If you add or change a gold mart the app reads, add it there with its key.
- The job retries itself twice and is safe to re-run. Its first attempt can end with "The Python process
  exited unexpectedly with an abort signal (SIGABRT)" inside the serverless runtime; the retry then
  succeeds, so this needs no action.

## The app shows yesterday's numbers after a data refresh

The app reads the Lakebase copy of gold, which the "Lakebase sync" job refreshes every 5 minutes (and the
deploy refreshes straight after the medallion). After you re-run the medallion by hand, allow up to 5
minutes, or run the "Lakebase sync" job now. The app also caches each screen for a couple of minutes.
Genie and the dashboard read gold directly, so they update first.

## "Your databricks command does not support bundles"

You have the legacy Python databricks-cli, not the current unified CLI. Install the current CLI from
the [Databricks CLI install guide](https://docs.databricks.com/dev-tools/cli/install.html) and re-run.
On macOS/Linux brew install databricks or the curl installer; on Windows winget install Databricks.DatabricksCLI.

## The CLI was installed but is "not found" (Windows)

winget updates PATH, but the current PowerShell session does not see it. Open a **new** PowerShell
window and run the deploy again.

## Sign-in / auth problems

- Make sure the --host / -WorkspaceUrl is the full workspace URL (starts with https://). Pasting
  the whole browser address (with ?o=...) is fine; the scripts trim it.
- Passing the URL always signs you in again to exactly that workspace, under a profile named after it
  (e.g. gastwin-dbc-1234abcd-5678), so a stale sign-in to another workspace is never used.
- If you previously set DATABRICKS_AUTH_STORAGE in your environment, unset it (the scripts already do
  this for their own session).

## The first screen after a quiet spell is slow

Lakebase scales to zero after 5 idle minutes. The first request after that wakes the compute, which takes
a moment; the app retries the connection once, and screens are fast after that. The SQL warehouse is only
used by the deploy, Genie and the dashboard (pass --warehouse &lt;id&gt; / -Warehouse &lt;id&gt; to choose one).

## Keeping Lakebase always on (no scale to zero)

The deploy creates the Lakebase compute with scale to zero (it suspends after 5 idle minutes). Note that
the 5-minute "Lakebase sync" job touches Lakebase on every run, so in practice the compute rarely gets
5 idle minutes. To keep it always on (no wake-up pause at all), run once after the deploy:

```bash
databricks postgres update-endpoint projects/gas-upstream-twin-lakebase/branches/production/endpoints/primary \
  spec.suspension --json '{"spec":{"no_suspension":true}}' -p <profile>
```

To change how often the app's copy is refreshed, pass --sync-cron "&lt;quartz cron&gt;" to the deploy (or
--sync-cron "" for no schedule), or edit the schedule of the "Lakebase sync" job in the UI.

## Apps not enabled

If the app resource fails to deploy, confirm **Databricks Apps** is enabled for the workspace
(**Compute > Apps**). It requires workspace-admin to enable.

## The Genie lamp is missing from the app header

The Genie space was not discovered after the surfaces step (it may still be building, or the
surfaces job hit an issue). Re-run the deploy with --skip-data: it rebuilds surfaces, rediscovers
the space id and restarts the app. You can also confirm a Genie space titled "Asset Intelligence
Genie" exists under **Genie** in the workspace.

## The dashboard or Genie step warned but the deploy continued

Surfaces (Genie and the dashboard) are best-effort: a hiccup there does not block the app, which is the
main artifact. Re-run --skip-data to retry them once the marts are in place. The Lakebase sync is not
best-effort: the app needs it, so the deploy stops if it fails (see above).

## Re-running safely

- scripts/deploy.sh --skip-data (or -SkipData) redeploys code, rebuilds the medallion, re-syncs
  Lakebase and rebuilds surfaces without regenerating data. Recommendation decisions in Lakebase are kept.
- scripts/generate_data.sh re-runs only the data generation (it truncates and rewrites the landing
  data, so it does not double-count).
