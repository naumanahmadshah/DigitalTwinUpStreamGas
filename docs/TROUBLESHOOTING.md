# Troubleshooting

The deploy scripts are **idempotent**: if a step fails, fix the cause and re-run the same command.
Existing resources are detected and reused; data generation and the medallion are safe to re-run.

## "Could not create catalog ..."

Your metastore may have no default storage location, so a managed catalog cannot be auto-created.
Options:

- Create the catalog once in the UI (**Catalog > Create catalog**), then re-run the deploy.
- Or point at an existing catalog you can write to: `--catalog <name>` (`-Catalog <name>` on Windows).

You need the `CREATE CATALOG` privilege (workspace-admin or metastore-admin typically have it).

## "Lakebase instance did not become ready"

Lakebase (Database Instances) must be available in your workspace region, and provisioning takes a few
minutes. Check:

- Open **Compute > Database instances** in the workspace. If the feature is not present, Lakebase is
  not enabled in this workspace/region; enable it or use a region where it is available.
- If an instance was half-created, delete it and re-run, or pass a different `--db-instance` name.

The app binds to Lakebase at deploy time, so this must succeed for the deploy to complete.

## "Your databricks command does not support bundles"

You have the legacy Python `databricks-cli`, not the current unified CLI. Install the current CLI from
the [Databricks CLI install guide](https://docs.databricks.com/dev-tools/cli/install.html) and re-run.
On macOS/Linux `brew install databricks` or the curl installer; on Windows `winget install Databricks.DatabricksCLI`.

## The CLI was installed but is "not found" (Windows)

`winget` updates PATH, but the current PowerShell session does not see it. Open a **new** PowerShell
window and run the deploy again.

## Sign-in / auth problems

- Make sure the `--host` / `-WorkspaceUrl` is the full workspace URL (starts with `https://`). Pasting
  the whole browser address (with `?o=...`) is fine; the scripts trim it.
- Passing the URL always signs you in again to exactly that workspace, under a profile named after it
  (e.g. `gastwin-dbc-1234abcd-5678`), so a stale sign-in to another workspace is never used.
- If you previously set `DATABRICKS_AUTH_STORAGE` in your environment, unset it (the scripts already do
  this for their own session).

## No SQL warehouse / app queries are slow

- If the deploy created a warehouse, the **first** query after an idle period pays a serverless
  cold-start of a few seconds. It warms up after that.
- To use a specific warehouse, pass `--warehouse <id>` (`-Warehouse <id>`).

## Apps not enabled

If the app resource fails to deploy, confirm **Databricks Apps** is enabled for the workspace
(**Compute > Apps**). It requires workspace-admin to enable.

## The Genie lamp is missing from the app header

The Genie space was not discovered after the `surfaces` step (it may still be building, or the
`surfaces` job hit an issue). Re-run the deploy with `--skip-data`: it rebuilds surfaces, rediscovers
the space id and restarts the app. You can also confirm a Genie space titled "Asset Intelligence
Genie" exists under **Genie** in the workspace.

## The dashboard or Genie step warned but the deploy continued

Surfaces and Lakebase seeding are best-effort: a hiccup there does not block the app, which is the
main artifact. Re-run `--skip-data` to retry just those steps once the marts are in place.

## Re-running safely

- `scripts/deploy.sh --skip-data` (or `-SkipData`) redeploys code and rebuilds surfaces without
  regenerating data.
- `scripts/generate_data.sh` re-runs only the data generation (it truncates and rewrites the landing
  data, so it does not double-count).
