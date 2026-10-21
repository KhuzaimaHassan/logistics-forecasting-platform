# PostgreSQL Free-plan memory investigation

## Verified observations — 2026-10-21

Owner evidence: Aiven email reports a process interrupted by the out-of-memory
manager. Supplied graphs show gradual memory pressure rising toward95% used,
then a sharp fall near03:00UTC on October21 (08:00 Pakistan). CPU/load also rose
before the fall. Chart timing is approximate; a tooltip explicitly dates an
earlier point to October20 21:10UTC. This is a RAM alert, not a disk-full alert.
The graph cannot identify the killed process or establish why memory accumulated.

Read-only diagnostics over verified PostgreSQL TLS succeeded:

- [Baseline39578320625](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39578320625).
- [Expanded39578655289](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39578655289),
  source9a9a712, collection2026-10-21T09:39:51Z /14:39:51 Pakistan.
- Database64,829,119 bytes /61.83MiB. This excludes other databases, WAL and
  provider overhead and is not total service disk usage.
- No other visible sessions in this database; one backend including diagnostics.
  This does not measure peak connections or previous workloads.
- shared_buffers189MiB, work_mem2MiB, hash_mem_multiplier2,
  maintenance_work_mem89MiB, max_connections20, parallel workers per gather2.
- temp_bytes1,485,807,014,530 and temp_files206,582: cumulative historical writes
  with unknown reset time, **not current disk occupation**, RAM usage or proof
  that temporary files caused this incident.
- Largest relations: zone features35.23MiB, monitoring HTML10.69MiB, trips2.91MiB.
- Postmaster start2026-10-18T10:22:40Z. An individual process interruption need
  not change this timestamp; it neither proves nor disproves the supplied alert.

[Sanitized evidence](postgres-memory-diagnostics-evidence.json).
GitHub repository run history queried for October20 18:00 through October21
04:00UTC returned only cloud pipeline39482347476, completed19:22:50UTC. There
was no recorded GitHub job overlapping the apparent03:00UTC memory drop.
This excludes neither lingering database work, public traffic, provider activity
nor a different exact incident time. The graph alone cannot attribute the cause.

## Safe response, preserving Free and existing data

1. Owner: obtain Aiven service Logs/Events around October21 03:00–03:15UTC,
   including the killed process name if available. Share no credentials. If Free
   does not expose this information, request provider support to identify the
   OOM victim and memory consumer, explicitly requesting no plan change or paid
   resource. Root cause and sustained recovery remain **NOT VERIFIED**.
2. Avoid overlapping manual training/monitoring while investigating. Existing
   scheduled jobs use concurrency group free-cloud-warehouse; public API users
   and provider processes are outside this group. Keep the scheduled pipeline
   that maintains online feature availability; it has not been disabled.
3. After attribution, reduce the relevant working set: smaller Feast PIT batches,
   avoid unnecessary unchanged-sample rebuilds, or compute expensive intermediates
   on the existing GitHub runner. Preserve feature correctness, timestamps and
   honest job status. Inspect EXPLAIN without ANALYZE before an expensive live
   reproduction. No heavy stress test was launched against the affected service.
4. Check the actual connection path before applying session memory/parallelism
   limits. Feast opens separate offline-store connections; changing the shared
   SQLAlchemy engine alone does not configure those. work_mem is already2MiB;
   blindly lowering it further can increase scratch I/O on the small Free disk.
5. Validate real forecasts, persistence and representative jobs after a targeted
   fix, with Aiven RAM observed throughout and after subsequent scheduled runs.
   A passing mocked test or successful connection does not certify OOM recovery.

No data deletion, database-wide tuning, forced restart, backend cancellation,
paid resource or plan change was performed. No corrective change is claimed to
have resolved the OOM warning. Keep the Free plan and no-card hosting settings.

## Repeat read-only diagnostics

```powershell
gh workflow run etl_live_smoke.yml --repo KhuzaimaHassan/logistics-forecasting-platform --ref feature/d0-free-cloud-deployment -f cloud_job=diagnose-db
```

Use --ref main after the change is merged. The existing free-cloud environment
provides secrets; do not put credentials in CLI arguments. Download the
database-diagnostics-<run-id> artifact. The script uses a read-only transaction,
10-second statement timeout, allowlisted catalog fields and at most15 relation
results. It does not print raw SQL, usernames, hostnames or connection strings.
Provider historical RAM and process logs are outside its scope.

## Retention is a separate storage policy

The source sample is capped at10,000 positions, but predictions and operational
reports can grow. The pipeline's700MiB database safety check is not a full-disk
or RAM monitor. Before any purge: measure provider disk, prepare an archive and
dry-run counts, preserve active training/serving windows and obtain owner approval.
No automatic DELETE/DROP job was enabled. DELETE does not immediately return all
space to the OS; VACUUM FULL rewrites/locks tables and needs scratch disk. Neither
is an appropriate blind response to this RAM alert.

Official references:
[Aiven buffers and work memory](https://aiven.io/docs/products/postgresql/concepts/pg-shared-buffers),
[PostgreSQL18 resource consumption](https://www.postgresql.org/docs/18/runtime-config-resource.html).
Operation and parallel-worker memory can multiply across plans and sessions;
small persistent tables do not guarantee a small RAM peak.
