# Project working rules

The user wants enterprise practice to guide this project and all subsequent work.

- Base architecture, storage conventions, security and operations on documented practices appropriate to the workload. State sources and tradeoffs for material decisions; do not present a project convention as a universal industry standard.
- Build a realistic, operable migration: reproducible deployment, least-privilege identities, protected secrets, meaningful health checks, reconciliation, audit evidence, recovery and rollback. Record unfinished operational requirements honestly.
- Make lab simplifications explicit. The single Linux VM has 4 vCPU and 12 GiB RAM; high availability and production capacity are not established by small fixture tests. Avoid extra services without a demonstrated need.
- The current priority is a small, demonstrable POC. Keep follow-up steps focused and distinguish technical-fixture experiments from production qualification. The completed manual `lab.CdcProbe` purge pilot is documented in decision 012; it does not authorize business purges.
- The POC closure demonstration is complete; see `docs/validation/poc-closure-v1-2026-10-29.json`. Treat HA, off-VM recovery, further SQL objects and production cutover as optional new scope, not unfinished POC acceptance work. The source has been restarted. The current follow-up is preparing authentic demonstration screenshots with visible data for the user's Medium article.
- Preserve the user's choices: Docker-hosted open-source tooling, SQL Server as the migration source, PostgreSQL as the warehouse, SeaweedFS for S3-compatible storage, no Kafka broker/Connect deployment and no dbt. RabbitMQ is intended for one downstream application only.
- Keep raw data, control manifests and validated downstream exports distinguishable. Choose partitions from access patterns and scale; define their time basis and timezone. Do not add hash fanout solely because it looks technical or claim performance improvements without evidence.
- Changes to archive paths or formats are data-contract changes. Version them, migrate existing data with checksum and coverage checks, preserve a rollback route, update readers and documentation, and verify new CDC delivery. Do not silently reset offsets or discard acknowledged data.
- A written object or a minimum/maximum LSN alone does not authorize SQL CDC cleanup. Require reconciled archive coverage, consumer checkpoints, a retention margin and recovery evidence before implementing purge.
- Inventory and migrate stored procedures, functions, views, triggers, jobs and application dependencies as a separate workstream; copying tables alone does not decommission SQL Server.
- Keep architecture diagrams and repository documentation in English. Communicate with the user in French.
- Persist routine authorized work to completion. These rules do not add a new approval requirement for ordinary reversible fixes.
