# db_barrel

Generate an interactive HTML topology of your PostgreSQL machines, auto-discovered replication links, and the schema for each database.

## What it does
- Connects to PostgreSQL databases you list in a JSON config file.
- Introspects tables/columns.
- Discovers replication links from database metadata (`pg_stat_replication` / `pg_stat_wal_receiver`).
- Emits a single HTML file that shows a D3.js force-directed graph of machines and the discovered replication links.
- Double-click a machine node to switch to a schema view for its databases.

## Prerequisites
- Go 1.25.3.
- Network access to the databases listed in your config.
- Driver: `github.com/lib/pq` (PostgreSQL). Fetch it once with:
  ```bash
  go get github.com/lib/pq
  ```

## Quick start
1. Copy and edit the sample config:
   ```bash
   cp example_topology.json topology.json
   # fill in host/port/user/password per instance (databases are discovered automatically)
   ```
2. Fetch drivers (once per machine) and tidy modules:
   ```bash
   go mod tidy
   ```
3. Run the generator:
   ```bash
   go run . -config topology.json -out topology.html
   ```
4. Open `topology.html` in a browser.

## Configuration
See `example_topology.json` for a complete shape. The config is a flat list of connections:
- `databases[]`: `id` (optional), `name` (optional), `driver` (`postgres`, `postgresql`, or `pgx`), `host` (used as the machine identifier), optional `port` (defaults 5432), `user`, optional `password`, and optionally `database` or `databases[]` to seed/limit which DBs to introspect. If omitted, the script discovers all non-template Postgres databases on the host.

## Notes and extensions
- D3 is loaded from a CDN in the generated HTML. Keep internet access enabled when viewing, or swap in a local copy.
- Replication discovery requires metadata access: Postgres needs permissions to query `pg_stat_replication` / `pg_stat_wal_receiver`.
- To support other databases, add a new `inspect<DB>` function in `main.go` and extend the switch in `inspectDatabase`.
- The script uses a per-database timeout (`-timeout`, default 8s). Increase it if your links are slow.
