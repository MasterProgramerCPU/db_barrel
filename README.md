# db_barrel

Generate an interactive HTML topology of your PostgreSQL machines, auto-discovered replication links, and the schema for each database. One command produces a single self-contained HTML you can send around.

## What it does
- Connects to PostgreSQL databases you list in a JSON config file.
- Discovers the non-template databases on each host (unless you pre-seed them).
- Introspects tables/columns and foreign keys for each discovered DB.
- Discovers replication links from database metadata (`pg_stat_replication`, `pg_stat_wal_receiver`, `pg_stat_subscription`).
- Emits a single HTML file with an interactive D3.js visualization:
  - Replication view: nodes per database, grouped by machine/instance; arrows for replication links.
  - Schema view: double-click a DB node to see tables, columns, and FK arrows.

## Prerequisites
- Go 1.25.3.
- Network access to the databases listed in your config (firewall/VPN as needed).
- PostgreSQL-compatible DSN; driver is `github.com/lib/pq` (installed via `go mod tidy`).
- Privileges:
  - Introspection: read access to `information_schema` and `pg_catalog`.
  - Replication discovery: permissions for `pg_stat_replication`, `pg_stat_wal_receiver`, `pg_stat_subscription` (often `pg_monitor` or superuser).

## Quick start
1. Copy and edit the sample config:
   ```bash
   cp example_topology.json topology.json
   # fill in host/port/user/password per instance (databases auto-discovered unless you list them)
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
The config is a flat list of connections under `databases[]`. Example with SSL verify-full:
```json
{
  "databases": [
    {
      "id": "primary-eu",
      "name": "Primary EU",
      "driver": "postgres",
      "host": "db.example.com",
      "port": 5432,
      "user": "reporter",
      "password": "s3cret",
      "sslmode": "verify-full",
      "sslrootcert": "/path/to/root.crt",
      "databases": ["appdb"]   // optional: restrict/seed database list
    }
  ]
}
```

Fields (per entry):
- `id` (optional): stable identifier; defaults to `host[:port]:database`.
- `name` (optional): display label; defaults to the database name.
- `driver`: `postgres`, `postgresql`, or `pgx`.
- `host`: hostname or IP; used as the machine identifier in the graph.
- `port` (optional): defaults to 5432.
- `user`: database username.
- `password` (optional): password for the user.
- `sslmode` (optional): `disable` (default), `require`, `verify-ca`, `verify-full`, etc.
- `sslrootcert` (optional): path to a root CA when using `verify-ca`/`verify-full`.
- `database` (optional): a specific DB to inspect; if omitted and `databases` is empty, discovery is used.
- `databases` (optional): explicit list of DB names to inspect; skips discovery.

## CLI flags
- `-config` (default `topology.json`): path to the config file.
- `-out` (default `topology.html`): output HTML path.
- `-timeout` (default `8s`): per-connection/query timeout.

## Output
- A single HTML file with embedded JSON payload and inline SVG icons.
- Loads D3 from `https://cdnjs.cloudflare.com/ajax/libs/d3/7.8.5/d3.min.js` (internet needed when viewing; or replace with a local copy by editing `htmlTemplate`).

## Security notes
- Credentials are read from the config file; protect it accordingly.
- The generated HTML embeds the discovered metadata in clear text; treat it as sensitive (contains schema names, table/column names, and replication peers).
- When using `sslmode=verify-full`, provide `sslrootcert` pointing to the CA file issued by your provider.

## Extending/porting
- To add another database type, implement `inspect<DB>` and extend the switch in `inspectDatabase`.
- To change visuals, edit `htmlTemplate` in `main.go`; it is rendered with Go templates.

## Troubleshooting
- Connection/auth failures: re-check `host`/`port`/`user`/`password`, SSL params, and that firewalls/VPN allow access.
- Empty replication graph: ensure the role has access to `pg_stat_replication`/`pg_stat_wal_receiver`; check that replication is active.
- Missing tables/columns: verify the role can read `information_schema` for the target DBs.
