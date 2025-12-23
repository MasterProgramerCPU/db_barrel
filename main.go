package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	pq "github.com/lib/pq"
)

type Config struct {
	Databases []DatabaseConfig `json:"databases"`
}

type DatabaseConfig struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	Driver    string   `json:"driver"`
	Host      string   `json:"host"`
	Port      int      `json:"port,omitempty"`
	User      string   `json:"user"`
	Password  string   `json:"password,omitempty"`
	Database  string   `json:"database,omitempty"`
	Databases []string `json:"databases,omitempty"` // optional seed list; discovered if empty
	SSLMode   string   `json:"sslmode,omitempty"`
	SSLRootCA string   `json:"sslrootcert,omitempty"`
}

type ReplicationEdge struct {
	FromMachine string  `json:"from_machine"`
	ToMachine   string  `json:"to_machine"`
	FromDB      string  `json:"from_db,omitempty"`
	ToDB        string  `json:"to_db,omitempty"`
	Type        string  `json:"type,omitempty"`
	LagSeconds  float64 `json:"lag_seconds,omitempty"`
	Notes       string  `json:"notes,omitempty"`
}

type Machine struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Databases []DatabaseModel `json:"databases"`
}

type DatabaseModel struct {
	ID          string       `json:"id"`
	Name        string       `json:"name"`
	Driver      string       `json:"driver"`
	Host        string       `json:"host,omitempty"`
	Port        int          `json:"port,omitempty"`
	Tables      []Table      `json:"tables"`
	ForeignKeys []ForeignKey `json:"foreign_keys,omitempty"`
}

type Table struct {
	Schema  string   `json:"schema"`
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}

type Column struct {
	Name         string `json:"name"`
	DataType     string `json:"data_type"`
	IsNullable   bool   `json:"is_nullable"`
	DefaultValue string `json:"default_value,omitempty"`
}

type ForeignKey struct {
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	Column    string `json:"column"`
	RefSchema string `json:"ref_schema"`
	RefTable  string `json:"ref_table"`
	RefColumn string `json:"ref_column"`
}

type VisualizationPayload struct {
	Machines    []Machine         `json:"machines"`
	Replication []ReplicationEdge `json:"replication"`
	GeneratedAt time.Time         `json:"generated_at"`
}

type TopologyResult struct {
	Machines    []Machine
	Replication []ReplicationEdge
}

var errNoDatabases = errors.New("no databases discovered")

const (
	defaultDbIconData     = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0naHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmcnIHdpZHRoPSc0MCcgaGVpZ2h0PSc0MCcgdmlld0JveD0nMCAwIDQwIDQwJz48Y2lyY2xlIGN4PScyMCcgY3k9JzIwJyByPScxOCcgZmlsbD0nI2MwNzgzMycvPjwvc3ZnPg=="
	defaultBarrelIconData = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0naHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmcnIHdpZHRoPSc0MCcgaGVpZ2h0PSc0MCcgdmlld0JveD0nMCAwIDQwIDQwJz48Y2lyY2xlIGN4PScyMCcgY3k9JzIwJyByPScxOCcgZmlsbD0nI2MwNzgzMycvPjwvc3ZnPg=="
)

func isSystemDatabase(name string) bool {
	return strings.EqualFold(strings.TrimSpace(name), "postgres")
}

func ensureNonNilTables(tables []Table) []Table {
	if tables == nil {
		return []Table{}
	}
	for i := range tables {
		if tables[i].Columns == nil {
			tables[i].Columns = []Column{}
		}
	}
	return tables
}

func main() {
	var (
		configPath = flag.String("config", "topology.json", "Path to the topology configuration file")
		outputPath = flag.String("out", "topology.html", "Path to write the generated HTML")
		timeout    = flag.Duration("timeout", 8*time.Second, "Timeout per database connection and query")
	)
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	topology, err := introspectTopology(context.Background(), cfg, *timeout)
	if err != nil {
		log.Fatalf("failed to introspect topology: %v", err)
	}

	payload := VisualizationPayload{
		Machines:    topology.Machines,
		Replication: topology.Replication,
		GeneratedAt: time.Now().UTC(),
	}

	if err := writeHTML(*outputPath, payload); err != nil {
		log.Fatalf("failed to write HTML: %v", err)
	}

	log.Printf("Topology visualized to %s", *outputPath)
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return &cfg, nil
}

func introspectTopology(ctx context.Context, cfg *Config, timeout time.Duration) (*TopologyResult, error) {
	result := &TopologyResult{}
	hostIndex := buildHostMachineIndex(cfg.Databases)
	machineIndex := make(map[string]int)

	for _, connCfg := range cfg.Databases {
		hostKey := normalizeHost(connCfg.Host)
		if hostKey == "" {
			return nil, fmt.Errorf("config entry %q missing host", connCfg.ID)
		}

		if isSystemDatabase(connCfg.Database) {
			log.Printf("skipping system database %q on host %s", connCfg.Database, hostKey)
			continue
		}

		// normalize port for consistent IDs and grouping
		connPort := connCfg.Port
		if connPort == 0 {
			connPort = 5432
		}

		dbNames, err := discoverDatabases(ctx, connCfg, timeout)
		if err != nil {
			if errors.Is(err, errNoDatabases) {
				log.Printf("warning: host %s: %v", hostKey, err)
				continue
			}
			return nil, fmt.Errorf("host %s discover databases: %w", hostKey, err)
		}

		idx, ok := machineIndex[hostKey]
		if !ok {
			result.Machines = append(result.Machines, Machine{
				ID:   hostKey,
				Name: hostKey,
			})
			idx = len(result.Machines) - 1
			machineIndex[hostKey] = idx
		}

		for _, dbName := range dbNames {
			if isSystemDatabase(dbName) && len(dbNames) > 1 {
				continue
			}

			dbCfg := connCfg
			dbCfg.Port = connPort
			dbCfg.Database = dbName
			if dbCfg.ID == "" {
				baseID := hostKey
				if connPort != 0 {
					baseID = baseID + ":" + strconv.Itoa(connPort)
				}
				dbCfg.ID = baseID + ":" + dbName
			} else if len(dbNames) > 1 {
				dbCfg.ID = dbCfg.ID + ":" + dbName
			}
			if dbCfg.Name == "" {
				dbCfg.Name = dbName
			}

			dbResult, err := inspectDatabase(ctx, hostKey, dbCfg, timeout, hostIndex)
			if err != nil {
				return nil, fmt.Errorf("host %s database %s: %w", hostKey, dbCfg.ID, err)
			}

			dbModel := DatabaseModel{
				ID:          dbCfg.ID,
				Name:        dbCfg.Name,
				Driver:      dbCfg.Driver,
				Host:        hostKey,
				Port:        connPort,
				Tables:      ensureNonNilTables(dbResult.Tables),
				ForeignKeys: dbResult.ForeignKeys,
			}

			result.Machines[idx].Databases = append(result.Machines[idx].Databases, dbModel)
			result.Replication = append(result.Replication, dbResult.Replication...)

			log.Printf("introspected %s/%s: %d tables, %d foreign keys, %d replication links", hostKey, dbCfg.ID, len(dbModel.Tables), len(dbResult.ForeignKeys), len(dbResult.Replication))
		}
	}

	if result.Replication == nil {
		result.Replication = []ReplicationEdge{}
	}

	return result, nil
}

type dbInspectResult struct {
	Tables      []Table
	ForeignKeys []ForeignKey
	Replication []ReplicationEdge
}

func inspectDatabase(parentCtx context.Context, machineID string, cfg DatabaseConfig, timeout time.Duration, hostIndex map[string]string) (*dbInspectResult, error) {
	driver := strings.ToLower(cfg.Driver)
	if driver == "" {
		return nil, errors.New("driver is required")
	}
	if cfg.Host == "" {
		return nil, errors.New("host is required")
	}
	if cfg.User == "" {
		return nil, errors.New("user is required")
	}
	if cfg.Database == "" {
		return nil, errors.New("database is required")
	}

	ctx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	dsn, err := buildDSN(cfg)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	result := &dbInspectResult{}

	switch driver {
	case "postgres", "postgresql", "pgx":
		tables, fks, err := inspectPostgresTables(ctx, db, cfg)
		if err != nil {
			return nil, err
		}
		result.Tables = tables
		result.ForeignKeys = fks
		replication := discoverPostgresReplication(ctx, db, cfg, machineID, hostIndex)
		result.Replication = append(result.Replication, replication...)
	default:
		return nil, fmt.Errorf("unsupported driver %q", cfg.Driver)
	}

	return result, nil
}

func inspectPostgresTables(ctx context.Context, db *sql.DB, cfg DatabaseConfig) ([]Table, []ForeignKey, error) {
	exclusions := []string{"pg_catalog", "information_schema"}
	param := arrayParam(exclusions)

	tables, err := listTables(ctx, db, `
		select table_schema, table_name
		from information_schema.tables
		where table_type='BASE TABLE' and (coalesce(array_length($1::text[],1),0)=0 or table_schema <> all($1))
		order by table_schema, table_name
	`, param)
	if err != nil {
		return nil, nil, err
	}

	columns, err := listColumns(ctx, db, `
		select table_schema, table_name, column_name, data_type, is_nullable, coalesce(column_default, '')
		from information_schema.columns
		where (coalesce(array_length($1::text[],1),0)=0 or table_schema <> all($1))
		order by table_schema, table_name, ordinal_position
	`, param)
	if err != nil {
		return nil, nil, err
	}

	fks, err := listForeignKeys(ctx, db, `
		select
		  tc.table_schema,
		  tc.table_name,
		  kcu.column_name,
		  ccu.table_schema as foreign_table_schema,
		  ccu.table_name as foreign_table_name,
		  ccu.column_name as foreign_column_name
		from information_schema.table_constraints tc
		join information_schema.key_column_usage kcu
		  on tc.constraint_name = kcu.constraint_name
		 and tc.constraint_schema = kcu.constraint_schema
		join information_schema.constraint_column_usage ccu
		  on ccu.constraint_name = tc.constraint_name
		 and ccu.constraint_schema = tc.constraint_schema
		where tc.constraint_type = 'FOREIGN KEY'
		  and (coalesce(array_length($1::text[],1),0)=0 or tc.table_schema <> all($1))
	`, param)
	if err != nil {
		return nil, nil, err
	}

	if len(fks) == 0 {
		alt, altErr := listForeignKeys(ctx, db, `
			with fk as (
			  select
			    c.oid,
			    c.conname,
			    n.nspname as table_schema,
			    rel.relname as table_name,
			    nf.nspname as foreign_table_schema,
			    relf.relname as foreign_table_name,
			    c.conkey as src_cols,
			    c.confkey as tgt_cols
			  from pg_constraint c
			  join pg_class rel on rel.oid = c.conrelid
			  join pg_namespace n on n.oid = rel.relnamespace
			  join pg_class relf on relf.oid = c.confrelid
			  join pg_namespace nf on nf.oid = relf.relnamespace
			  where c.contype = 'f'
			    and (coalesce(array_length($1::text[],1),0)=0 or n.nspname <> all($1))
			)
			select
			  fk.table_schema,
			  fk.table_name,
			  att.attname as column_name,
			  fk.foreign_table_schema,
			  fk.foreign_table_name,
			  attf.attname as foreign_column_name
			from fk
			join unnest(fk.src_cols) with ordinality as src(attnum, ord) on true
			join unnest(fk.tgt_cols) with ordinality as tgt(attnum, ord) on src.ord = tgt.ord
			join pg_class rel on rel.relname = fk.table_name
			join pg_namespace n on n.nspname = fk.table_schema and n.oid = rel.relnamespace
			join pg_attribute att on att.attrelid = rel.oid and att.attnum = src.attnum
			join pg_class relf on relf.relname = fk.foreign_table_name
			join pg_namespace nf on nf.nspname = fk.foreign_table_schema and nf.oid = relf.relnamespace
			join pg_attribute attf on attf.attrelid = relf.oid and attf.attnum = tgt.attnum
		`, param)
		if altErr == nil && len(alt) > 0 {
			log.Printf("info: foreign keys discovered via pg_catalog fallback: %d", len(alt))
			fks = alt
		} else if altErr != nil {
			log.Printf("warning: pg_catalog FK fallback failed: %v", altErr)
		}
	}

	attachColumns(tables, columns)
	return tables, fks, nil
}

type tableRow struct {
	Schema string
	Name   string
}

type columnRow struct {
	Schema       string
	Table        string
	Column       string
	DataType     string
	IsNullable   bool
	DefaultValue string
}

func listTables(ctx context.Context, db *sql.DB, query string, param any) ([]Table, error) {
	rows, err := db.QueryContext(ctx, query, param)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer rows.Close()

	var result []Table
	for rows.Next() {
		var r tableRow
		if err := rows.Scan(&r.Schema, &r.Name); err != nil {
			return nil, fmt.Errorf("scan tables: %w", err)
		}
		result = append(result, Table{Schema: r.Schema, Name: r.Name})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tables: %w", err)
	}
	return result, nil
}

func listColumns(ctx context.Context, db *sql.DB, query string, param any) ([]columnRow, error) {
	rows, err := db.QueryContext(ctx, query, param)
	if err != nil {
		return nil, fmt.Errorf("list columns: %w", err)
	}
	defer rows.Close()

	var result []columnRow
	for rows.Next() {
		var r columnRow
		var nullable string
		if err := rows.Scan(&r.Schema, &r.Table, &r.Column, &r.DataType, &nullable, &r.DefaultValue); err != nil {
			return nil, fmt.Errorf("scan columns: %w", err)
		}
		r.IsNullable = strings.EqualFold(nullable, "YES")
		result = append(result, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate columns: %w", err)
	}
	return result, nil
}

func attachColumns(tables []Table, columns []columnRow) {
	tableIndex := make(map[string]*Table)
	for i := range tables {
		key := tables[i].Schema + "." + tables[i].Name
		tableIndex[key] = &tables[i]
	}

	for _, col := range columns {
		key := col.Schema + "." + col.Table
		if tbl, ok := tableIndex[key]; ok {
			tbl.Columns = append(tbl.Columns, Column{
				Name:         col.Column,
				DataType:     col.DataType,
				IsNullable:   col.IsNullable,
				DefaultValue: col.DefaultValue,
			})
		}
	}
}

type fkRow struct {
	Schema    string
	Table     string
	Column    string
	RefSchema string
	RefTable  string
	RefColumn string
}

func listForeignKeys(ctx context.Context, db *sql.DB, query string, param any) ([]ForeignKey, error) {
	rows, err := db.QueryContext(ctx, query, param)
	if err != nil {
		return nil, fmt.Errorf("list foreign keys: %w", err)
	}
	defer rows.Close()

	var result []ForeignKey
	for rows.Next() {
		var r fkRow
		if err := rows.Scan(&r.Schema, &r.Table, &r.Column, &r.RefSchema, &r.RefTable, &r.RefColumn); err != nil {
			return nil, fmt.Errorf("scan foreign keys: %w", err)
		}
		result = append(result, ForeignKey{
			Schema:    r.Schema,
			Table:     r.Table,
			Column:    r.Column,
			RefSchema: r.RefSchema,
			RefTable:  r.RefTable,
			RefColumn: r.RefColumn,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate foreign keys: %w", err)
	}
	if len(result) == 0 {
		log.Printf("info: no foreign keys discovered; ensure FK constraints exist and user can read information_schema")
	}
	return result, nil
}

func arrayParam(values []string) any {
	if len(values) == 0 {
		return nil
	}
	return pq.Array(values)
}

func writeHTML(path string, payload VisualizationPayload) error {
	if payload.Machines == nil {
		payload.Machines = []Machine{}
	}
	if payload.Replication == nil {
		payload.Replication = []ReplicationEdge{}
	}

	dbIcon := defaultDbIconData
	if b64, err := fileDataURI("database.svg"); err == nil {
		dbIcon = b64
	}
	logoIcon := defaultBarrelIconData
	if b64, err := fileDataURI("barrel.svg"); err == nil {
		logoIcon = b64
	}

	jsonBytes, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	data := struct {
		Payload template.JS
		DbIcon  template.JS
		Logo    template.URL
	}{
		Payload: template.JS(jsonBytes),
		DbIcon:  template.JS(strconv.Quote(dbIcon)),
		Logo:    template.URL(logoIcon),
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer f.Close()

	tmpl, err := template.New("page").Parse(htmlTemplate)
	if err != nil {
		return fmt.Errorf("parse template: %w", err)
	}

	if err := tmpl.Execute(f, data); err != nil {
		return fmt.Errorf("execute template: %w", err)
	}
	return nil
}

func fileDataURI(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	enc := base64.StdEncoding.EncodeToString(b)
	return "data:image/svg+xml;base64," + enc, nil
}

func displayName(id, display string) string {
	if strings.TrimSpace(display) != "" {
		return display
	}
	return id
}

func buildHostMachineIndex(dbs []DatabaseConfig) map[string]string {
	index := make(map[string]string)
	for _, db := range dbs {
		host := normalizeHost(db.Host)
		if host == "" {
			continue
		}
		if _, exists := index[host]; !exists {
			index[host] = host
		}
	}
	return index
}

func discoverDatabases(ctx context.Context, cfg DatabaseConfig, timeout time.Duration) ([]string, error) {
	if len(cfg.Databases) > 0 {
		return cfg.Databases, nil
	}

	driver := strings.ToLower(cfg.Driver)
	if driver == "" {
		driver = "postgres"
	}
	if driver != "postgres" && driver != "postgresql" && driver != "pgx" {
		return nil, fmt.Errorf("unsupported driver %q", cfg.Driver)
	}

	controlCfg := cfg
	if controlCfg.Database == "" {
		controlCfg.Database = "postgres"
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	dsn, err := buildDSN(controlCfg)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("open control connection: %w", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping control: %w", err)
	}

	rows, err := db.QueryContext(ctx, `
		select datname
		from pg_database
		where datistemplate = false and datallowconn = true
	`)
	if err != nil {
		return nil, fmt.Errorf("list databases: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan databases: %w", err)
		}
		if !isSystemDatabase(name) {
			names = append(names, name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate databases: %w", err)
	}

	if len(names) == 0 {
		log.Printf("info: %s:%d no databases discovered; falling back to postgres for replication visibility", cfg.Host, cfg.Port)
		return []string{"postgres"}, nil
	}
	return names, nil
}

func parseConninfoHost(dsn string) string {
	parts := strings.Fields(dsn)
	for _, p := range parts {
		if strings.HasPrefix(p, "host=") {
			return strings.TrimPrefix(p, "host=")
		}
	}
	return ""
}

func normalizeHost(hostPort string) string {
	val := strings.TrimSpace(strings.ToLower(hostPort))
	val = strings.Trim(val, "[]")
	if val == "" {
		return ""
	}
	if strings.Contains(val, ":") {
		host, _, err := net.SplitHostPort(val)
		if err == nil {
			return host
		}
	}
	return val
}

func mapHostToMachine(host string, index map[string]string) string {
	normalized := normalizeHost(host)
	if normalized == "" {
		return host
	}
	if machine, ok := index[normalized]; ok {
		return machine
	}
	return host
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func discoverPostgresReplication(ctx context.Context, db *sql.DB, cfg DatabaseConfig, machineID string, hostIndex map[string]string) []ReplicationEdge {
	var edges []ReplicationEdge

	logPrefix := "postgres " + cfg.ID
	hostForLog := cfg.Host

	outgoing, err := db.QueryContext(ctx, `
		select coalesce(client_addr::text,''), coalesce(application_name,''), coalesce(state,''), coalesce(extract(epoch from write_lag),0)
		from pg_stat_replication
	`)
	if err == nil {
		defer outgoing.Close()
		for outgoing.Next() {
			var clientAddr, appName, state string
			var lagSeconds float64
			if err := outgoing.Scan(&clientAddr, &appName, &state, &lagSeconds); err != nil {
				log.Printf("warning: %s parse pg_stat_replication: %v", logPrefix, err)
				break
			}
			target := mapHostToMachine(firstNonEmpty(clientAddr, appName), hostIndex)
			edges = append(edges, ReplicationEdge{
				FromMachine: machineID,
				ToMachine:   target,
				FromDB:      cfg.ID,
				Type:        "postgres-streaming",
				LagSeconds:  lagSeconds,
				Notes:       state,
			})
		}
		if err := outgoing.Err(); err != nil {
			log.Printf("warning: %s iterate pg_stat_replication: %v", logPrefix, err)
		} else {
			log.Printf("info: %s pg_stat_replication rows: %d", logPrefix, len(edges))
		}
	} else {
		log.Printf("warning: %s pg_stat_replication unavailable: %v", logPrefix, err)
	}

	var status, senderHost, conninfo sql.NullString
	err = db.QueryRowContext(ctx, `
		select status, coalesce(sender_host,''), coalesce(conninfo,'')
		from pg_stat_wal_receiver
		limit 1
	`).Scan(&status, &senderHost, &conninfo)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		log.Printf("warning: %s pg_stat_wal_receiver unavailable: %v", logPrefix, err)
	} else if err == nil {
		host := firstNonEmpty(senderHost.String, parseConninfoHost(conninfo.String))
		if host != "" || status.Valid {
			edges = append(edges, ReplicationEdge{
				FromMachine: mapHostToMachine(host, hostIndex),
				ToMachine:   machineID,
				ToDB:        cfg.ID,
				Type:        "postgres-upstream",
				Notes:       firstNonEmpty(status.String, "wal_receiver"),
			})
		} else {
			log.Printf("info: %s wal_receiver row has empty host/status", logPrefix)
		}
	}

	// subscriptions (downstream/slave)
	subs, err := db.QueryContext(ctx, `
		select coalesce(subname,''), coalesce(conninfo,''), coalesce(publications,''), coalesce(phase,'')
		from pg_stat_subscription
	`)
	if err == nil {
		defer subs.Close()
		for subs.Next() {
			var subname, conninfoStr, pubs, phase string
			if err := subs.Scan(&subname, &conninfoStr, &pubs, &phase); err != nil {
				log.Printf("warning: %s parse pg_stat_subscription: %v", logPrefix, err)
				break
			}
			host := parseConninfoHost(conninfoStr)
			if host != "" {
				edges = append(edges, ReplicationEdge{
					FromMachine: mapHostToMachine(host, hostIndex),
					ToMachine:   machineID,
					ToDB:        cfg.ID,
					Type:        "postgres-subscription",
					Notes:       firstNonEmpty(subname, pubs, phase),
				})
			}
		}
		if err := subs.Err(); err != nil {
			log.Printf("warning: %s iterate pg_stat_subscription: %v", logPrefix, err)
		}
	} else {
		// older PG versions don't expose conninfo in pg_stat_subscription; try pg_subscription
		if strings.Contains(strings.ToLower(err.Error()), "column \"conninfo\" does not exist") {
			log.Printf("info: %s pg_stat_subscription missing conninfo; trying pg_subscription catalog", logPrefix)
			subRows, subErr := db.QueryContext(ctx, `
				select coalesce(subname,''), coalesce(conninfo,''), coalesce(array_to_string(publist, ','),'')
				from pg_catalog.pg_subscription
			`)
			if subErr == nil {
				defer subRows.Close()
				for subRows.Next() {
					var subname, conninfoStr, pubs string
					if err := subRows.Scan(&subname, &conninfoStr, &pubs); err != nil {
						log.Printf("warning: %s parse pg_subscription: %v", logPrefix, err)
						break
					}
					host := parseConninfoHost(conninfoStr)
					if host != "" {
						edges = append(edges, ReplicationEdge{
							FromMachine: mapHostToMachine(host, hostIndex),
							ToMachine:   machineID,
							ToDB:        cfg.ID,
							Type:        "postgres-subscription",
							Notes:       firstNonEmpty(subname, pubs, hostForLog),
						})
					}
				}
				if err := subRows.Err(); err != nil {
					log.Printf("warning: %s iterate pg_subscription: %v", logPrefix, err)
				}
			} else {
				log.Printf("info: %s pg_subscription unavailable: %v", logPrefix, subErr)
			}
		} else {
			log.Printf("info: %s pg_stat_subscription unavailable: %v", logPrefix, err)
		}
	}

	if len(edges) == 0 {
		log.Printf("info: %s found no replication links; check permissions (need pg_monitor or superuser) and that replication is active", logPrefix)
	}

	return edges
}

func buildDSN(cfg DatabaseConfig) (string, error) {
	driver := strings.ToLower(cfg.Driver)
	if driver == "" {
		driver = "postgres"
	}
	if driver != "postgres" && driver != "postgresql" && driver != "pgx" {
		return "", fmt.Errorf("unsupported driver %q", cfg.Driver)
	}
	host := cfg.Host
	port := cfg.Port
	if port == 0 {
		port = 5432
	}
	sslmode := cfg.SSLMode
	if sslmode == "" {
		sslmode = "disable"
	}
	parts := []string{
		"host=" + host,
		"port=" + strconv.Itoa(port),
		"user=" + cfg.User,
		"dbname=" + cfg.Database,
		"sslmode=" + sslmode,
	}
	if cfg.Password != "" {
		parts = append(parts, "password="+cfg.Password)
	}
	if strings.TrimSpace(cfg.SSLRootCA) != "" {
		parts = append(parts, "sslrootcert="+cfg.SSLRootCA)
	}
	return strings.Join(parts, " "), nil
}

const htmlTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>DB Barrel Topology</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg1: #f7f1e5;
      --bg2: #eddcc2;
      --bg3: #e3cfae;
      --panel: rgba(255, 252, 245, 0.94);
      --panel-border: rgba(120, 84, 40, 0.14);
      --text: #2b1f14;
      --muted: #6b5744;
      --accent: #c47a32;
      --accent-2: #8a5a2e;
      --success: #3f7f5f;
      --link: #d9a441;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Space Grotesk", "SF Pro Display", "Segoe UI", system-ui, -apple-system, sans-serif;
      color: var(--text);
      background: radial-gradient(130% 120% at 15% 20%, var(--bg3) 0%, var(--bg2) 50%, var(--bg1) 100%);
      min-height: 100vh;
      overflow: hidden;
    }
    #graph {
      width: 100vw;
      height: 100vh;
      background: linear-gradient(135deg, rgba(196, 122, 50, 0.08), rgba(138, 90, 46, 0.07)), radial-gradient(140% 120% at 80% 20%, rgba(217, 164, 65, 0.16), rgba(247, 241, 229, 0.42));
    }
    .overlay {
      position: absolute;
      top: 16px;
      left: 16px;
      display: flex;
      flex-direction: column;
      gap: 10px;
      background: var(--panel);
      border: 1px solid var(--panel-border);
      border-radius: 14px;
      padding: 14px 16px;
      backdrop-filter: blur(12px);
      box-shadow: 0 12px 40px rgba(0, 0, 0, 0.35);
      z-index: 10;
      max-width: 360px;
    }
    .title {
      font-size: 20px;
      font-weight: 600;
      letter-spacing: 0.4px;
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .title::before {
      content: "";
      display: inline-block;
      width: 12px;
      height: 12px;
      border-radius: 50%;
      background: radial-gradient(circle at 30% 30%, #e2a148, #c47a32);
      box-shadow: 0 0 12px rgba(201, 138, 49, 0.8);
    }
    .subtitle { color: var(--muted); font-size: 13px; }
    .badges { display: flex; gap: 8px; flex-wrap: wrap; }
    .badge {
      background: rgba(255, 255, 255, 0.06);
      border: 1px solid var(--panel-border);
      border-radius: 999px;
      padding: 6px 10px;
      font-size: 12px;
      color: var(--muted);
    }
    .legend {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px 10px;
      font-size: 12px;
      color: var(--muted);
    }
    .legend span { display: flex; align-items: center; gap: 8px; }
    .dot {
      width: 10px; height: 10px; border-radius: 50%;
      box-shadow: 0 0 10px rgba(0, 0, 0, 0.15);
    }
    .dot.db { background: var(--accent); }
    .dot.instance { background: #5b3b22; }
    .dot.machine { background: var(--accent-2); }
    .dot.link { background: var(--link); }
    .title-row { display:flex; align-items:center; gap:12px; }
    .pill {
      border: 1px solid var(--panel-border);
      background: rgba(255, 255, 255, 0.06);
      color: var(--text);
      padding: 8px 12px;
      border-radius: 10px;
      font-size: 13px;
      cursor: pointer;
      transition: transform 120ms ease, border-color 120ms ease;
    }
    .pill:hover { transform: translateY(-1px); border-color: rgba(255, 255, 255, 0.18); }
    .logo-btn {
      width: 40px; height: 40px; padding: 4px;
      border-radius: 10px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      background: linear-gradient(135deg, #f4e3c9, #eddcc2);
      border: 1px solid var(--panel-border);
      box-shadow: inset 0 -2px 6px rgba(0,0,0,0.25), 0 6px 16px rgba(0,0,0,0.3);
    }
    .logo-img {
      width: 100%;
      height: 100%;
      object-fit: contain;
      filter: drop-shadow(0 0 8px rgba(160,90,44,0.35));
    }
    #backBtn {
      display: none;
      position: absolute;
      top: 16px;
      right: 16px;
      z-index: 10;
    }
    #tooltip {
      position: absolute;
      pointer-events: none;
      background: linear-gradient(160deg, rgba(255, 252, 245, 0.98), rgba(247, 232, 206, 0.98));
      border: 1px solid rgba(120, 84, 40, 0.18);
      border-radius: 12px;
      padding: 10px 12px;
      font-size: 12px;
      color: #2b1f14;
      box-shadow: 0 12px 28px rgba(43, 31, 20, 0.18);
      opacity: 0;
      transition: opacity 100ms ease;
      max-width: 260px;
      backdrop-filter: blur(8px);
    }
    #tooltip.visible { opacity: 1; }
    .badge strong { color: var(--text); }
  </style>
</head>
<body>
  <div class="overlay">
    <div class="title-row">
      <button id="resetBtn" class="pill logo-btn" title="Reset view">
        <img class="logo-img" src="{{ .Logo }}" alt="Barrel logo" />
      </button>
      <div class="title">DB Barrel</div>
    </div>
    <div class="subtitle" id="subtitle">Generating...</div>
    <div class="badges">
      <span class="badge">Machines: <strong id="machineCount">-</strong></span>
      <span class="badge">Databases: <strong id="dbCount">-</strong></span>
      <span class="badge">Replication links: <strong id="linkCount">-</strong></span>
      <span class="badge">Generated: <strong id="generatedAt">-</strong></span>
    </div>
    <div class="legend">
      <span><span class="dot db"></span>Database</span>
      <span><span class="dot instance"></span>Instance outline</span>
      <span><span class="dot machine"></span>Machine outline</span>
      <span><span class="dot link"></span>Replication link</span>
    </div>
    <div style="display:flex; gap:8px; flex-wrap: wrap; margin-top: 4px;">
      <button id="backBtn" class="pill">Back to replication</button>
    </div>
  </div>
  <div id="tooltip"></div>
  <svg id="graph" width="1200" height="720"></svg>

  <script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.8.5/d3.min.js"></script>
  <script>
    const width = window.innerWidth;
    const height = window.innerHeight;
  const svg = d3.select("#graph")
      .attr("viewBox", [0, 0, width, height]);

    const payload = {{ .Payload }};
    const dbIconUri = {{ .DbIcon }};
    const backBtn = document.getElementById("backBtn");
    const resetBtn = document.getElementById("resetBtn");
    const tooltip = document.getElementById("tooltip");

    const machineCount = (payload.machines || []).length;
    const dbCount = (payload.machines || []).reduce((sum, m) => sum + (m.databases || []).length, 0);
    const linkCount = (payload.replication || []).length;

    document.getElementById("machineCount").textContent = machineCount;
    document.getElementById("dbCount").textContent = dbCount;
    document.getElementById("linkCount").textContent = linkCount;
    document.getElementById("generatedAt").textContent = payload.generated_at ? new Date(payload.generated_at).toLocaleString() : "n/a";
    document.getElementById("subtitle").textContent = "Double-click a node for schema; drag to explore.";

    function clearSvg() { svg.selectAll("*").remove(); }

    function groupBy(arr, keyFn) {
      const out = {};
      arr.forEach(item => {
        const k = keyFn(item);
        if (!out[k]) out[k] = [];
        out[k].push(item);
      });
      return out;
    }

    function replicationNodesLinks() {
      const nodes = [];
      const links = [];
      (payload.machines || []).forEach(m => {
        (m.databases || []).forEach(db => {
          if ((db.name || db.id || "").toLowerCase() === "postgres") return;
          nodes.push({
            id: db.id,
            label: db.name || db.id,
            machine: m.id,
            instance: (db.host || m.id) + ":" + (db.port || ""),
          });
        });
      });
      (payload.replication || []).forEach(r => {
        links.push({
          source: r.from_db || findFirstDb(r.from_machine),
          target: r.to_db || findFirstDb(r.to_machine),
          label: r.type || "replication",
          lag: r.lag_seconds
        });
      });
      return { nodes, links };
    }

    function findFirstDb(machineId) {
      const m = (payload.machines || []).find(mm => mm.id === machineId);
      return m?.databases?.[0]?.id || machineId;
    }

    function labelForLink(d) {
      const lag = (d.lag || d.lag === 0) ? " (" + d.lag + "s)" : "";
      return (d.label || "replication") + lag;
    }

    function drawDbIcon(selection) {
      selection.append("image")
        .attr("href", dbIconUri)
        .attr("x", -28)
        .attr("y", -28)
        .attr("width", 56)
        .attr("height", 56);
    }

  function drawOutlines(layer, groups, style) {
    Object.values(groups).forEach(nodes => {
      if (!nodes.length) return;
      const minX = d3.min(nodes, d => d.x);
      const maxX = d3.max(nodes, d => d.x);
      const minY = d3.min(nodes, d => d.y);
      const maxY = d3.max(nodes, d => d.y);
      layer.append("rect")
        .attr("x", minX - 70)
        .attr("y", minY - 70)
        .attr("width", (maxX - minX) + 140)
        .attr("height", (maxY - minY) + 140)
        .attr("rx", 14)
        .attr("ry", 14)
        .attr("fill", style.fill || "none")
        .attr("stroke", style.stroke)
        .attr("stroke-width", style.width)
        .attr("stroke-dasharray", style.dash)
        .attr("opacity", style.opacity ?? 0.7)
        .attr("pointer-events", "none");
    });
  }

  function boundsForGroups(groups) {
    const result = [];
    Object.entries(groups).forEach(([key, nodes]) => {
      if (!nodes.length) return;
      const minX = d3.min(nodes, d => d.x);
      const maxX = d3.max(nodes, d => d.x);
      const minY = d3.min(nodes, d => d.y);
      const maxY = d3.max(nodes, d => d.y);
      result.push({
        id: key,
        minX, maxX, minY, maxY,
        cx: (minX + maxX) / 2,
        cy: (minY + maxY) / 2 - 60
      });
    });
    return result;
  }

    function drag(sim) {
      function dragstarted(event, d) {
        if (!event.active) sim.alphaTarget(0.3).restart();
        d.fx = d.x; d.fy = d.y;
      }
      function dragged(event, d) { d.fx = event.x; d.fy = event.y; }
      function dragended(event, d) { if (!event.active) sim.alphaTarget(0); }
      return d3.drag().on("start", dragstarted).on("drag", dragged).on("end", dragended);
    }

  function attachTooltip(selection, formatter) {
      selection
        .on("mouseover", (event, d) => {
          tooltip.innerHTML = formatter(d);
          tooltip.style.left = (event.pageX + 12) + "px";
          tooltip.style.top = (event.pageY + 12) + "px";
          tooltip.classList.add("visible");
        })
        .on("mousemove", (event) => {
          tooltip.style.left = (event.pageX + 12) + "px";
          tooltip.style.top = (event.pageY + 12) + "px";
        })
        .on("mouseout", () => {
          tooltip.classList.remove("visible");
        });
    }

    function renderReplicationGraph() {
      backBtn.style.display = "none";
      document.getElementById("subtitle").textContent = "Replication view. Double-click a DB to see its schema.";
      updateLegend("replication");
      clearSvg();

      const { nodes, links } = replicationNodesLinks();
      const defs = svg.append("defs");
      defs.append("marker")
          .attr("id", "arrow")
          .attr("viewBox", "0 0 10 10")
          .attr("refX", 16)
          .attr("refY", 5)
          .attr("markerWidth", 10)
          .attr("markerHeight", 10)
          .attr("orient", "auto")
        .append("path")
          .attr("d", "M 0 0 L 10 5 L 0 10 z")
          .attr("fill", "var(--link)");

      const zoomLayer = svg.append("g");
      const zoomBehavior = d3.zoom().scaleExtent([0.35, 2.5]).on("zoom", (event) => zoomLayer.attr("transform", event.transform));
      svg.call(zoomBehavior);
      svg.call(zoomBehavior.transform, d3.zoomIdentity.scale(0.8).translate(80, 40));

      const machineIds = Array.from(new Set((payload.machines || []).map(m => m.id)));
      const radius = Math.min(width, height) * 0.42;
      const machineCenters = {};
      machineIds.forEach((id, idx) => {
        const angle = (2 * Math.PI * idx) / Math.max(machineIds.length, 1);
        machineCenters[id] = {
          x: width / 2 + Math.cos(angle) * radius,
          y: height / 2 + Math.sin(angle) * radius,
        };
      });

      const instanceCenters = {};
      (payload.machines || []).forEach(m => {
        const dbs = (m.databases || []).filter(db => (db.name || db.id || "").toLowerCase() !== "postgres");
        const uniqInstances = Array.from(new Set(dbs.map(db => (db.host || m.id) + ":" + (db.port || ""))));
        const instRadius = 260;
        uniqInstances.forEach((inst, idx) => {
          const angle = (2 * Math.PI * idx) / Math.max(uniqInstances.length, 1);
          const mc = machineCenters[m.id] || { x: width / 2, y: height / 2 };
          instanceCenters[inst] = {
            x: mc.x + Math.cos(angle) * instRadius,
            y: mc.y + Math.sin(angle) * instRadius,
          };
        });
      });

      // radial positions per machine/instance for initial layout
      const simNodes = nodes.map(n => {
        const instPos = instanceCenters[n.instance] || machineCenters[n.machine] || { x: width / 2, y: height / 2 };
        return { ...n, x: instPos.x, y: instPos.y };
      });
      const sim = d3.forceSimulation(simNodes)
        .force("link", d3.forceLink(links).id(d => d.id).distance(180))
        .force("charge", d3.forceManyBody().strength(-150))
        .force("collision", d3.forceCollide().radius(70))
      .force("machineX", d3.forceX(d => machineCenters[d.machine]?.x || width / 2).strength(0.08))
      .force("machineY", d3.forceY(d => machineCenters[d.machine]?.y || height / 2).strength(0.08))
        .force("instanceX", d3.forceX(d => instanceCenters[d.instance]?.x || width / 2).strength(0.1))
        .force("instanceY", d3.forceY(d => instanceCenters[d.instance]?.y || height / 2).strength(0.1));

      const link = zoomLayer.append("g")
          .attr("stroke", "rgba(217, 164, 65, 0.8)")
          .attr("stroke-width", 2)
        .selectAll("line")
        .data(links)
        .join("line")
          .attr("marker-end", "url(#arrow)");

      const linkLabel = zoomLayer.append("g")
        .selectAll("text")
        .data(links)
        .join("text")
          .attr("class", "lag")
          .attr("fill", "#6b5744")
          .attr("font-size", 11)
          .text(d => labelForLink(d));

      const node = zoomLayer.append("g")
        .selectAll("g")
        .data(simNodes)
        .join("g")
          .style("cursor", "pointer")
          .on("dblclick", (_, d) => renderDbGraph(d.id))
          .call(drag(sim));

      drawDbIcon(node);
      attachTooltip(node, d => '<strong>' + d.label + '</strong><br/>Machine: ' + d.machine + '<br/>Instance: ' + d.instance);

      node.append("text")
        .attr("text-anchor", "middle")
        .attr("dy", 40)
        .attr("fill", "#2b1f14")
        .style("font", "12px \"Space Grotesk\", system-ui, sans-serif")
        .text(d => d.label);

      const machines = groupBy(simNodes, n => n.machine);
      const instances = groupBy(simNodes, n => n.instance);
      const machineLayer = zoomLayer.append("g");
      const instanceLayer = zoomLayer.append("g");
      const machineLabelLayer = zoomLayer.append("g");
      const instanceLabelLayer = zoomLayer.append("g");

      sim.on("tick", () => {
        link
          .attr("x1", d => d.source.x)
          .attr("y1", d => d.source.y)
          .attr("x2", d => d.target.x)
          .attr("y2", d => d.target.y);

        linkLabel
          .attr("x", d => (d.source.x + d.target.x) / 2)
          .attr("y", d => (d.source.y + d.target.y) / 2 - 6);

        node.attr("transform", d => 'translate(' + d.x + ',' + d.y + ')');

        machineLayer.selectAll("*").remove();
        instanceLayer.selectAll("*").remove();
        machineLabelLayer.selectAll("*").remove();
        instanceLabelLayer.selectAll("*").remove();

        const machineGroups = groupBy(simNodes, n => n.machine);
        const instanceGroups = groupBy(simNodes, n => n.instance);
        drawOutlines(machineLayer, machineGroups, { stroke: "var(--accent-2)", width: 2, dash: "6,4", fill: "rgba(196,122,50,0.08)", opacity: 0.82 });
        drawOutlines(instanceLayer, instanceGroups, { stroke: "#5b3b22", width: 1.6, dash: "0", fill: "rgba(91,59,34,0.07)", opacity: 0.6 });

        const machineBounds = boundsForGroups(machineGroups);
        machineLabelLayer.selectAll("text")
          .data(machineBounds)
          .join("text")
          .attr("x", d => d.cx)
          .attr("y", d => d.minY - 80)
          .attr("text-anchor", "middle")
          .attr("fill", "var(--accent-2)")
          .style("font", "11px \"Space Grotesk\", system-ui, sans-serif")
          .text(d => d.id);

      const instanceBounds = boundsForGroups(instanceGroups);
      instanceLabelLayer.selectAll("text")
        .data(instanceBounds)
        .join("text")
        .attr("x", d => d.cx)
        .attr("y", d => d.minY - 50)
        .attr("text-anchor", "middle")
        .attr("fill", "#2b1f14")
        .style("font", "10px \"Space Grotesk\", system-ui, sans-serif")
        .text(d => d.id);
      });
    }

  function renderDbGraph(dbId) {
    const dbNode = (payload.machines || []).flatMap(m => m.databases || []).find(d => d.id === dbId);
    if (!dbNode) return;
    const machine = (payload.machines || []).find(m => (m.databases || []).some(d => d.id === dbId));
    backBtn.style.display = "none";
    document.getElementById("subtitle").textContent = "";
    clearSvg();

    const tables = [];
    let totalColumns = 0;
    (dbNode.tables || []).forEach(tbl => {
      tables.push({
        id: (dbNode.id || dbNode.name || "db") + ":" + (tbl.schema ? tbl.schema + "." : "") + tbl.name,
        label: (tbl.schema ? tbl.schema + "." : "") + tbl.name,
        columns: tbl.columns || [],
        dbId: dbNode.id || dbNode.name || "db"
      });
      totalColumns += (tbl.columns || []).length;
    });

    const fkLinks = [];
    (dbNode.foreign_keys || []).forEach(fk => {
      const sourceId = (dbNode.id || dbNode.name || "db") + ":" + (fk.schema ? fk.schema + "." : "") + fk.table;
      const targetId = (dbNode.id || dbNode.name || "db") + ":" + (fk.ref_schema ? fk.ref_schema + "." : "") + fk.ref_table;
      if (sourceId !== targetId) {
        fkLinks.push({
          source: sourceId,
          target: targetId,
          label: fk.column + " → " + fk.ref_column,
          srcCol: fk.column,
          tgtCol: fk.ref_column,
        });
      }
    });

    const zoomLayer = svg.append("g");
    const tableZoom = d3.zoom().scaleExtent([0.25, 2.5]).on("zoom", (event) => zoomLayer.attr("transform", event.transform));
    svg.call(tableZoom);

    const tableWidth = Math.min(340, Math.max(240, width * 0.24));
    const headerHeight = 32;
    const baseHeight = headerHeight + 14;
    const rowHeight = 18;

    // lay out tables in a grid and allow per-node dragging
    const cols = Math.max(1, Math.floor(width / (tableWidth + 60)));
    tables.forEach((t, idx) => {
      const col = idx % cols;
      const row = Math.floor(idx / cols);
      t.x = 100 + col * (tableWidth + 60);
      t.y = 140 + row * 260;
    });

    const tableById = {};
    tables.forEach(t => { tableById[t.id] = t; });
    const fkLinksData = fkLinks
      .map(l => ({ ...l, source: tableById[l.source], target: tableById[l.target] }))
      .filter(l => l.source && l.target);

    updateLegend("schema", { tables: tables.length, columns: totalColumns, fks: fkLinksData.length });

    const defs = svg.append("defs");
    defs.append("marker")
      .attr("id", "fk-arrow")
      .attr("viewBox", "0 0 10 10")
      .attr("refX", 8)
      .attr("refY", 5)
      .attr("markerWidth", 8)
      .attr("markerHeight", 8)
      .attr("orient", "auto")
      .append("path")
      .attr("d", "M 0 0 L 10 5 L 0 10 z")
      .attr("fill", "var(--link)");

    const headerGrad = defs.append("linearGradient")
      .attr("id", "tableHeaderGrad")
      .attr("x1", "0%").attr("x2", "100%")
      .attr("y1", "0%").attr("y2", "0%");
    headerGrad.append("stop").attr("offset", "0%").attr("stop-color", "#8a5a2e");
    headerGrad.append("stop").attr("offset", "100%").attr("stop-color", "#c47a32");

    let tableSim;
    const dragTables = d3.drag()
      .on("start", (event, d) => {
        d.fx = d.x; d.fy = d.y;
        if (tableSim) tableSim.alphaTarget(0.3).restart();
      })
      .on("drag", (event, d) => { d.fx = event.x; d.fy = event.y; redraw(); })
      .on("end", (event, d) => { d.fx = d.fy = null; if (tableSim) tableSim.alphaTarget(0); });

    const node = zoomLayer.append("g")
      .selectAll("g")
      .data(tables)
      .join("g")
        .style("cursor", "move")
        .call(dragTables);

    const fkLine = zoomLayer.append("g")
      .attr("stroke", "var(--link)")
      .attr("stroke-width", 1.4)
      .attr("stroke-opacity", 0.7)
      .selectAll("line")
      .data(fkLinksData)
      .join("line")
      .attr("marker-end", "url(#fk-arrow)");

    const fkLabel = zoomLayer.append("g")
      .selectAll("text")
      .data(fkLinksData)
      .join("text")
      .attr("fill", "var(--link)")
      .attr("font-size", 11)
      .style("font", "11px \"Space Grotesk\", system-ui, sans-serif")
      .text(d => d.label);

    node.append("rect")
      .attr("x", -tableWidth / 2)
      .attr("y", d => - (baseHeight + d.columns.length * rowHeight) / 2)
      .attr("width", tableWidth)
      .attr("height", d => baseHeight + d.columns.length * rowHeight)
      .attr("rx", 12)
      .attr("ry", 12)
      .attr("fill", "#1b120a")
      .attr("stroke", "#c47a32")
      .attr("stroke-width", 1.2)
      .attr("filter", "drop-shadow(0 8px 18px rgba(0, 0, 0, 0.25))");

    node.append("rect")
      .attr("x", -tableWidth / 2)
      .attr("y", d => - (baseHeight + d.columns.length * rowHeight) / 2)
      .attr("width", tableWidth)
      .attr("height", headerHeight)
      .attr("rx", 12)
      .attr("ry", 12)
      .attr("fill", "url(#tableHeaderGrad)")
      .attr("opacity", 0.92);

    node.append("text")
      .attr("text-anchor", "middle")
      .attr("dy", d => - (baseHeight + d.columns.length * rowHeight) / 2 + headerHeight / 2 + 4)
      .attr("fill", "#ffffff")
      .style("font", "13px \"Space Grotesk\", system-ui, sans-serif")
      .style("font-weight", "600")
      .text(d => d.label);

    node.each(function(d) {
      const g = d3.select(this);
      d.anchorMap = d.anchorMap || {};
      d.columns.forEach((col, i) => {
        const y = - (baseHeight + d.columns.length * rowHeight) / 2 + headerHeight + 6 + (i + 1) * rowHeight;
        d.anchorMap[col.name] = y;
        g.append("rect")
          .attr("x", -tableWidth / 2 + 6)
          .attr("y", y - rowHeight + 2)
          .attr("width", tableWidth - 12)
          .attr("height", rowHeight)
          .attr("rx", 4)
          .attr("ry", 4)
          .attr("fill", i % 2 === 0 ? "rgba(43, 31, 20, 0.4)" : "rgba(79, 52, 31, 0.35)")
          .attr("stroke", "rgba(196, 122, 50, 0.35)")
          .attr("stroke-width", 0.5);
        g.append("text")
          .attr("x", -tableWidth / 2 + 12)
          .attr("y", y)
          .attr("fill", "#f5f3ef")
          .style("font", "12px \"Space Grotesk\", monospace")
          .text(col.name);
        g.append("text")
          .attr("x", tableWidth / 2 - 10)
          .attr("y", y)
          .attr("fill", "#d9a441")
          .style("font", "11px \"Space Grotesk\", monospace")
          .attr("text-anchor", "end")
          .text(col.data_type + (col.is_nullable ? "" : " not null"));
      });
    });

    attachTooltip(node, d => '<strong>' + d.label + '</strong><br/>' + d.columns.length + ' columns');

    function focusOnTables() {
      if (!tables.length) return;
      const padding = 200;
      const minX = d3.min(tables, d => d.x);
      const maxX = d3.max(tables, d => d.x);
      const minY = d3.min(tables, d => d.y);
      const maxY = d3.max(tables, d => d.y);
      const contentW = Math.max(10, maxX - minX);
      const contentH = Math.max(10, maxY - minY);
      const scale = Math.max(0.35, Math.min(1.4, 0.9 * Math.min(width / (contentW + padding), height / (contentH + padding))));
      const tx = width / 2 - scale * (minX + contentW / 2);
      const ty = height / 2 - scale * (minY + contentH / 2);
      const target = d3.zoomIdentity.translate(tx, ty).scale(scale);
      svg.call(tableZoom.transform, d3.zoomIdentity);
      svg.transition().duration(400).call(tableZoom.transform, target);
    }

    function redraw() {
      const anchorPos = (tbl, colName, otherX) => {
        const dir = (otherX - tbl.x) >= 0 ? 1 : -1;
        const yRel = (tbl.anchorMap && tbl.anchorMap[colName]) || 0;
        const x = tbl.x + dir * (tableWidth / 2);
        const y = tbl.y + yRel;
        return { x, y };
      };

      fkLine
        .attr("x1", d => anchorPos(d.source, d.srcCol, d.target.x).x)
        .attr("y1", d => anchorPos(d.source, d.srcCol, d.target.x).y)
        .attr("x2", d => anchorPos(d.target, d.tgtCol, d.source.x).x)
        .attr("y2", d => anchorPos(d.target, d.tgtCol, d.source.x).y);

      fkLabel
        .attr("x", d => (anchorPos(d.source, d.srcCol, d.target.x).x + anchorPos(d.target, d.tgtCol, d.source.x).x) / 2)
        .attr("y", d => (anchorPos(d.source, d.srcCol, d.target.x).y + anchorPos(d.target, d.tgtCol, d.source.x).y) / 2 - 6);

      node.attr("transform", d => 'translate(' + d.x + ',' + d.y + ')');
    }

    redraw();
    tableSim = d3.forceSimulation(tables)
      .force("collision", d3.forceCollide().radius(d => Math.max(tableWidth / 2 + 22, (baseHeight + d.columns.length * rowHeight) / 2 + 22)).iterations(2))
      .force("charge", d3.forceManyBody().strength(-80))
      .on("tick", redraw);
    setTimeout(() => tableSim.stop(), 1200);
    setTimeout(focusOnTables, 200);
  }

function updateLegend(mode, meta = {}) {
      const legendEl = document.querySelector(".legend");
      if (!legendEl) return;
      legendEl.innerHTML = "";
      const items = mode === "schema"
        ? [
            { label: "Tables: " + ((meta.tables ?? "-")), color: "var(--accent)" },
            { label: "Columns: " + ((meta.columns ?? "-")), color: "rgba(91,59,34,0.8)" },
            { label: "Foreign keys: " + ((meta.fks ?? "-")), color: "var(--link)" }
          ]
        : [
            { label: "Database", color: "var(--accent)" },
            { label: "Instance outline (solid)", color: "#5b3b22" },
            { label: "Machine outline (dotted)", color: "var(--accent-2)" },
            { label: "Replication link", color: "var(--link)" }
          ];
      items.forEach(item => {
        const span = document.createElement("span");
        span.style.display = "flex";
        span.style.alignItems = "center";
        span.style.gap = "8px";
        span.style.color = "var(--muted)";
        const dot = document.createElement("span");
        dot.style.width = "10px";
        dot.style.height = "10px";
        dot.style.borderRadius = "50%";
        dot.style.background = item.color;
        dot.style.boxShadow = "0 0 10px " + item.color;
        span.appendChild(dot);
        const text = document.createTextNode(item.label);
        span.appendChild(text);
        legendEl.appendChild(span);
      });
    }

    backBtn.onclick = () => renderReplicationGraph();
    resetBtn.onclick = () => renderReplicationGraph();

    renderReplicationGraph();
  </script>
</body>
</html>
`
