"""PostgreSQL-only runtime storage. SQLite is read only by the explicit importer."""
import re
from contextlib import contextmanager
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

SCHEMA_VERSION = 4
TABLES = ('devices','media','tasks','events','archive_claims')
DDL = '''
CREATE TABLE IF NOT EXISTS gateway_schema (version INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS devices (
 tenant TEXT NOT NULL, id TEXT NOT NULL UNIQUE, token_hash TEXT UNIQUE NOT NULL,
 scope TEXT NOT NULL, revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0,1)),
 session TEXT, installation TEXT, hello TEXT, heartbeat TEXT, seen DOUBLE PRECISION,
 PRIMARY KEY(tenant,id));
CREATE TABLE IF NOT EXISTS media (
 tenant TEXT NOT NULL, id TEXT NOT NULL, sha256 TEXT NOT NULL, md5 TEXT NOT NULL,
 size BIGINT NOT NULL, mime TEXT NOT NULL, data BYTEA NOT NULL,
 PRIMARY KEY(tenant,id), UNIQUE(tenant,sha256));
CREATE TABLE IF NOT EXISTS tasks (
 tenant TEXT NOT NULL, id TEXT PRIMARY KEY, device TEXT NOT NULL,
 dedup TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL,
 created DOUBLE PRECISION NOT NULL, deadline DOUBLE PRECISION NOT NULL,
 lease_token TEXT, lease_until DOUBLE PRECISION, session TEXT,
 intent_at DOUBLE PRECISION, submitted_at DOUBLE PRECISION, updated DOUBLE PRECISION NOT NULL,
 UNIQUE(tenant,dedup), FOREIGN KEY(tenant,device) REFERENCES devices(tenant,id));
CREATE INDEX IF NOT EXISTS tasks_device_state ON tasks(tenant,device,state,created,id);
CREATE INDEX IF NOT EXISTS tasks_deadline ON tasks(deadline,tenant,device)
 WHERE state IN ('queued','leased');
CREATE INDEX IF NOT EXISTS tasks_lease_due ON tasks(lease_until,tenant,device)
 WHERE state IN ('leased','submitting','submitted');
CREATE TABLE IF NOT EXISTS events (
 seq BIGSERIAL PRIMARY KEY, tenant TEXT NOT NULL, task TEXT NOT NULL,
 event_id TEXT NOT NULL, at DOUBLE PRECISION NOT NULL, state TEXT NOT NULL, details TEXT NOT NULL,
 UNIQUE(task,event_id), FOREIGN KEY(task) REFERENCES tasks(id));
CREATE TABLE IF NOT EXISTS archive_claims (
 tenant TEXT NOT NULL, event_id TEXT NOT NULL, task TEXT NOT NULL,
 PRIMARY KEY(tenant,event_id), FOREIGN KEY(task) REFERENCES tasks(id));
CREATE TABLE IF NOT EXISTS gateway_imports (
 source_sha256 TEXT PRIMARY KEY, imported_at TIMESTAMPTZ NOT NULL DEFAULT now(), summary TEXT NOT NULL);
'''
UPGRADE = '''
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS desktop_released_at DOUBLE PRECISION;
CREATE UNIQUE INDEX IF NOT EXISTS one_desktop_execution_per_device ON tasks(tenant,device)
 WHERE state IN ('leased','submitting') OR (state IN ('submitted','unknown') AND desktop_released_at IS NULL);
DROP INDEX IF EXISTS one_active_task_per_device;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 0;
ALTER TABLE media ALTER COLUMN data DROP NOT NULL;
ALTER TABLE media ADD COLUMN IF NOT EXISTS object_key TEXT;
ALTER TABLE media ADD COLUMN IF NOT EXISTS created DOUBLE PRECISION NOT NULL DEFAULT EXTRACT(EPOCH FROM now());
ALTER TABLE media ADD COLUMN IF NOT EXISTS deleting BOOLEAN NOT NULL DEFAULT false;
CREATE TABLE IF NOT EXISTS outbox (
 id BIGSERIAL PRIMARY KEY, tenant TEXT NOT NULL, device TEXT NOT NULL, revision BIGINT NOT NULL,
 kind TEXT NOT NULL, created DOUBLE PRECISION NOT NULL, delivered_at DOUBLE PRECISION);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(id) WHERE delivered_at IS NULL;
CREATE INDEX IF NOT EXISTS outbox_pending_age ON outbox(created) WHERE delivered_at IS NULL;
CREATE INDEX IF NOT EXISTS tasks_alert_states ON tasks(state) WHERE state IN ('queued','unknown');
CREATE INDEX IF NOT EXISTS tasks_media_references ON tasks USING gin ((payload::jsonb->'media_ids'));
CREATE TABLE IF NOT EXISTS media_uploads (
 tenant TEXT NOT NULL, id TEXT PRIMARY KEY, staging_key TEXT NOT NULL UNIQUE,
 expected_sha256 TEXT NOT NULL, expected_md5 TEXT NOT NULL, expected_size BIGINT NOT NULL,
 state TEXT NOT NULL, created DOUBLE PRECISION NOT NULL, updated DOUBLE PRECISION NOT NULL,
 media_id TEXT, reason TEXT NOT NULL DEFAULT '', lease_until DOUBLE PRECISION, attempts INTEGER NOT NULL DEFAULT 0);
ALTER TABLE media_uploads ADD COLUMN IF NOT EXISTS validation_seconds DOUBLE PRECISION;
CREATE INDEX IF NOT EXISTS media_uploads_ready ON media_uploads(updated) WHERE state='ready';
CREATE INDEX IF NOT EXISTS media_uploads_pending ON media_uploads(state,updated);
CREATE TABLE IF NOT EXISTS worker_health (
 name TEXT PRIMARY KEY, updated DOUBLE PRECISION NOT NULL, error_type TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS retention_audit (
 id BIGSERIAL PRIMARY KEY, at DOUBLE PRECISION NOT NULL, kind TEXT NOT NULL,
 affected BIGINT NOT NULL, details TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS alert_deliveries (id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,payload TEXT NOT NULL,state TEXT NOT NULL,created DOUBLE PRECISION NOT NULL,next_at DOUBLE PRECISION NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,delivered_at DOUBLE PRECISION,last_error TEXT NOT NULL DEFAULT '');
ALTER TABLE alert_deliveries ADD COLUMN IF NOT EXISTS remote_message_id BIGINT;
CREATE INDEX IF NOT EXISTS alert_pending ON alert_deliveries(next_at) WHERE state='queued';
CREATE TABLE IF NOT EXISTS source_routes (
 tenant TEXT NOT NULL, inbox TEXT NOT NULL, device TEXT NOT NULL,
 mode TEXT NOT NULL CHECK(mode IN ('shadow','gateway','paused')),
 floor BIGINT NOT NULL, cursor BIGINT NOT NULL, generation BIGINT NOT NULL DEFAULT 1,
 configured_at DOUBLE PRECISION NOT NULL, client_ready_at DOUBLE PRECISION,
 PRIMARY KEY(tenant,inbox));
CREATE TABLE IF NOT EXISTS source_jobs (
 tenant TEXT NOT NULL, inbox TEXT NOT NULL, message_id BIGINT NOT NULL,
 task TEXT REFERENCES tasks(id), state TEXT NOT NULL DEFAULT 'pending',
 error_type TEXT NOT NULL DEFAULT '', attempts INTEGER NOT NULL DEFAULT 0,
 next_at DOUBLE PRECISION NOT NULL DEFAULT 0, reported_state TEXT,
 PRIMARY KEY(tenant,inbox,message_id));
CREATE TABLE IF NOT EXISTS maintenance_cursors (name TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS media_gc (
 object_key TEXT PRIMARY KEY, created DOUBLE PRECISION NOT NULL);
ALTER TABLE source_jobs ADD COLUMN IF NOT EXISTS enqueued_at DOUBLE PRECISION NOT NULL DEFAULT EXTRACT(EPOCH FROM now());
ALTER TABLE source_jobs ADD COLUMN IF NOT EXISTS source_created_at DOUBLE PRECISION;
ALTER TABLE source_jobs ADD COLUMN IF NOT EXISTS queue_until DOUBLE PRECISION;
ALTER TABLE source_jobs ADD COLUMN IF NOT EXISTS deadline DOUBLE PRECISION;
ALTER TABLE source_jobs ADD COLUMN IF NOT EXISTS started_at DOUBLE PRECISION;
ALTER TABLE source_jobs ADD COLUMN IF NOT EXISTS attempt_until DOUBLE PRECISION;
ALTER TABLE source_jobs ADD COLUMN IF NOT EXISTS lease_token TEXT;
ALTER TABLE source_jobs ADD COLUMN IF NOT EXISTS lease_until DOUBLE PRECISION;
ALTER TABLE source_jobs ADD COLUMN IF NOT EXISTS route_generation BIGINT;
ALTER TABLE source_jobs ADD COLUMN IF NOT EXISTS report_token TEXT;
ALTER TABLE source_jobs ADD COLUMN IF NOT EXISTS report_until DOUBLE PRECISION;
ALTER TABLE source_jobs ADD COLUMN IF NOT EXISTS report_next_at DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE source_jobs ADD COLUMN IF NOT EXISTS report_error TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS source_pending_jobs ON source_jobs(tenant,inbox,next_at,message_id) WHERE task IS NULL AND state IN ('pending','preparing');
CREATE INDEX IF NOT EXISTS source_active_leases ON source_jobs(lease_until) WHERE task IS NULL AND state='preparing';
CREATE TABLE IF NOT EXISTS source_scheduling (
 tenant TEXT NOT NULL,inbox TEXT NOT NULL,last_claimed DOUBLE PRECISION NOT NULL DEFAULT 0,
 capture_owner TEXT,capture_until DOUBLE PRECISION,PRIMARY KEY(tenant,inbox));
CREATE TABLE IF NOT EXISTS source_runtime_limits (name TEXT PRIMARY KEY,value INTEGER NOT NULL);
ALTER TABLE source_scheduling ADD COLUMN IF NOT EXISTS last_reported DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE worker_health ADD COLUMN IF NOT EXISTS last_progress DOUBLE PRECISION;
ALTER TABLE worker_health ADD COLUMN IF NOT EXISTS active_jobs INTEGER NOT NULL DEFAULT 0;
CREATE UNIQUE INDEX IF NOT EXISTS one_task_per_official_source ON tasks(tenant,(payload::jsonb->>'inbox_id'),(payload::jsonb->'context'->>'chatwoot_message_id'))
 WHERE payload::jsonb->'context'->>'provider'='chatwoot_official';
'''


class Database:
    def __init__(self, dsn, pool_size=8):
        # Fail closed on absent DSNs and on accidentally using Chatwoot's business DB.
        values = conninfo_to_dict(dsn)
        if not re.fullmatch(r'rpa_gateway(?:_test)?',values.get('dbname','')):
            raise ValueError('A dedicated rpa_gateway or rpa_gateway_test PostgreSQL database is required')
        if not 1 <= pool_size <= 16:
            raise ValueError('Database pool must contain 1 to 16 connections')
        def configure(conn):
            conn.execute("SET statement_timeout='5s'")
            conn.execute("SET lock_timeout='2s'")
            conn.execute("SET idle_in_transaction_session_timeout='10s'")
        self.pool = ConnectionPool(make_conninfo(dsn,connect_timeout=5,application_name='qunl-rpa-gateway'),
            min_size=1,max_size=pool_size,timeout=5,max_waiting=64,open=True,
            configure=configure,check=ConnectionPool.check_connection,kwargs={'autocommit':True,'row_factory':dict_row})
        self.pool.wait(timeout=10)

    @contextmanager
    def transaction(self):
        with self.pool.connection() as conn:
            with conn.transaction():
                yield conn

    def initialize(self):
        with self.transaction() as db:
            # Serialize DDL within this dedicated DB/schema, never lock Chatwoot tables.
            db.execute("SELECT pg_advisory_xact_lock(hashtext(current_database()),hashtext(current_schema() || ':rpa_gateway_schema'))")
            exists=db.execute("SELECT to_regclass('gateway_schema') name").fetchone()['name']
            if exists:
                versions=[row['version'] for row in db.execute('SELECT version FROM gateway_schema')]
                if versions==[SCHEMA_VERSION]:return
                if versions and versions not in ([1],[2],[3]):
                    raise RuntimeError('Unsupported gateway database schema')
            for statement in DDL.split(';'):
                if statement.strip():
                    db.execute(statement)
            versions = [row['version'] for row in db.execute('SELECT version FROM gateway_schema')]
            if versions and versions not in ([1],[2],[3],[SCHEMA_VERSION]):
                raise RuntimeError('Unsupported gateway database schema')
            for statement in UPGRADE.split(';'):
                if statement.strip():db.execute(statement)
            db.execute('DELETE FROM gateway_schema WHERE version<>%s',(SCHEMA_VERSION,))
            db.execute('INSERT INTO gateway_schema(version) VALUES(%s) ON CONFLICT DO NOTHING',(SCHEMA_VERSION,))

    def close(self):
        self.pool.close()
