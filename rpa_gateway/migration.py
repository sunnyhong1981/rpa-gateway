"""Explicit one-time, atomic SQLite import; never a runtime storage fallback."""
import hashlib
import json
import sqlite3
from pathlib import Path
from urllib.parse import quote
from psycopg import sql
from .database import TABLES

COLUMNS = {
 'devices':('tenant','id','token_hash','scope','revoked','session','installation','hello','heartbeat','seen'),
 'media':('tenant','id','sha256','md5','size','mime','data'),
 'tasks':('tenant','id','device','dedup','payload','state','created','deadline','lease_token','lease_until','session','intent_at','submitted_at','updated'),
 'events':('seq','tenant','task','event_id','at','state','details'),
 'archive_claims':('tenant','event_id','task')}
ORDER = {'devices':('tenant','id'),'media':('tenant','id'),'tasks':('id',),'events':('seq',),'archive_claims':('tenant','event_id')}


def row_digest(rows, columns):
    digest=hashlib.sha256();count=0
    for row in rows:
        values=[]
        for name in columns:
            value=row[name]
            if isinstance(value,(bytes,bytearray,memoryview)):
                value={'bytes_sha256':hashlib.sha256(value).hexdigest(),'length':len(value)}
            values.append(value)
        digest.update(json.dumps(values,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()+b'\n')
        count+=1
    return {'rows':count,'sha256':digest.hexdigest()}


def source_summary(source):
    return {table:row_digest(source.execute('SELECT '+','.join(COLUMNS[table])+' FROM '+table+' ORDER BY '+','.join(ORDER[table])),COLUMNS[table]) for table in TABLES}


def destination_summary(db):
    result={}
    for table in TABLES:
        query=sql.SQL('SELECT {} FROM {} ORDER BY {}').format(
            sql.SQL(',').join(map(sql.Identifier,COLUMNS[table])),sql.Identifier(table),sql.SQL(',').join(map(sql.Identifier,ORDER[table])))
        with db.cursor(name='verify_'+table) as cursor:
            cursor.itersize=1 if table=='media' else 1000
            cursor.execute(query)
            result[table]=row_digest(cursor,COLUMNS[table])
    return result


def import_sqlite(database, source_path):
    path=Path(source_path).resolve()
    if not path.is_file():
        raise ValueError('SQLite snapshot is missing')
    with path.open('rb') as stream:
        source_sha=hashlib.file_digest(stream,'sha256').hexdigest()
    source=sqlite3.connect('file:'+quote(str(path),safe='/')+'?mode=ro',uri=True)
    source.row_factory=sqlite3.Row
    try:
        if source.execute('PRAGMA integrity_check').fetchone()[0]!='ok':
            raise ValueError('SQLite snapshot integrity check failed')
        expected=source_summary(source)
        sequence=source.execute("SELECT seq FROM sqlite_sequence WHERE name='events'").fetchone()
        sequence_floor=sequence[0] if sequence else 0
        with database.transaction() as db:
            db.execute("SELECT pg_advisory_xact_lock(hashtext(current_database()),hashtext(current_schema() || ':rpa_gateway_import'))")
            previous=db.execute('SELECT summary FROM gateway_imports WHERE source_sha256=%s',(source_sha,)).fetchone()
            if previous:
                # A retry never reimports or resets live state after the original import.
                return {'already_imported':True,'source_sha256':source_sha,'tables':json.loads(previous['summary'])}
            if db.execute('SELECT 1 FROM gateway_imports LIMIT 1').fetchone():
                raise ValueError('Target already has another import; refusing a different source')
            for table in TABLES:
                if db.execute(sql.SQL('SELECT 1 FROM {} LIMIT 1').format(sql.Identifier(table))).fetchone():
                    raise ValueError('Target gateway tables must be empty before import')
            for table in TABLES:
                columns=COLUMNS[table]
                insert=sql.SQL('INSERT INTO {} ({}) VALUES ({})').format(sql.Identifier(table),
                    sql.SQL(',').join(map(sql.Identifier,columns)),sql.SQL(',').join(sql.Placeholder() for _ in columns))
                for row in source.execute('SELECT '+','.join(columns)+' FROM '+table+' ORDER BY '+','.join(ORDER[table])):
                    db.execute(insert,tuple(row[name] for name in columns))
            actual=destination_summary(db)
            if actual!=expected:
                raise ValueError('Migration row count or content hash differs; rolled back')
            # Preserve monotonically increasing event IDs after importing explicit sequence values.
            db.execute("SELECT setval(pg_get_serial_sequence('events','seq'), GREATEST(%s,COALESCE((SELECT MAX(seq) FROM events),0),1), %s OR EXISTS(SELECT 1 FROM events))",(sequence_floor,sequence_floor>0))
            db.execute('INSERT INTO gateway_imports(source_sha256,summary) VALUES(%s,%s)',(source_sha,json.dumps(actual,sort_keys=True)))
            return {'already_imported':False,'source_sha256':source_sha,'tables':actual,'content_verified':True}
    finally:
        source.close()
