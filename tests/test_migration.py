import hashlib
import io
import json
import sqlite3
from PIL import Image
import psycopg
import pytest
from rpa_gateway.database import Database, TABLES
from rpa_gateway.migration import COLUMNS, import_sqlite
from tests.legacy_sqlite_schema import DDL


def legacy(tmp_path):
    path=tmp_path/'legacy.sqlite3'
    with sqlite3.connect(path) as db:
        db.executescript(DDL)
        db.execute('INSERT INTO devices VALUES(?,?,?,?,?,?,?,?,?,?)',
            ('migration','device',hashlib.sha256(b'legacy-credential').hexdigest(),'{"bindings":[]}',0,'session','installation','{}','{}',100.0))
        stream=io.BytesIO();Image.new('RGB',(8,8),'blue').save(stream,format='PNG');image=stream.getvalue()
        db.execute('INSERT INTO media VALUES(?,?,?,?,?,?,?)',('migration','media',hashlib.sha256(image).hexdigest(),hashlib.md5(image).hexdigest(),len(image),'image/png',image))
        for task,state in [('one','confirmed'),('two','unknown')]:
            payload=json.dumps({'media_ids':['media'],'caption':'运单号：JSE迁移测试'})
            db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                ('migration',task,'device',task,payload,state,100.,280.,'lease-token',190.,'session',110.,115.,120.))
            db.execute('INSERT INTO events(tenant,task,event_id,at,state,details) VALUES(?,?,?,?,?,?)',
                ('migration',task,'event-'+task,120.,state,'{"说明":"保留原始回执"}'))
        db.execute('INSERT INTO archive_claims VALUES(?,?,?)',('migration','archive-event','one'))
        db.execute("UPDATE sqlite_sequence SET seq=999 WHERE name='events'")
    return path,image


@pytest.fixture
def destination(database_dsn):
    database=Database(database_dsn,pool_size=2)
    database.initialize()
    try:yield database
    finally:database.close()


def test_import_preserves_all_five_tables_blobs_unknown_and_sequence(destination,tmp_path):
    source,image=legacy(tmp_path)
    result=import_sqlite(destination,source)
    assert result['content_verified'] is True
    assert result['tables']['tasks']['rows']==2
    with destination.transaction() as db:
        assert db.execute("SELECT state FROM tasks WHERE id='two'").fetchone()['state']=='unknown'
        assert db.execute('SELECT data FROM media').fetchone()['data']==image
        event=db.execute("INSERT INTO events(tenant,task,event_id,at,state,details) VALUES('migration','two','post-migration',200,'unknown','{}') RETURNING seq").fetchone()
        assert event['seq']==1000
        db.execute("UPDATE tasks SET state='closed_unconfirmed' WHERE id='two'")
    assert import_sqlite(destination,source)['already_imported'] is True
    with destination.transaction() as db:
        assert db.execute("SELECT state FROM tasks WHERE id='two'").fetchone()['state']=='closed_unconfirmed'


def test_import_refuses_nonempty_target_without_modifying_it(destination,tmp_path):
    source,_=legacy(tmp_path)
    with destination.transaction() as db:
        db.execute("INSERT INTO devices(tenant,id,token_hash,scope) VALUES('existing','existing','hash','{}')")
    with pytest.raises(ValueError,match='must be empty'):import_sqlite(destination,source)
    with destination.transaction() as db:
        assert db.execute('SELECT count(*) n FROM devices').fetchone()['n']==1
        assert db.execute('SELECT count(*) n FROM tasks').fetchone()['n']==0


def test_bad_foreign_key_rolls_back_entire_import(destination,tmp_path):
    source,_=legacy(tmp_path)
    with sqlite3.connect(source) as db:
        db.execute("INSERT INTO archive_claims VALUES('migration','bad-event','missing-task')")
    with pytest.raises(psycopg.errors.ForeignKeyViolation):import_sqlite(destination,source)
    with destination.transaction() as db:
        for table in TABLES+('gateway_imports',):
            assert db.execute('SELECT count(*) n FROM '+table).fetchone()['n']==0


def test_import_rejects_multiple_active_tasks_for_one_device(destination,tmp_path):
    source,_=legacy(tmp_path)
    with sqlite3.connect(source) as db:db.execute("UPDATE tasks SET state='leased' WHERE id='one'")
    with pytest.raises(psycopg.errors.UniqueViolation):import_sqlite(destination,source)
    with destination.transaction() as db:
        assert db.execute('SELECT count(*) n FROM devices').fetchone()['n']==0


def test_different_source_cannot_replace_prior_import(destination,tmp_path):
    source,_=legacy(tmp_path)
    import_sqlite(destination,source)
    with sqlite3.connect(source) as db:db.execute("UPDATE tasks SET updated=999 WHERE id='two'")
    with pytest.raises(ValueError,match='another import'):import_sqlite(destination,source)
    with destination.transaction() as db:
        assert db.execute("SELECT updated FROM tasks WHERE id='two'").fetchone()['updated']==120.


def test_missing_snapshot_does_not_create_sqlite_file(destination,tmp_path):
    path=tmp_path/'missing.sqlite3'
    with pytest.raises(ValueError,match='missing'):import_sqlite(destination,path)
    assert not path.exists()
