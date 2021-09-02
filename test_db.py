import os
import pytest
import db


@pytest.fixture
def cursor(tmp_path):
    db.db_init(tmp_path / "test.db")
    return db.get_conn().cursor()


def test_init(cursor):
    assert isinstance(cursor, db.sqlite3.Cursor)


def test_save_empty(cursor):
    changes = []
    db.save_changes(changes)

    
def test_save(cursor):
    changes = [
        db.StateChange(
            address="NASYMBOLLK6FSL7GSEMQEAWN7VW55ZSZU25TBOA",
            height=1,
            type_=db.StateChangeType.TX_OUT.value,
        )
    ]
    db.save_changes(changes)
