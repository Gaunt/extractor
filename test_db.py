import os
import pytest
import nem_extract
import db


@pytest.fixture
def conn():
    db.DB_FILE = "test.db"
    db.db_clean()
    return db.get_conn()


@pytest.fixture
def cursor(conn):
    db.db_init()
    return conn.cursor()


def test_init(conn):
    db.db_init()
    assert os.path.exists(db.DB_FILE)


def test_save_empty(cursor):
    changes = []
    db.save_changes(changes)


def test_save(cursor):
    changes = [
        db.AccountStateChange(
            address="NASYMBOLLK6FSL7GSEMQEAWN7VW55ZSZU25TBOA",
            xym_change=0,
            height=1,
            type_="tx_out",
            fee=0,
            fee_multiplier=None,
            sender_address=None,
            recipient_address=None,
            tx_type=None,
            explorer_link=None,
        )
    ]
    db.save_changes(changes)

