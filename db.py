import sqlite3
from enum import Enum
from textwrap import dedent
from typing import NamedTuple, Optional, Iterable
import pickle
import contextlib
import os
from nem_extract import StateChange, StateChangeType


DB_FILE = "symbol.db"

BATCH_SIZE = 500_000

_conn = None


def get_conn(db_file=DB_FILE):
    global _conn
    if not _conn:
        _conn = sqlite3.connect(DB_FILE)
    return _conn


def db_init(db_file=DB_FILE):
    conn = get_conn(db_file)
    cursor = conn.cursor()
    try:
        cursor.execute(
            dedent(
                """
                CREATE TABLE account_state_changes(
                 address text,
                 height int,
                 type_ text,
                 amount int,
                 payload_type int,
                 fee int,
                 fee_multiplier int,
                 sender_address text,
                 recipient_address text,
                 entity_hash text,
                 merkle_component_hash text,
                 mosaic text)
                """
            )
        )
    except sqlite3.OperationalError:
        pass
    conn.commit()


def db_clean(db_file=DB_FILE):
    os.remove(db_file)


@contextlib.contextmanager
def batch_saver():
    batch = []

    def saver(changes: Iterable[StateChange]):
        nonlocal batch
        batch.extend(changes)
        if len(batch) > BATCH_SIZE:
            save_changes(batch)
            batch = []

    try:
        yield saver
    finally:
        save_changes(batch)


def save_changes(changes: Iterable[StateChange]):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.executemany(
        """INSERT INTO account_state_changes
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        changes,
    )
    conn.commit()


def pickle_balance_history(address):
    with open("state_map.pkl") as f:
        state_map = pickle.load(f)
        return state_map['xym_balance'].items()


def db_balance_history(address):
    conn = get_conn()
    cursor = conn.cursor()
    sql = """SELECT height, sum(xym_change) from account_state_changes where address='?' group by height order by height"""
    cursor.execute(sql, address)
    return cursor.fetchall()
