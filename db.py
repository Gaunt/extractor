import sqlite3
from enum import Enum
from textwrap import dedent
from typing import NamedTuple, Optional, Iterable
import contextlib
import os


DB_FILE = "symbol.db"

BATCH_SIZE = 500_000

_conn = None


def get_conn(db_file=DB_FILE):
    global _conn
    if not _conn:
        _conn = sqlite3.connect(DB_FILE)
    return _conn


class StateChangeType(Enum):
    TX_OUT = "tx_out"
    TX_IN = "tx_in"
    RX_DEBIT = "rx_debit"
    RX_CREDIT = "rx_credit"


class AccountStateChange(NamedTuple):
    address: str
    xym_change: int
    height: int
    type_: str
    fee: Optional[int] = None
    fee_multiplier: Optional[int] = None
    sender_address: str = None
    recipient_address: str = None
    tx_type: str = None
    explorer_link: Optional[str] = None


def db_init():
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(
            dedent(
                """CREATE TABLE account_state_changes
        (address text, xym_change int, height int, change_type int, fee int, fee_multiplier int,
        sender_address text, recipient_address text, tx_type text, explorer_link text)"""
            )
        )
    except sqlite3.OperationalError:
        pass
    conn.commit()


def db_clean(db_file=DB_FILE):
    try:
        os.remove(db_file)
    except FileNotFoundError:
        pass


@contextlib.contextmanager
def batch_saver():
    batch = []

    def saver(changes: Iterable[AccountStateChange]):
        nonlocal batch
        batch.extend(changes)
        if len(batch) > BATCH_SIZE:
            save_changes(batch)
            batch = []
    try:
        yield saver
    finally:
        save_changes(batch)


def save_changes(changes: Iterable[AccountStateChange]):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.executemany(
        """INSERT INTO account_state_changes VALUES (?,?,?,?,?,?,?,?,?,?)""",
        changes,
    )
    conn.commit()
