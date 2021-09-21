from binascii import unhexlify
import numpy as np
from collections import defaultdict
import itertools as it
from util import public_key_to_address
import sqlite3
from textwrap import dedent
from enum import Enum
from typing import NamedTuple, Optional, Iterable
import pickle
import contextlib
import os


class StateChangeType(Enum):
    TX_OUT = "tx_out"
    TX_IN = "tx_in"
    TX_KEY_LINK = "tx_key_link"
    RX_DEBIT = "rx_debit"
    RX_CREDIT = "rx_credit"


class StateChange(NamedTuple):
    address: str
    height: int
    type_: str
    amount: int = 0
    payload_type: int = 0
    fee: Optional[int] = None
    fee_multiplier: Optional[int] = None
    sender_address: Optional[str] = None
    recipient_address: Optional[str] = None
    entity_hash: Optional[str] = None
    merkle_component_hash: Optional[str] = None
    mosaic: str = "XYM"


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
    try:
        os.remove(db_file)
    except FileNotFoundError:
        pass


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
        return state_map["xym_balance"].items()


def get_distinct_wallets():
    sql = """select distinct address from account_state_changes order by address"""
    conn = get_conn()
    cursor = conn.cursor()
    return cursor.fetchall()


def get_balance_history(address):
    conn = get_conn()
    cursor = conn.cursor()
    sql = """SELECT height, sum(amount) from account_state_changes where address=? group by height order by height"""
    cursor.execute(sql, (address,))
    return cursor.fetchall()


def get_balance_history_state_map(address, state_map):
    return [*sorted(state_map[address]["xym_balance"].items())]


def decode_hashes(block):
    hashes = block["tx_hashes"]
    if not hashes:
        yield from it.repeat(
            (
                None,
                None,
            )
        )
    else:
        for tx_hashes in hashes:
            yield tx_hashes["entity_hash"].hex().upper(), tx_hashes[
                "merkle_component_hash"
            ].hex().upper()


def decompose_tx(tx, height, fee_multiplier, tx_hash=None, parent_hash=None):
    # TODO: handle flows for *all* mosaics, not just XYM
    address = public_key_to_address(unhexlify(tx["signer_public_key"]))
    sender_amount = 0

    if tx["type"] == b"4154":  # transfer tx
        if (
            len(tx["payload"]["message"])
            and tx["payload"]["message"][0] == 0xFE
        ):
            pass
            # self.state_map[address]["delegation_requests"][
            #     tx["payload"]["recipient_address"]
            # ].append(height)
        elif tx["payload"]["mosaics_count"] > 0:
            for mosaic in tx["payload"]["mosaics"]:
                if hex(mosaic["mosaic_id"]) in [
                    "0x6bed913fa20223f8",
                    "0xe74b99ba41f4afee",
                ]:

                    sender_amount -= mosaic["amount"]
                    recipient_address = tx["payload"]["recipient_address"]
                    yield StateChange(
                        address=recipient_address,
                        type_=StateChangeType.TX_IN.value,
                        recipient_address=recipient_address,
                        height=height,
                        payload_type=tx["type"],
                        entity_hash=tx_hash,
                        merkle_component_hash=tx_hash,
                        amount=mosaic["amount"],
                    )

    elif tx["type"] in [b"4243", b"424c", b"414c"]:  # key link tx
        if tx["type"] == b"4243":
            link_key = "vrf_key_link"
        elif tx["type"] == b"424c":
            link_key = "node_key_link"
        elif tx["type"] == b"414c":
            link_key = "account_key_link"
        if tx["payload"]["link_action"] == 1:
            pass
            # self.state_map[address][link_key][
            #     public_key_to_address(tx["payload"]["linked_public_key"])
            # ].append([height, np.inf])
        else:
            pass
            # self.state_map[address][link_key][
            #     public_key_to_address(tx["payload"]["linked_public_key"])
            # ][-1][1] = height

    elif tx["type"] in [b"4141", b"4241"]:  # aggregate tx
        for sub_tx in tx["payload"]["embedded_transactions"]:
            yield from decompose_tx(sub_tx, height, None, tx_hash=tx_hash)

    if fee_multiplier is not None:  # handle fees
        fee = min(tx["max_fee"], tx["size"] * fee_multiplier)
        # self.state_map[address]["xym_balance"][height] -= min(
        #     tx["max_fee"], tx["size"] * fee_multiplier
        # )
        yield StateChange(
            address=address,
            amount=sender_amount - fee,
            fee=fee,
            height=height,
            payload_type=tx["type"],
            entity_hash=tx_hash,
            merkle_component_hash=tx_hash,
            type_=StateChangeType.TX_OUT.value,
        )


def insert_tx(self, tx, height, fee_multiplier):
    with batch_saver() as saver:
        for state_change in self.decompose_tx(
            tx, height, fee_multiplier, tx_hash="", parent_hash=""
        ):
            saver([state_change])


def insert_blocks(blocks):
    with batch_saver() as saver:
        for block in blocks:
            saver([*decompose_block(block)])


def decompose_block(block):
    header = block["header"]
    height = header["height"]

    # handle transactions
    fee_multiplier = block["header"]["fee_multiplier"]
    transactions = block["footer"]["transactions"]
    tx_hashes = decode_hashes(block)
    for tx, (entity_hash, merkle_component_hash) in zip(
        transactions, tx_hashes
    ):
        yield from decompose_tx(
            tx,
            height,
            fee_multiplier,
            tx_hash=entity_hash,
        )


def insert_statements(statements):
    with batch_saver() as saver:
        for height, stmts, s_path in statements:
            for stmt in stmts["transaction_statements"]:
                for rx in stmt["receipts"]:
                    saver([*decompose_rx(rx, height)])


def decompose_rx(rx, height):

    # rental fee receipts
    if rx["type"] in [0x124D, 0x134E]:
        if hex(rx["payload"]["mosaic_id"]) in [
            "0x6bed913fa20223f8",
            "0xe74b99ba41f4afee",
        ]:
            # self.state_map[rx["payload"]["sender_address"]]["xym_balance"][
            #     height
            # ] -= rx["payload"]["amount"]
            yield StateChange(
                address=rx["payload"]["sender_address"],
                amount=-rx["payload"]["amount"],
                height=height,
                payload_type=rx["type"],
                type_=StateChangeType.RX_DEBIT.value,
            )
            # self.state_map[rx["payload"]["recipient_address"]]["xym_balance"][
            #     height
            # ] += rx["payload"]["amount"]
            yield StateChange(
                address=rx["payload"]["recipient_address"],
                amount=rx["payload"]["amount"],
                height=height,
                payload_type=rx["type"],
                type_=StateChangeType.RX_CREDIT.value,
            )

    # balance change receipts (credit)
    elif rx["type"] in [0x2143, 0x2248, 0x2348, 0x2252, 0x2352]:
        # self.state_map[rx["payload"]["target_address"]]["xym_balance"][
        #     height
        # ] += rx["payload"]["amount"]
        yield StateChange(
            address=rx["payload"]["target_address"],
            amount=rx["payload"]["amount"],
            height=height,
            payload_type=rx["type"],
            type_=StateChangeType.RX_CREDIT.value,
        )

    # balance change receipts (debit)
    elif rx["type"] in [0x3148, 0x3152]:
        # self.state_map[rx["payload"]["target_address"]]["xym_balance"][
        #     height
        # ] -= rx["payload"]["amount"]
        yield StateChange(
            address=rx["payload"]["target_address"],
            amount=-rx["payload"]["amount"],
            height=height,
            payload_type=rx["type"],
            type_=StateChangeType.RX_DEBIT.value,
        )

    # aggregate receipts
    if rx["type"] == 0xE143:
        for sub_rx in rx["receipts"]:
            yield from decompose_rx(sub_rx, height)
