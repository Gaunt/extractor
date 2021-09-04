import argparse
import itertools as it
import functools
import operator as op
import sys
import nem_extract
from tqdm import tqdm
import db


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--block_dir",
        type=str,
        default="./data",
        help="Location of block store",
    )
    parser.add_argument(
        "--sqlite_db_file",
        type=str,
        default=db.DB_FILE,
        help="path to write the extracted block data to",
    )
    parser.add_argument(
        "--block_extension",
        type=str,
        default=".dat",
        help="extension of block files; must be unique",
    )
    parser.add_argument(
        "--statement_extension",
        type=str,
        default=".stmt",
        help="extension of block files; must be unique",
    )
    parser.add_argument(
        "--quiet", action="store_true", help="do not show progress bars"
    )
    args = parser.parse_args(argv)
    return args


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


def main(args):
    if args.quiet:
        nem_extract.tqdm = functools.partial(tqdm, disable=True)

    db.db_clean()
    db.db_init()

    with db.batch_saver() as saver:
        block_paths = nem_extract.get_block_paths(
            args.block_dir, args.block_extension
        )
        blocks = nem_extract.deserialize_blocks(block_paths, True)
        state_map = nem_extract.XYMStateMap()

        for block in blocks:
            height = block["header"]["height"]
            transactions = block["footer"]["transactions"]
            tx_hashes = decode_hashes(block)
            for tx, (entity_hash, merkle_component_hash) in zip(
                transactions, tx_hashes
            ):
                saver(
                    state_map.decompose_tx(
                        tx,
                        height,
                        block["header"]["fee_multiplier"],
                        tx_hash=entity_hash,
                    ),
                )

        print("block data extraction complete!\n")
        print(f"block data written to {args.sqlite_db_file}")

        statements = nem_extract.deserialize_statements(
            nem_extract.get_statement_paths(
                block_dir=args.block_dir,
                statement_extension=args.statement_extension,
            )
        )

        for height, stmts, s_path in statements:
            for stmt in stmts["transaction_statements"]:
                for rx in stmt["receipts"]:
                    saver(state_map.decompose_rx(rx, height))

        print("statement data extraction complete!\n")
        print(f"statement data written to {args.sqlite_db_file}")


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    main(args)
