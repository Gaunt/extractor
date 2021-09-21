import argparse
import itertools as it
import functools
import operator as op
import sys
import nem_extract
from tqdm import tqdm
import sql


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
        default=sql.DB_FILE,
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


def main(args):
    if args.quiet:
        nem_extract.tqdm = functools.partial(tqdm, disable=True)

    sql.db_clean()
    sql.db_init()

    block_paths = nem_extract.get_block_paths(
        args.block_dir, args.block_extension
    )
    blocks = nem_extract.deserialize_blocks(block_paths, True)
    sql.insert_blocks(blocks)

    print("block data extraction complete!\n")
    print(f"block data written to {args.sqlite_db_file}")

    statement_paths = nem_extract.get_statement_paths(
        block_dir=args.block_dir,
        statement_extension=args.statement_extension,
    )

    statements = nem_extract.deserialize_statements(statement_paths)
    sql.insert_statements(statements)

    print("statement data extraction complete!\n")
    print(f"statement data written to {args.sqlite_db_file}")


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    main(args)
