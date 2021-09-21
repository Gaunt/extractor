import pytest
import sql
import nem_extract_to_sqlite


@pytest.fixture
def cursor(tmp_path):
    sql.db_init(tmp_path / "test.db")
    return sql.get_conn().cursor()


def test_init(cursor):
    assert isinstance(cursor, sql.sqlite3.Cursor)


def test_save_empty(cursor):
    changes = []
    sql.save_changes(changes)


def test_save(cursor):
    changes = [
        sql.StateChange(
            address="NASYMBOLLK6FSL7GSEMQEAWN7VW55ZSZU25TBOA",
            height=1,
            type_=sql.StateChangeType.TX_OUT.value,
        )
    ]
    sql.save_changes(changes)


def test_compare_balances():
    address = 'NAUORQPKZ6NNECGAD6Y42MAEZBB6GF42X4Z7AAI'
    sql.db_init("symbol.db")


def test_main(tmp_path, data_dir):
    data_main = str(data_dir / 'data_main')
    sqlite_db_file = str(tmp_path / "./symbol.sql")
    nem_extract_to_sqlite.main(
        nem_extract_to_sqlite.parse_args(
            [
                f"--block_dir={data_main}",
                f"--sqlite_db_file={sqlite_db_file}",
                "--quiet",
            ]
        )
    )
