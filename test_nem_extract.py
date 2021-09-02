import pytest
import subprocess
import pathlib
import shlex
import nem_extract


@pytest.fixture(scope="session", autouse=True)
def fetch_text_data_dir():
    p = pathlib.Path("./symbol_test_data")
    if not p.exists():
        subprocess.run(
            shlex.split(
                """git clone https://github.com/Gaunt/symbol_test_data"""
            )
        )


def test_stm_file_deserialize(tmp_path):
    statement_save_path = str(tmp_path / "stmt_data.msgpack")
    nem_extract.main(
        nem_extract.parse_args(
            [
                "--block_dir=./symbol_test_data/data_main",
                f"--statement_save_path={str(statement_save_path)}",
                "--quiet",
            ]
        )
    )
    statements = nem_extract.load_stm_data(
        statement_save_path=str(statement_save_path)
    )
    assert list(statements.keys()) == [
        "transaction_statements",
        "address_resolution_statements",
        "mosaic_resolution_statements",
    ]
