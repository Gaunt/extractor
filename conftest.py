import subprocess
import pathlib
import pytest
import shlex


@pytest.fixture
def data_dir():
    p = pathlib.Path("./symbol_test_data")
    if not p.exists():
        subprocess.run(
            shlex.split(
                """git clone https://github.com/Gaunt/symbol_test_data"""
            )
        )
    return p
