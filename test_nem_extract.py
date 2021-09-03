import pytest
import subprocess
import pathlib
import shlex
import nem_extract
import state


@pytest.fixture(scope="session", autouse=True)
def fetch_text_data_dir():
    p = pathlib.Path("./symbol_test_data")
    if not p.exists():
        subprocess.run(
            shlex.split(
                """git clone https://github.com/Gaunt/symbol_test_data"""
            )
        )


def test_state_map(tmp_path):
    state_map_path = str(tmp_path / "./state_map.msgpack")
    nem_extract.main(
        nem_extract.parse_args(
            [
                "--block_dir=./symbol_test_data/data_main",
                f"--state_save_path={state_map_path}",
                "--quiet",
            ]
        )
    )
    state_map = state.XYMStateMap.read_msgpack(state_map_path)
    assert isinstance(state_map, state.XYMStateMap)
