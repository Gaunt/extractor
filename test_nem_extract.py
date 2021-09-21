import nem_extract
import state


def test_main(tmp_path, data_dir):
    data_main = str(data_dir / 'data_main')
    state_map_path = str(tmp_path / "./state_map.msgpack")
    nem_extract.main(
        nem_extract.parse_args(
            [
                f"--block_dir={str(data_dir)}",
                f"--state_save_path={state_map_path}",
                "--quiet",
            ]
        )
    )
    state_map = state.XYMStateMap.read_msgpack(state_map_path)
    assert isinstance(state_map, state.XYMStateMap)
