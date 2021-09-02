import nem_extract


def test_stm_file_deserialize(tmp_path):
    statement_save_path = str(tmp_path / "stmt_data.msgpack")
    nem_extract.main(
        nem_extract.parse_args(
            [
                "--block_dir=data_test",
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


def test_deserialize_blocks():
    block_paths = nem_extract.get_block_paths(block_dir="data_test")
    for block in nem_extract.deserialize_blocks(block_paths):
        assert len(block['tx_hashes']) == len(block['footer']['transactions'])
