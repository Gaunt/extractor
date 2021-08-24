import nem_extract


def test_stm_file():
    statements = nem_extract.load_stm_data()
    assert list(statements.keys()) == ['transaction_statements', 'address_resolution_statements', 'mosaic_resolution_statements']
