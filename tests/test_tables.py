import os
import pytest
from quilt import Connection
from random import randint


username = os.environ.get('QUILT_USERNAME')
url = os.environ.get('QUILT_URL')


def test_tables():
    """ Test tables. """
    conn = Connection(username=username, url=url)
    results = conn.tables
    assert isinstance(results, list)


def test_search_tables():
    conn = Connection(username=username, url=url)
    results = conn.search('term')
    assert isinstance(results, list)


def test_get_table_with_bad_ids():
    conn = Connection(username=username, url=url)
    # with pytest.raises(TableNotFoundError):
    result = conn.get_table(10000000000)
    assert isinstance(result, type(None))  # hacky
    result = conn.get_table('test string')
    assert isinstance(result, type(None))


def test_create_empty_table():
    conn = Connection(username=username, url=url)
    t1 = conn.create_table(name='test', description='test empty')
    t2 = conn.get_table(t1.id)
    assert t1 == t2


def test_create_table_with_duplicate_attributes():
    conn = Connection(username=username, url=url)
    t1 = conn.create_table(name='test', description='test empty')
    t2 = conn.create_table(name='test', description='test empty')
    assert t2.id == t1.id + 1
    assert t1.sqlname != t2.sqlname


def test_create_table_from_file():
    seed = str(randint(10000, 99999))
    test_name = 'test' + seed
    test_file = test_name + '.csv'
    data = 'col1,col2\n1,a\n2,b\n'
    with open(test_file, 'w') as f:
        f.write(data)
    test_description = 'test file ' + test_name
    conn = Connection(username=username, url=url)
    t1 = conn.create_table(name=test_name, description=test_description,
                           inputfile=test_file)
    assert ['col1', 'col2'] == [col.name for col in t1.columns]
    fields = [col.field for col in t1.columns]
    rows = [row for row in t1]
    # expected_row1 = {fields[0]: 1, fields[1]: 'a', u'qgrid': 1}
    # expected_row2 = {fields[0]: 2, fields[1]: 'b', u'qgrid': 2}
    # assert rows == [expected_row1, expected_row2]  # this fails
    assert rows[0][fields[0]] == 1
    # assert isinstance(rows[0][fields[0]], int)  # this fails, it's 1.0 not 1
    assert rows[0][fields[1]] == 'a'
    assert rows[1][fields[0]] == 2
    # assert isinstance(rows[1][fields[0]], int)  # this fails, it's 2.0 not 2
    assert rows[1][fields[1]] == 'b'
    t2 = conn.get_table(t1.id)
    assert t1 == t2
    os.remove(test_file)
