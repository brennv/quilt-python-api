import os
import pytest
from quilt import Connection


def test_connection_login_ok():
    """ Test a good login. Before running test, in terminal run:
    $ export QUILT_USERNAME="valid-username"
    $ export QUILT_PASSWORD="valid-password" """
    username = os.environ.get('QUILT_USERNAME')
    password = os.environ.get('QUILT_PASSWORD')
    url = os.environ.get('QUILT_URL')
    conn = Connection(username=username, url=url)
    assert conn.username == username
    assert conn.password == password
    assert conn.status_code == 200


def test_connection_login_bad():
    """ Test a bad login. """
    with pytest.raises(TypeError):
        conn = Connection()
    conn = Connection('0')
    assert conn.status_code == 403
