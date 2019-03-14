import pytest

from slurmpy import slurmpy as slm

def test_get_env():
    assert slm.get_env() is not None
