"""config.save_env / secret_key on a tmp_path .env (never the real one): keys, atomic replace, failures, threads."""
import os
import threading

import pytest

import config


@pytest.fixture
def env_file(tmp_path, monkeypatch):
    path = tmp_path / ".env"
    monkeypatch.setattr(config, "ENV_PATH", str(path))
    monkeypatch.setattr(config, "REPLACE_WAIT", 0)
    return path


def test_add_update_remove_keep_order_and_lf(env_file):
    config.save_env({"A": "1", "B": "2"})
    assert env_file.read_bytes() == b"A=1\nB=2\n"
    config.save_env({"A": "9", "C": "3"})
    assert env_file.read_bytes() == b"A=9\nB=2\nC=3\n"
    config.save_env({"B": None})
    assert env_file.read_bytes() == b"A=9\nC=3\n"
    assert config.load_env() == {"A": "9", "C": "3"}


def test_no_temp_file_left(env_file):
    config.save_env({"A": "1"})
    assert sorted(os.listdir(env_file.parent)) == [".env"]


def test_replace_failure_leaves_old_file(env_file, monkeypatch):
    config.save_env({"A": "1"})
    old = env_file.read_bytes()

    def boom(src, dst):
        raise OSError("disk full")

    monkeypatch.setattr(config.os, "replace", boom)
    with pytest.raises(OSError, match="disk full"):
        config.save_env({"A": "2", "B": "3"})
    assert env_file.read_bytes() == old
    assert sorted(os.listdir(env_file.parent)) == [".env"]


def test_replace_retried_while_busy(env_file, monkeypatch):
    config.save_env({"A": "1"})
    real = os.replace
    calls = []

    def busy_twice(src, dst):
        calls.append(1)
        if len(calls) <= 2:
            raise PermissionError("in use")
        real(src, dst)

    monkeypatch.setattr(config.os, "replace", busy_twice)
    config.save_env({"A": "2"})
    assert len(calls) == 3
    assert env_file.read_bytes() == b"A=2\n"
    assert sorted(os.listdir(env_file.parent)) == [".env"]


def test_replace_always_busy_raises_clear_error(env_file, monkeypatch):
    config.save_env({"SECRET": "s3cr3t-value"})
    old = env_file.read_bytes()

    def busy(src, dst):
        raise PermissionError("in use")

    monkeypatch.setattr(config.os, "replace", busy)
    with pytest.raises(OSError) as err:
        config.save_env({"SECRET": "new-s3cr3t"})
    assert "could not replace" in str(err.value)
    assert "s3cr3t" not in str(err.value)
    assert env_file.read_bytes() == old
    assert sorted(os.listdir(env_file.parent)) == [".env"]


def test_threads_all_keys_saved(env_file):
    start = threading.Barrier(20)

    def save(i):
        start.wait()
        config.save_env({f"K{i}": str(i)})

    threads = [threading.Thread(target=save, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert config.load_env() == {f"K{i}": str(i) for i in range(20)}
    assert sorted(os.listdir(env_file.parent)) == [".env"]


def test_secret_key_generated_once(env_file):
    config.save_env({"KITE_API_KEY": "k"})
    key = config.secret_key()
    assert key and config.secret_key() == key
    lines = env_file.read_text(encoding="utf-8").splitlines()
    assert lines == ["KITE_API_KEY=k", f"FLASK_SECRET_KEY={key}"]
