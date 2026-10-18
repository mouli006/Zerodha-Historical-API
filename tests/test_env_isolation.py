"""Guard: tests never read or write the real <repo>/.env (conftest points config.ENV_PATH at a session temp file).
The real file is only stat()ed here, never opened."""
import importlib
import os
import tempfile

import config

REPO_ENV = os.path.join(config.BASE_DIR, ".env")


def snapshot(path):
    """(exists, mtime_ns, size) of path, without reading it."""
    if not os.path.exists(path):
        return (False, None, None)
    st = os.stat(path)
    return (True, st.st_mtime_ns, st.st_size)


def test_env_path_is_not_repo_env():
    assert not config.same_path(config.ENV_PATH, REPO_ENV)
    temp = os.path.normcase(os.path.realpath(tempfile.gettempdir()))
    assert os.path.normcase(os.path.realpath(config.ENV_PATH)).startswith(temp)


def test_app_import_and_secret_key_leave_repo_env_alone():
    before = snapshot(REPO_ENV)
    importlib.import_module("app")
    key = config.secret_key()
    assert config.secret_key() == key
    with open(config.ENV_PATH, encoding="utf-8") as f:
        assert f"FLASK_SECRET_KEY={key}\n" in f.read()
    assert snapshot(REPO_ENV) == before


def test_session_env_file_is_used():
    before = snapshot(REPO_ENV)
    config.save_env({"X_ENV_ISOLATION_TEST": "1"})
    try:
        with open(config.ENV_PATH, encoding="utf-8") as f:
            assert "X_ENV_ISOLATION_TEST=1\n" in f.read()
        assert config.load_env()["X_ENV_ISOLATION_TEST"] == "1"
    finally:
        config.save_env({"X_ENV_ISOLATION_TEST": None})
    assert "X_ENV_ISOLATION_TEST" not in config.load_env()
    assert snapshot(REPO_ENV) == before
