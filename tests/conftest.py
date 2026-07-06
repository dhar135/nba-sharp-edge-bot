import pytest


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    """Fresh SQLite DB per test; patches services.db to use it."""
    db_path = str(tmp_path / "test.db")
    import services.db as db
    monkeypatch.setattr(db, "DB_NAME", db_path)
    db.init_db()
    return db_path
