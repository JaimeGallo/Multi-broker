from __future__ import annotations

import pytest

from packages.common.config import AppConfig
from tests.helpers import make_config


@pytest.fixture
def config() -> AppConfig:
    return make_config()
