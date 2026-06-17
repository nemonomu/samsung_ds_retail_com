import os
from datetime import datetime

import pytz


KST = pytz.timezone('Asia/Seoul')


def _setdefault_env(name, value):
    existing = os.environ.get(name)
    if existing:
        return existing
    os.environ[name] = value
    return value


SESSION_STARTED_AT_KST = _setdefault_env(
    'DS_CRAWLER_SESSION_STARTED_AT_KST',
    datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')
)

SESSION_ID = _setdefault_env(
    'DS_CRAWLER_SESSION_ID',
    f"c{datetime.now(KST).strftime('%y%m%d%H%M%S')}{os.getpid() % 10000:04d}"
)

MONITORING_CREATED_ID = SESSION_ID
