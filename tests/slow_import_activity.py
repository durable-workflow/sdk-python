"""An activity whose module import stalls in spawned children (sdk-python#388).

The test process imports it normally. A test then sets the variable, so a
spawned callback that unpickles the handler stalls before reporting readiness,
as a callback does on an overloaded host.
"""
from __future__ import annotations

import os
import time

STALL_ENV = "DURABLE_WORKFLOW_TEST_STALL_ACTIVITY_IMPORT"

if os.environ.get(STALL_ENV):
    time.sleep(float(os.environ[STALL_ENV]))


def stalled_import_activity() -> str:
    return "ran"
