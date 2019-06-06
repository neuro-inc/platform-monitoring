from uuid import uuid4

import pytest

from .conftest_kube import MyPodDescriptor


@pytest.fixture
def job_pod() -> MyPodDescriptor:
    return MyPodDescriptor(f"job-{uuid4()}")
