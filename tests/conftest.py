import pytest
from sqlite_anyio import enable_cancellation, disable_cancellation


@pytest.fixture(params=[False, True], ids=["cancellation-disabled", "cancellation-enabled"])
def cancellation(request):
    cancellation_enabled = request.param
    if cancellation_enabled:
        enable_cancellation()
    else:
        disable_cancellation()
