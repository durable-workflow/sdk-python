from __future__ import annotations

import httpx
import pytest

from durable_workflow.retry_policy import RetryPolicy


class TestRetryPolicy:
    def test_should_retry_connection_error(self) -> None:
        policy = RetryPolicy(max_attempts=3)
        exc = httpx.ConnectError("connection failed")
        assert policy.should_retry(exc, attempt=0) is True
        assert policy.should_retry(exc, attempt=1) is True
        assert policy.should_retry(exc, attempt=2) is True
        assert policy.should_retry(exc, attempt=3) is False  # max_attempts reached

    def test_should_retry_timeout(self) -> None:
        policy = RetryPolicy(max_attempts=3)
        exc = httpx.TimeoutException("timeout")
        assert policy.should_retry(exc, attempt=0) is True

    def test_should_retry_network_error(self) -> None:
        policy = RetryPolicy(max_attempts=3)
        exc = httpx.NetworkError("network error")
        assert policy.should_retry(exc, attempt=0) is True

    def test_should_retry_5xx_server_error(self) -> None:
        policy = RetryPolicy(max_attempts=3)
        response = httpx.Response(status_code=500, request=httpx.Request("GET", "http://test"))
        exc = httpx.HTTPStatusError("server error", request=response.request, response=response)
        assert policy.should_retry(exc, attempt=0) is True

    def test_should_retry_429_rate_limit(self) -> None:
        policy = RetryPolicy(max_attempts=3)
        response = httpx.Response(status_code=429, request=httpx.Request("GET", "http://test"))
        exc = httpx.HTTPStatusError("rate limited", request=response.request, response=response)
        assert policy.should_retry(exc, attempt=0) is True

    def test_should_not_retry_4xx_client_error(self) -> None:
        policy = RetryPolicy(max_attempts=3)
        response = httpx.Response(status_code=404, request=httpx.Request("GET", "http://test"))
        exc = httpx.HTTPStatusError("not found", request=response.request, response=response)
        assert policy.should_retry(exc, attempt=0) is False

    def test_should_not_retry_400_bad_request(self) -> None:
        policy = RetryPolicy(max_attempts=3)
        response = httpx.Response(status_code=400, request=httpx.Request("GET", "http://test"))
        exc = httpx.HTTPStatusError("bad request", request=response.request, response=response)
        assert policy.should_retry(exc, attempt=0) is False

    def test_should_not_retry_other_exceptions(self) -> None:
        policy = RetryPolicy(max_attempts=3)
        exc = ValueError("not a network error")
        assert policy.should_retry(exc, attempt=0) is False

    def test_backoff_calculation(self) -> None:
        policy = RetryPolicy(
            initial_backoff_seconds=0.1,
            max_backoff_seconds=5.0,
            backoff_multiplier=2.0,
            jitter=False,
        )
        assert policy.backoff_seconds(0) == 0.1
        assert policy.backoff_seconds(1) == 0.2
        assert policy.backoff_seconds(2) == 0.4
        assert policy.backoff_seconds(10) == 5.0  # capped at max

    def test_backoff_with_jitter(self) -> None:
        policy = RetryPolicy(initial_backoff_seconds=1.0, jitter=True)
        # Jitter should give us ±25%
        backoff = policy.backoff_seconds(0)
        assert 0.75 <= backoff <= 1.25

    @pytest.mark.asyncio
    async def test_execute_success_on_first_try(self) -> None:
        policy = RetryPolicy(max_attempts=3)
        call_count = 0

        async def fn() -> str:
            nonlocal call_count
            call_count += 1
            return "success"

        result = await policy.execute(fn)
        assert result == "success"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_execute_success_after_retry(self) -> None:
        policy = RetryPolicy(max_attempts=3, initial_backoff_seconds=0.01, jitter=False)
        call_count = 0

        async def fn() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise httpx.ConnectError("connection failed")
            return "success"

        result = await policy.execute(fn)
        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_execute_exhausted_retries(self) -> None:
        policy = RetryPolicy(max_attempts=3, initial_backoff_seconds=0.01, jitter=False)
        call_count = 0

        async def fn() -> str:
            nonlocal call_count
            call_count += 1
            raise httpx.ConnectError("connection failed")

        with pytest.raises(httpx.ConnectError):
            await policy.execute(fn)

        assert call_count == 3

    @pytest.mark.asyncio
    async def test_execute_non_retryable_error(self) -> None:
        policy = RetryPolicy(max_attempts=3)
        call_count = 0

        async def fn() -> str:
            nonlocal call_count
            call_count += 1
            response = httpx.Response(status_code=400, request=httpx.Request("GET", "http://test"))
            raise httpx.HTTPStatusError("bad request", request=response.request, response=response)

        with pytest.raises(httpx.HTTPStatusError):
            await policy.execute(fn)

        assert call_count == 1  # Should not retry 400 errors

    @pytest.mark.asyncio
    async def test_execute_retries_500_but_not_400(self) -> None:
        policy = RetryPolicy(max_attempts=3, initial_backoff_seconds=0.01, jitter=False)
        call_count = 0

        async def fn() -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                response = httpx.Response(status_code=500, request=httpx.Request("GET", "http://test"))
                raise httpx.HTTPStatusError("server error", request=response.request, response=response)
            return "success"

        result = await policy.execute(fn)
        assert result == "success"
        assert call_count == 2  # First attempt 500, second attempt success
