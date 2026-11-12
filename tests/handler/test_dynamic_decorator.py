import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

import nexusrpc
from nexusrpc.handler import (
    CancelOperationContext,
    Handler,
    MiddlewareSafeOperationHandler,
    OperationContext,
    OperationHandler,
    OperationHandlerMiddleware,
    StartOperationContext,
    StartOperationResultAsync,
    StartOperationResultSync,
    operation_handler,
    service_handler,
    sync_operation,
)
from nexusrpc.handler._core import ServiceHandler
from tests.helpers import DummySerializer, TestOperationTaskCancellation


def start_context(name: str, service: str = "tools") -> StartOperationContext:
    return StartOperationContext(
        service=service,
        operation=name,
        headers={},
        request_id="req",
        task_cancellation=TestOperationTaskCancellation(),
    )


def cancel_context(name: str, service: str = "tools") -> CancelOperationContext:
    return CancelOperationContext(
        service=service,
        operation=name,
        headers={},
        task_cancellation=TestOperationTaskCancellation(),
    )


class Dynamic(OperationHandler[nexusrpc.LazyValue, Any]):
    def __init__(self) -> None:
        self.tools = {"echo"}
        self.cancelled: list[tuple[str, str]] = []

    async def start(
        self, ctx: StartOperationContext, input: nexusrpc.LazyValue
    ) -> StartOperationResultSync[Any] | StartOperationResultAsync:
        assert isinstance(input, nexusrpc.LazyValue)
        value = await input.consume(as_type=dict)
        if ctx.operation not in self.tools:
            raise nexusrpc.HandlerError(
                "Unknown tool", type=nexusrpc.HandlerErrorType.NOT_FOUND
            )
        if value.get("async"):
            return StartOperationResultAsync(f"{ctx.operation}:{value['id']}")
        return StartOperationResultSync((ctx.operation, ctx.request_id, value))

    async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
        self.cancelled.append((ctx.operation, token))


@service_handler(name="tools")
class Tools:
    def __init__(self) -> None:
        self.dynamic = Dynamic()

    @sync_operation
    async def list_tools(self, _ctx: StartOperationContext, _input: None) -> str:
        return "manifest"

    @operation_handler(dynamic=True)
    def forward(self) -> OperationHandler[nexusrpc.LazyValue, Any]:
        return self.dynamic


@pytest.mark.asyncio
async def test_dynamic_decorator_routes_raw_input_and_preserves_named_handlers():
    tools = Tools()
    handler = Handler([tools])

    def raw(value: Any) -> nexusrpc.LazyValue:
        return nexusrpc.LazyValue(DummySerializer(value), {})

    result = await handler.start_operation(start_context("list_tools"), raw(None))
    assert isinstance(result, StartOperationResultSync) and result.value == "manifest"
    result = await handler.start_operation(start_context("echo"), raw({"id": 5}))
    assert isinstance(result, StartOperationResultSync) and result.value == (
        "echo",
        "req",
        {"id": 5},
    )
    result = await handler.start_operation(
        start_context("echo"), raw({"id": 5, "async": True})
    )
    assert isinstance(result, StartOperationResultAsync) and result.token == "echo:5"
    assert set(handler.service_handlers["tools"].operation_handlers) == {"list_tools"}
    tools.dynamic.tools.remove("echo")
    with pytest.raises(nexusrpc.HandlerError) as error:
        _ = await handler.start_operation(start_context("echo"), raw({}))
    assert error.value.type == nexusrpc.HandlerErrorType.NOT_FOUND
    await handler.cancel_operation(cancel_context("echo"), "echo:5")
    assert tools.dynamic.cancelled == [("echo", "echo:5")]
    with pytest.raises(nexusrpc.HandlerError):
        await handler.cancel_operation(cancel_context("echo", "missing"), "echo:5")
    assert len(tools.dynamic.cancelled) == 1


def test_dynamic_handler_validation():
    with pytest.raises(ValueError, match="cannot specify name"):
        _ = operation_handler(name="named", dynamic=True)
    with pytest.raises(RuntimeError, match="more than one dynamic"):

        @service_handler(name="duplicate")
        class _Duplicate(Tools):
            @operation_handler(dynamic=True)
            def other(self) -> OperationHandler[nexusrpc.LazyValue, Any]:
                return Dynamic()

        _ = _Duplicate

    with pytest.raises(TypeError, match="LazyValue"):

        @service_handler(name="decoded")
        class _Decoded:
            @operation_handler(dynamic=True)
            def forward(self) -> OperationHandler[dict[str, Any], Any]:
                raise AssertionError("Reject decoded input before creating the handler")

        _ = _Decoded


@pytest.mark.asyncio
async def test_dynamic_handler_with_explicit_service_and_no_named_operations():
    @nexusrpc.service(name="empty")
    class Empty:
        pass

    @service_handler(service=Empty)
    class CatchAll:
        @operation_handler(dynamic=True)
        def forward(self) -> OperationHandler[nexusrpc.LazyValue, Any]:
            return Dynamic()

    instance = CatchAll()
    service = ServiceHandler.from_user_instance(instance)
    assert not service.operation_handlers
    handler = Handler([instance])
    result = await handler.start_operation(
        start_context("echo", "empty"), nexusrpc.LazyValue(DummySerializer({}), {})
    )
    assert isinstance(result, StartOperationResultSync) and result.value[0] == "echo"


@pytest.mark.asyncio
async def test_blocking_dynamic_handler_requires_an_executor():
    class Blocking(OperationHandler[nexusrpc.LazyValue, Any]):
        def start(
            self, ctx: StartOperationContext, input: nexusrpc.LazyValue
        ) -> StartOperationResultSync[Any]:  # type: ignore[override]
            return StartOperationResultSync(asyncio.run(input.consume(as_type=dict)))

        async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
            pass

    @service_handler(name="tools")
    class BlockingService:
        @operation_handler(dynamic=True)
        def forward(self) -> OperationHandler[nexusrpc.LazyValue, Any]:
            return Blocking()

    with pytest.raises(RuntimeError, match="executor"):
        _ = Handler([BlockingService()])
    with ThreadPoolExecutor() as executor:
        handler = Handler([BlockingService()], executor=executor)
        result = await handler.start_operation(
            start_context("echo"), nexusrpc.LazyValue(DummySerializer({"id": 5}), {})
        )
        assert isinstance(result, StartOperationResultSync) and result.value == {
            "id": 5
        }


class RecordingMiddleware(OperationHandlerMiddleware):
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, Any]] = []

    def intercept(
        self, ctx: OperationContext, next: MiddlewareSafeOperationHandler
    ) -> MiddlewareSafeOperationHandler:
        return RecordingOperationHandler(next, self)


class RecordingOperationHandler(MiddlewareSafeOperationHandler):
    def __init__(
        self, next: MiddlewareSafeOperationHandler, middleware: RecordingMiddleware
    ) -> None:
        self._next = next
        self._middleware = middleware

    async def start(
        self, ctx: StartOperationContext, input: Any
    ) -> StartOperationResultSync[Any] | StartOperationResultAsync:
        self._middleware.calls.append(("start", ctx.operation, input))
        return await self._next.start(ctx, input)

    async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
        self._middleware.calls.append(("cancel", ctx.operation, token))
        return await self._next.cancel(ctx, token)


@pytest.mark.asyncio
async def test_middleware_runs_for_dynamic_handler():
    middleware = RecordingMiddleware()
    tools = Tools()
    handler = Handler([tools], middleware=[middleware])
    raw = nexusrpc.LazyValue(DummySerializer({"id": 1, "async": True}), {})
    result = await handler.start_operation(start_context("echo"), raw)
    assert isinstance(result, StartOperationResultAsync)
    await handler.cancel_operation(cancel_context("echo"), result.token)
    assert middleware.calls == [
        ("start", "echo", raw),
        ("cancel", "echo", "echo:1"),
    ]
    assert tools.dynamic.cancelled == [("echo", "echo:1")]


@pytest.mark.asyncio
async def test_not_found_from_dynamic_handler_is_final():
    tools = Tools()
    tools.dynamic.tools.clear()
    handler = Handler([tools])
    with pytest.raises(nexusrpc.HandlerError) as error:
        _ = await handler.start_operation(
            start_context("echo"), nexusrpc.LazyValue(DummySerializer({}), {})
        )
    assert error.value.type == nexusrpc.HandlerErrorType.NOT_FOUND
    assert str(error.value) == "Unknown tool"


@pytest.mark.asyncio
async def test_unknown_operation_without_dynamic_handler_is_not_found():
    @service_handler(name="named")
    class NamedOnly:
        @sync_operation
        async def list_tools(self, _ctx: StartOperationContext, _input: None) -> str:
            return "manifest"

    handler = Handler([NamedOnly()])
    with pytest.raises(nexusrpc.HandlerError) as error:
        _ = await handler.start_operation(
            start_context("echo", "named"), nexusrpc.LazyValue(DummySerializer({}), {})
        )
    assert error.value.type == nexusrpc.HandlerErrorType.NOT_FOUND
    with pytest.raises(nexusrpc.HandlerError) as error:
        await handler.cancel_operation(cancel_context("echo", "named"), "token")
    assert error.value.type == nexusrpc.HandlerErrorType.NOT_FOUND
