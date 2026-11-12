# Nexus Python SDK

## What is Nexus?

[Nexus](https://github.com/nexus-rpc/) is a synchronous RPC protocol. Arbitrary duration operations are modeled on top of
a set of pre-defined synchronous RPCs.

A Nexus caller calls a handler. The handler may respond inline (synchronous response) or
return a token referencing the ongoing operation (asynchronous response). The caller can
cancel an asynchronous operation, check for its outcome, or fetch its current state. The
caller can also specify a callback URL, which the handler uses to deliver the result of
an asynchronous operation when it is ready.

## Installation

```
uv add nexus-rpc
```
or
```
pip install nexus-rpc
```

## Usage

The SDK currently supports two use cases:

1. As an end user, defining Nexus services and operations.

2. Implementing a Nexus handler that can accept and respond to incoming Nexus requests, dispatching to the corresponding user-defined Nexus operation.

The handler in (2) would form part of a server or worker that processes Nexus requests; the SDK does not yet provide reference implementations of these, or of a Nexus client.

### Defining Nexus services and operations

```python
from dataclasses import dataclass

import nexusrpc


@dataclass
class MyInput:
    name: str


@dataclass
class MyOutput:
    message: str


@nexusrpc.service
class MyNexusService:
    my_sync_operation: nexusrpc.Operation[MyInput, MyOutput]


@nexusrpc.handler.service_handler(service=MyNexusService)
class MyNexusServiceHandler:
    # You can create an __init__ method accepting what is needed by your operation
    # handlers to handle requests. You will typically instantiate your service handler class
    # when starting your Nexus server/worker.

    # This is a Nexus operation that responds synchronously to all requests. That means
    # that the `start` method returns the final operation result.
    #
    # Sync operations are free to make arbitrary network calls.
    @nexusrpc.handler.sync_operation
    async def my_sync_operation(
        self, ctx: nexusrpc.handler.StartOperationContext, input: MyInput
    ) -> MyOutput:
        return MyOutput(message=f"Hello {input.name}!")
```

---

_Versions 1.0.1 and earlier of the `nexus-rpc` package on PyPI belong to an unrelated
project. The Nexus Python SDK starts at version 1.1.0._

## Dynamic operation handlers

Use `@operation_handler(dynamic=True)` on one factory in a service handler. The
factory returns an `OperationHandler[nexusrpc.LazyValue, Any]`. Its start method
receives raw input and reads the original operation name from `ctx.operation`.
Decode input with `await input.consume(as_type=...)`.

Named operations take precedence. Unmatched start and cancel requests go to the
service's dynamic handler. Unknown services return `NOT_FOUND`. Register the
service instance with `Handler` as usual. A service can have at most one dynamic
handler, and it cannot specify a name. Handlers must support concurrent requests.
Blocking handlers require an executor.

To reject a name, raise `HandlerError` with type `NOT_FOUND`. That error is final;
no other handler runs. Middleware runs for the dynamic handler as for named
operations and receives the unconsumed `LazyValue` as input.
