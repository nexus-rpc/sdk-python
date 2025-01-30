# Nexus Python SDK

## What is Nexus?

Nexus is a synchronous RPC protocol. Arbitrary duration operations are modelled on top
of a set of pre-defined synchronous RPCs.

A Nexus caller calls a handler. The handler may respond inline (synchronous response) or
return a token referencing the ongoing operation (asynchronous response). The caller can
cancel an asynchronous operation, check for its outcome, or fetch its current state. The
caller can also specify a callback URL, which the handler uses to deliver the result of
an asynchronous operation when it is ready.

TODO(prerelease): README content