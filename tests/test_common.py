from nexusrpc._common import (
    HandlerError,
    HandlerErrorType,
)


def test_handler_error_retryable_behavior():
    """Test retryable behavior based on error type and override."""
    # Retryable error type (RESOURCE_EXHAUSTED)
    retryable_type = HandlerErrorType.RESOURCE_EXHAUSTED
    err = HandlerError("test", type=retryable_type)
    assert err.retryable
    assert err.type == retryable_type
    assert err.raw_error_type == retryable_type.value

    err = HandlerError("test", type=retryable_type, retryable_override=False)
    assert not err.retryable

    # Non-retryable error type (BAD_REQUEST)
    non_retryable_type = HandlerErrorType.BAD_REQUEST
    err = HandlerError("test", type=non_retryable_type)
    assert not err.retryable
    assert err.type == non_retryable_type
    assert err.raw_error_type == non_retryable_type.value

    err = HandlerError("test", type=non_retryable_type, retryable_override=True)
    assert err.retryable


def test_handler_error_unknown_error_type():
    """Test handling of unknown error type strings."""
    err = HandlerError("test", type="SOME_UNKNOWN_TYPE")
    assert err.retryable
    assert err.type == HandlerErrorType.UNKNOWN
    assert err.raw_error_type == "SOME_UNKNOWN_TYPE"

    err = HandlerError("test", type="SOME_UNKNOWN_TYPE", retryable_override=False)
    assert not err.retryable
