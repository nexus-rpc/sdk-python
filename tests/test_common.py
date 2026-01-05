from nexusrpc._common import HandlerError, HandlerErrorType


def test_handler_error_retryable_type():
    retryable_error_type = HandlerErrorType.RESOURCE_EXHAUSTED
    err = HandlerError(
        "test",
        error_type=retryable_error_type,
        retryable_override=True,
    )
    assert err.retryable
    assert err.error_type == retryable_error_type
    assert err.raw_error_type == retryable_error_type.value

    err = HandlerError(
        "test",
        error_type=retryable_error_type,
        retryable_override=False,
    )
    assert not err.retryable
    assert err.error_type == retryable_error_type
    assert err.raw_error_type == retryable_error_type.value

    err = HandlerError(
        "test",
        error_type=retryable_error_type,
    )
    assert err.retryable
    assert err.error_type == retryable_error_type
    assert err.raw_error_type == retryable_error_type.value


def test_handler_error_non_retryable_type():
    non_retryable_error_type = HandlerErrorType.BAD_REQUEST
    err = HandlerError(
        "test",
        error_type=non_retryable_error_type,
        retryable_override=True,
    )
    assert err.retryable
    assert err.error_type == non_retryable_error_type
    assert err.raw_error_type == non_retryable_error_type.value

    err = HandlerError(
        "test",
        error_type=non_retryable_error_type,
        retryable_override=False,
    )
    assert not err.retryable
    assert err.error_type == non_retryable_error_type
    assert err.raw_error_type == non_retryable_error_type.value

    err = HandlerError(
        "test",
        error_type=non_retryable_error_type,
    )
    assert not err.retryable
    assert err.error_type == non_retryable_error_type
    assert err.raw_error_type == non_retryable_error_type.value

def test_handler_error_unknown_error_type():
    # Verify that unknown raw errors are retriable and the error_type is unknown
    err = HandlerError("test", error_type="SOME_UNKNOWN_TYPE")
    assert err.retryable
    assert err.error_type == HandlerErrorType.UNKNOWN
    assert err.raw_error_type == "SOME_UNKNOWN_TYPE"
