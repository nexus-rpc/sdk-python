from nexusrpc._common import HandlerError, HandlerErrorType


def test_handler_error_retryable_type():
    retryable_error_type = HandlerErrorType.RESOURCE_EXHAUSTED
    assert HandlerError(
        "test",
        type=retryable_error_type,
        retryable_override=True,
    ).retryable

    assert not HandlerError(
        "test",
        type=retryable_error_type,
        retryable_override=False,
    ).retryable

    assert HandlerError(
        "test",
        type=retryable_error_type,
    ).retryable


def test_handler_error_non_retryable_type():
    non_retryable_error_type = HandlerErrorType.BAD_REQUEST
    assert HandlerError(
        "test",
        type=non_retryable_error_type,
        retryable_override=True,
    ).retryable

    assert not HandlerError(
        "test",
        type=non_retryable_error_type,
        retryable_override=False,
    ).retryable

    assert not HandlerError(
        "test",
        type=non_retryable_error_type,
    ).retryable
