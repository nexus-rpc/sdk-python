from nexusrpc._common import HandlerError, HandlerErrorRetryBehavior, HandlerErrorType


def test_handler_error_retryable_type():
    retryable_error_type = HandlerErrorType.RESOURCE_EXHAUSTED
    assert HandlerError(
        "test",
        type=retryable_error_type,
        retry_behavior=HandlerErrorRetryBehavior.RETRYABLE,
    ).retryable

    assert not HandlerError(
        "test",
        type=retryable_error_type,
        retry_behavior=HandlerErrorRetryBehavior.NON_RETRYABLE,
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
        retry_behavior=HandlerErrorRetryBehavior.RETRYABLE,
    ).retryable

    assert not HandlerError(
        "test",
        type=non_retryable_error_type,
        retry_behavior=HandlerErrorRetryBehavior.NON_RETRYABLE,
    ).retryable

    assert not HandlerError(
        "test",
        type=non_retryable_error_type,
    ).retryable
