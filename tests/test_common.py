from nexusrpc._common import HandlerError, HandlerErrorType


def test_handler_error_retryable_type():
    retryable_error_type = HandlerErrorType.RESOURCE_EXHAUSTED
    assert HandlerError(
        "test",
        type=retryable_error_type,
        retryable=True,
    ).should_be_retried

    assert not HandlerError(
        "test",
        type=retryable_error_type,
        retryable=False,
    ).should_be_retried

    assert HandlerError(
        "test",
        type=retryable_error_type,
    ).should_be_retried


def test_handler_error_non_retryable_type():
    non_retryable_error_type = HandlerErrorType.BAD_REQUEST
    assert HandlerError(
        "test",
        type=non_retryable_error_type,
        retryable=True,
    ).should_be_retried

    assert not HandlerError(
        "test",
        type=non_retryable_error_type,
        retryable=False,
    ).should_be_retried

    assert not HandlerError(
        "test",
        type=non_retryable_error_type,
    ).should_be_retried
