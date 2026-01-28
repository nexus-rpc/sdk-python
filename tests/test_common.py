import pytest

from nexusrpc._common import (
    Failure,
    HandlerError,
    HandlerErrorType,
    OperationError,
    OperationErrorState,
)


def test_failure_basic():
    f = Failure("test message")
    assert str(f) == "test message"
    assert f.message == "test message"
    assert f.stack_trace is None
    assert f.metadata is None
    assert f.details is None
    assert f.cause is None
    assert isinstance(f, Exception)


def test_failure_with_all_fields():
    cause = Failure("root cause")
    f = Failure(
        "test",
        stack_trace="Traceback:\n  File 'test.py', line 1",
        metadata={"key": "value"},
        details={"code": 123},
        cause=cause,
    )
    assert f.message == "test"
    assert f.stack_trace == "Traceback:\n  File 'test.py', line 1"
    assert f.metadata == {"key": "value"}
    assert f.details == {"code": 123}
    assert f.cause is cause


def test_handler_error_spec_representation():
    """Test that HandlerError is a Failure and sets metadata/details per spec."""
    # Basic error with spec-compliant metadata and details
    err = HandlerError("test", error_type=HandlerErrorType.INTERNAL)
    assert isinstance(err, Failure)
    assert isinstance(err, Exception)
    assert err.message == "test"
    assert err.metadata == {"type": "nexus.HandlerError"}
    assert err.details == {"type": "INTERNAL"}

    # With retryable_override
    err_with_retry = HandlerError(
        "test",
        error_type=HandlerErrorType.INTERNAL,
        retryable_override=True,
    )
    assert err_with_retry.details == {"type": "INTERNAL", "retryableOverride": True}


def test_handler_error_with_all_fields():
    """Test HandlerError with all Failure fields populated."""
    cause = Failure("root cause")
    err = HandlerError(
        "test",
        error_type=HandlerErrorType.INTERNAL,
        stack_trace="stack trace",
        metadata={"k": "v"},
        details={"code": 1},
        cause=cause,
    )
    assert err.message == "test"
    assert err.stack_trace == "stack trace"
    # User-provided keys merged with spec-required keys
    assert err.metadata == {"type": "nexus.HandlerError", "k": "v"}
    assert err.details == {"type": "INTERNAL", "code": 1}
    assert err.cause is cause


def test_handler_error_spec_keys_cannot_be_overridden():
    """Test that user-provided values cannot override spec-required keys."""
    err = HandlerError(
        "test",
        error_type=HandlerErrorType.INTERNAL,
        retryable_override=True,
        metadata={"type": "user-type", "user-key": "user-value"},
        details={
            "type": "user-type",
            "retryableOverride": False,
            "user-key": "user-value",
        },
    )
    assert err.metadata is not None
    assert err.details is not None
    # Spec keys take precedence
    assert err.metadata["type"] == "nexus.HandlerError"
    assert err.details["type"] == "INTERNAL"
    assert err.details["retryableOverride"] is True
    # User keys are preserved
    assert err.metadata["user-key"] == "user-value"
    assert err.details["user-key"] == "user-value"


def test_handler_error_retryable_behavior():
    """Test retryable behavior based on error type and override."""
    # Retryable error type (RESOURCE_EXHAUSTED)
    retryable_type = HandlerErrorType.RESOURCE_EXHAUSTED
    err = HandlerError("test", error_type=retryable_type)
    assert err.retryable
    assert err.error_type == retryable_type
    assert err.raw_error_type == retryable_type.value

    err = HandlerError("test", error_type=retryable_type, retryable_override=False)
    assert not err.retryable

    # Non-retryable error type (BAD_REQUEST)
    non_retryable_type = HandlerErrorType.BAD_REQUEST
    err = HandlerError("test", error_type=non_retryable_type)
    assert not err.retryable
    assert err.error_type == non_retryable_type
    assert err.raw_error_type == non_retryable_type.value

    err = HandlerError("test", error_type=non_retryable_type, retryable_override=True)
    assert err.retryable


def test_handler_error_unknown_error_type():
    """Test handling of unknown error type strings."""
    err = HandlerError("test", error_type="SOME_UNKNOWN_TYPE")
    assert err.retryable
    assert err.error_type == HandlerErrorType.UNKNOWN
    assert err.raw_error_type == "SOME_UNKNOWN_TYPE"

    err = HandlerError("test", error_type="SOME_UNKNOWN_TYPE", retryable_override=False)
    assert not err.retryable


def test_operation_error_spec_representation():
    """Test that OperationError is a Failure and sets metadata/details per spec."""
    # Failed state
    err = OperationError("test", state=OperationErrorState.FAILED)
    assert isinstance(err, Failure)
    assert isinstance(err, Exception)
    assert err.message == "test"
    assert err.state == OperationErrorState.FAILED
    assert err.metadata == {"type": "nexus.OperationError"}
    assert err.details == {"state": "failed"}

    # Canceled state
    err_canceled = OperationError("test", state=OperationErrorState.CANCELED)
    assert err_canceled.state == OperationErrorState.CANCELED
    assert err_canceled.details == {"state": "canceled"}


def test_operation_error_with_all_fields():
    """Test OperationError with all Failure fields populated."""
    cause = Failure("root cause")
    err = OperationError(
        "test",
        state=OperationErrorState.CANCELED,
        stack_trace="stack trace",
        metadata={"k": "v"},
        details={"code": 1},
        cause=cause,
    )
    assert err.message == "test"
    assert err.state == OperationErrorState.CANCELED
    assert err.stack_trace == "stack trace"
    # User-provided keys merged with spec-required keys
    assert err.metadata == {"type": "nexus.OperationError", "k": "v"}
    assert err.details == {"state": "canceled", "code": 1}
    assert err.cause is cause


def test_operation_error_spec_keys_cannot_be_overridden():
    """Test that user-provided values cannot override spec-required keys."""
    err = OperationError(
        "test",
        state=OperationErrorState.FAILED,
        metadata={"type": "user-type", "user-key": "user-value"},
        details={"state": "user-state", "user-key": "user-value"},
    )
    assert err.metadata is not None
    assert err.details is not None
    # Spec keys take precedence
    assert err.metadata["type"] == "nexus.OperationError"
    assert err.details["state"] == "failed"
    # User keys are preserved
    assert err.metadata["user-key"] == "user-value"
    assert err.details["user-key"] == "user-value"


def test_failure_traceback_when_raised():
    """Test that stack_trace captures traceback when exception is raised."""
    # Failure
    try:
        raise Failure("raised failure")
    except Failure as f:
        assert f.stack_trace is not None
        assert "test_failure_traceback_when_raised" in f.stack_trace
        assert "raise Failure" in f.stack_trace

    # HandlerError
    try:
        raise HandlerError("raised handler error", error_type=HandlerErrorType.INTERNAL)
    except HandlerError as e:
        assert e.stack_trace is not None
        assert "test_failure_traceback_when_raised" in e.stack_trace
        assert "raise HandlerError" in e.stack_trace

    # OperationError
    try:
        raise OperationError("raised operation error", state=OperationErrorState.FAILED)
    except OperationError as e:
        assert e.stack_trace is not None
        assert "test_failure_traceback_when_raised" in e.stack_trace
        assert "raise OperationError" in e.stack_trace


def test_explicit_stack_trace_takes_precedence():
    """Test that explicit stack_trace takes precedence over __traceback__."""
    try:
        raise Failure("test", stack_trace="explicit trace")
    except Failure as f:
        # Even though __traceback__ is set, explicit stack_trace wins
        assert f.stack_trace == "explicit trace"
        assert f.__traceback__ is not None  # Verify traceback exists


def test_metadata_details_immutable():
    """Test that metadata and details cannot be modified after construction."""
    err = HandlerError("test", error_type=HandlerErrorType.INTERNAL)

    with pytest.raises(TypeError):
        err.metadata["new_key"] = "value"  # type: ignore[index]

    with pytest.raises(TypeError):
        err.details["new_key"] = "value"  # type: ignore[index]


def test_failure_repr():
    """Test __repr__ methods for debugging."""
    # Failure
    f = Failure("test message", metadata={"k": "v"}, details={"code": 1})
    repr_str = repr(f)
    assert "Failure(" in repr_str
    assert "message='test message'" in repr_str

    # HandlerError
    err = HandlerError("test", error_type=HandlerErrorType.INTERNAL)
    repr_str = repr(err)
    assert "HandlerError(" in repr_str
    assert "message='test'" in repr_str
    assert "error_type=" in repr_str
    assert "retryable=" in repr_str

    # OperationError
    op_err = OperationError("test", state=OperationErrorState.FAILED)
    repr_str = repr(op_err)
    assert "OperationError(" in repr_str
    assert "message='test'" in repr_str
    assert "state=" in repr_str
