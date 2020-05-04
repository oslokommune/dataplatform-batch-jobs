from io import BytesIO, StringIO
from unittest.mock import Mock


def _mock_output_target(result_class):
    result = result_class()
    # Don't permit actually closing the IO stream, since that will discard the
    # buffer before we get a chance to read it.
    result.close = Mock()
    output_target = Mock(open=Mock(return_value=result))

    return result, output_target


def mock_byte_output_target():
    """Return a byte stream and an open()-able mock object that can be used to read
    and write to and from the stream.
    """
    return _mock_output_target(BytesIO)


def mock_string_output_target():
    """Return a string stream and an open()-able mock object that can be used to
    read and write to and from the stream.
    """
    return _mock_output_target(StringIO)
