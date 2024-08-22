from unittest.mock import Mock


def create_mock_twilio_client(
    mock_messages_create: bool = True, mock_calls_create: bool = True
) -> Mock:
    """Create a mock Twilio client instance"""

    twilio_client = Mock()

    # Define a function to simulate the behavior of client.messages.create()
    def mock_create(*args, **kwargs):
        # Get the 'body' parameter from the keyword arguments
        body = kwargs.get("body", "")

        # You can customize the message based on the user-submitted 'body'
        # For example, you can prepend "Mocked: " to the body.
        response_message = f"Mocked: {body}"

        # Create a mock message object with the desired response
        message = Mock()
        message.body = response_message
        message.error_code = kwargs.get("error_code", None)
        message.error_message = kwargs.get("error_message", None)
        message.status = kwargs.get("status", "queued")
        message._from = kwargs.get("_from", "+13069884140")
        message.to = kwargs.get("to", "+14036897250")
        message.status_callback = kwargs.get("status_callback", None)

        return message

    if mock_messages_create:
        # Attach the mock create method to client.messages.create
        twilio_client.messages.create = Mock(side_effect=mock_create)

    if mock_calls_create:
        twilio_client.calls.create = Mock(side_effect=mock_create)

    return twilio_client
