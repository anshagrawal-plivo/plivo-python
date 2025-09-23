# -*- coding: utf-8 -*-
"""
Plivo Audio Streaming WebSocket Abstraction Layer

This module provides an abstraction layer for handling Plivo audio streaming
over WebSocket connections. It allows users to easily handle streaming events
and send audio data.
"""

import json
import threading
from typing import Callable, Optional, Dict, Any

try:
    from plivo.exceptions import PlivoRestError, InvalidRequestError
except ImportError:
    # Fallback for standalone usage
    class PlivoRestError(Exception):
        pass

    class InvalidRequestError(PlivoRestError):
        pass


try:
    import websocket
except ImportError:
    websocket = None


class PlivoAudioStreamError(PlivoRestError):
    """Exception raised for audio streaming errors"""

    pass


class PlivoAudioStreamClient:
    """
    WebSocket-based audio streaming client for Plivo.

    This class provides an abstraction layer over WebSocket connections
    for handling Plivo audio streaming. It manages the connection lifecycle
    and provides event-driven handlers for different streaming events.

    Example:
        ws = websocket.WebSocket()
        stream_client = PlivoAudioStreamClient(ws)

        @stream_client.onAudio
        def handle_audio(data):
            print("Received audio data:", data)

        @stream_client.onStart
        def handle_start(data):
            print("Stream started:", data)

        @stream_client.onEnd
        def handle_end(data):
            print("Stream ended:", data)

        stream_client.start_listening()
        stream_client.playAudio(base64_audio_data)
    """

    def __init__(self, websocket_connection):
        """
        Initialize the PlivoAudioStreamClient.

        Args:
            websocket_connection: An active WebSocket connection object
        """
        if websocket is None:
            raise PlivoRestError(
                "websocket-client library is required for audio streaming. "
                "Install it with: pip install websocket-client"
            )

        if not websocket_connection:
            raise InvalidRequestError("WebSocket connection is required")

        self._websocket = websocket_connection
        self._is_listening = False
        self._listener_thread = None
        self._event_handlers = {"media": None, "start": None, "end": None}

    def onAudio(self, handler: Callable[[Dict[str, Any]], None]) -> Callable:
        """
        Register an event handler for audio data events.

        This method is called when websocket receives data.event === 'media'

        Args:
            handler: Function to call when audio data is received.
                    Should accept a dictionary containing the media data.

        Returns:
            The handler function (for decorator usage)
        """
        if not callable(handler):
            raise InvalidRequestError("Handler must be callable")

        self._event_handlers["media"] = handler
        return handler

    def onStart(self, handler: Callable[[Dict[str, Any]], None]) -> Callable:
        """
        Register an event handler for stream start events.

        This method is called when websocket receives data.event === 'start'

        Args:
            handler: Function to call when stream starts.
                    Should accept a dictionary containing the start event data.

        Returns:
            The handler function (for decorator usage)
        """
        if not callable(handler):
            raise InvalidRequestError("Handler must be callable")

        self._event_handlers["start"] = handler
        return handler

    def onEnd(self, handler: Callable[[Dict[str, Any]], None]) -> Callable:
        """
        Register an event handler for stream end events.

        This method is called when websocket receives data.event === 'end'

        Args:
            handler: Function to call when stream ends.
                    Should accept a dictionary containing the end event data.

        Returns:
            The handler function (for decorator usage)
        """
        if not callable(handler):
            raise InvalidRequestError("Handler must be callable")

        self._event_handlers["end"] = handler
        return handler

    def playAudio(
        self,
        audio_data: str,
        sample_rate: int = 24000,
        content_type: str = "audio/x-l16",
    ) -> None:
        """
        Send base64 audio data to the WebSocket connection.

        Args:
            audio_data: Base64 encoded audio data
            sample_rate: Audio sample rate (default: 24000)
            content_type: Audio content type (default: "audio/x-l16")

        Raises:
            PlivoAudioStreamError: If there's an error sending the audio data
            InvalidRequestError: If audio_data is invalid
        """
        if not audio_data:
            raise InvalidRequestError("Audio data is required")

        if not isinstance(audio_data, str):
            raise InvalidRequestError("Audio data must be a base64 encoded string")

        message = {
            "event": "playAudio",
            "media": {
                "payload": audio_data,
                "sampleRate": sample_rate,
                "contentType": content_type,
            },
        }

        try:
            message_json = json.dumps(message)
            self._websocket.send(message_json)
        except Exception as e:
            raise PlivoAudioStreamError(f"Failed to send audio data: {str(e)}")

    def start_listening(self) -> None:
        """
        Start listening for WebSocket messages in a separate thread.

        This method will continuously listen for incoming WebSocket messages
        and dispatch them to the appropriate event handlers based on the
        event type.

        Raises:
            PlivoAudioStreamError: If already listening or WebSocket is not connected
        """
        if self._is_listening:
            raise PlivoAudioStreamError("Already listening for messages")

        if not self._websocket:
            raise PlivoAudioStreamError("WebSocket connection not available")

        self._is_listening = True
        self._listener_thread = threading.Thread(target=self._message_listener)
        self._listener_thread.daemon = True
        self._listener_thread.start()

    def stop_listening(self) -> None:
        """
        Stop listening for WebSocket messages.

        This method will stop the message listener thread and clean up resources.
        """
        self._is_listening = False
        if self._listener_thread and self._listener_thread.is_alive():
            self._listener_thread.join(timeout=1.0)

    def _message_listener(self) -> None:
        """
        Internal method to listen for WebSocket messages.

        This runs in a separate thread and processes incoming messages,
        dispatching them to the appropriate event handlers.
        """
        try:
            while self._is_listening:
                try:
                    # Receive message from WebSocket
                    message = self._websocket.recv()
                    if not message:
                        continue

                    # Parse JSON message
                    try:
                        data = json.loads(message)
                    except json.JSONDecodeError as e:
                        # Log or handle invalid JSON, but don't crash
                        continue

                    # Dispatch to appropriate handler based on event type
                    event_type = data.get("event")
                    if event_type == "media" and self._event_handlers["media"]:
                        self._event_handlers["media"](data)
                    elif event_type == "start" and self._event_handlers["start"]:
                        self._event_handlers["start"](data)
                    elif event_type == "end" and self._event_handlers["end"]:
                        self._event_handlers["end"](data)

                except Exception as e:
                    # Handle WebSocket timeout exceptions if websocket library is available
                    if (
                        websocket
                        and hasattr(websocket, "WebSocketTimeoutException")
                        and isinstance(e, websocket.WebSocketTimeoutException)
                    ):
                        # Timeout is normal, continue listening
                        continue
                    else:
                        # Log error but don't crash the listener
                        # In a production environment, you might want to use proper logging
                        print(f"Error in message listener: {str(e)}")
                        # For critical errors, we might want to break the loop
                        break

        except Exception as e:
            # Critical error, stop listening
            self._is_listening = False
            raise PlivoAudioStreamError(f"Message listener failed: {str(e)}")

    def is_listening(self) -> bool:
        """
        Check if the client is currently listening for messages.

        Returns:
            True if listening, False otherwise
        """
        return self._is_listening

    def get_websocket(self):
        """
        Get the underlying WebSocket connection.

        Returns:
            The WebSocket connection object
        """
        return self._websocket

    def close(self) -> None:
        """
        Close the WebSocket connection and stop listening.

        This method cleanly shuts down the streaming client.
        """
        self.stop_listening()
        if self._websocket:
            try:
                self._websocket.close()
            except Exception:
                # Ignore errors when closing
                pass

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
