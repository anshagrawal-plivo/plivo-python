# -*- coding: utf-8 -*-
"""
Plivo Audio Streaming WebSocket Abstraction Layer

This module provides an abstraction layer for handling Plivo audio streaming
over WebSocket connections. It allows users to easily handle streaming events
and send audio data.
"""

import base64
import json
import threading
import asyncio
import numpy as np
from typing import Callable, Optional, Dict, Any, Union

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

try:
    import librosa
except ImportError:
    librosa = None


class PlivoAudioStreamError(PlivoRestError):
    """Exception raised for audio streaming errors"""

    pass


def _resample_audio_for_plivo(
    audio_data: bytes, 
    original_sample_rate: int, 
    target_format: str = "l16_16khz",
    original_channels: int = 1
) -> tuple[bytes, int, str]:
    """
    Resample audio data to Plivo-supported formats.
    
    Plivo supports:
    - 8kHz with mulaw (audio/x-mulaw)
    - 16kHz with l-16 (audio/x-l16)
    
    Args:
        audio_data: Raw audio bytes (assumed to be 16-bit PCM)
        original_sample_rate: Original sample rate of the audio
        target_format: Either "mulaw_8khz" or "l16_16khz" (default)
        original_channels: Number of channels in original audio (default: 1)
        
    Returns:
        Tuple of (resampled_audio_bytes, sample_rate, content_type)
        
    Raises:
        PlivoAudioStreamError: If librosa is not available or resampling fails
    """
    if librosa is None:
        raise PlivoAudioStreamError(
            "librosa library is required for audio resampling. "
            "Install it with: pip install librosa"
        )
    
    if target_format not in ["mulaw_8khz", "l16_16khz"]:
        raise InvalidRequestError("target_format must be either 'mulaw_8khz' or 'l16_16khz'")
    
    try:
        # Convert bytes to numpy array (assuming 16-bit PCM)
        audio_array = np.frombuffer(audio_data, dtype=np.int16).astype(np.float32)
        
        # Normalize to [-1, 1] range
        audio_array = audio_array / 32768.0
        
        # Handle multi-channel audio by converting to mono
        if original_channels > 1:
            audio_array = audio_array.reshape(-1, original_channels).mean(axis=1)
        
        # Determine target sample rate and content type
        if target_format == "mulaw_8khz":
            target_sample_rate = 8000
            target_content_type = "audio/x-mulaw"
        else:  # l16_16khz
            target_sample_rate = 16000
            target_content_type = "audio/x-l16"
        
        # Resample if needed
        if original_sample_rate != target_sample_rate:
            audio_array = librosa.resample(
                audio_array, 
                orig_sr=original_sample_rate, 
                target_sr=target_sample_rate
            )
        
        # Convert back to appropriate format
        if target_format == "mulaw_8khz":
            # Convert to mu-law encoding
            # First, convert to 16-bit PCM
            audio_int16 = (audio_array * 32767).astype(np.int16)
            # Then convert to mu-law (simplified approach)
            # For proper mu-law encoding, you might want to use a dedicated library
            # But for basic functionality, we'll convert to 8-bit and use as mu-law approximation
            audio_mulaw = ((audio_int16 / 256) + 128).astype(np.uint8)
            resampled_bytes = audio_mulaw.tobytes()
        else:
            # Convert back to 16-bit PCM for l-16
            audio_int16 = (audio_array * 32767).astype(np.int16)
            resampled_bytes = audio_int16.tobytes()
        
        return resampled_bytes, target_sample_rate, target_content_type
        
    except Exception as e:
        raise PlivoAudioStreamError(f"Failed to resample audio: {str(e)}")


# def _detect_audio_format(audio_data: bytes) -> tuple[int, int]:
#     """
#     Attempt to detect basic audio format parameters from raw audio data.
    
#     This is a simple heuristic-based detection and may not be accurate for all formats.
#     It's better to explicitly provide sample_rate and channels when possible.
    
#     Args:
#         audio_data: Raw audio bytes
        
#     Returns:
#         Tuple of (estimated_sample_rate, estimated_channels)
#     """
#     # Simple heuristic: assume 16-bit PCM and estimate based on data length
#     # This is not reliable and should be used as fallback only
#     data_length = len(audio_data)
    
#     # Common sample rates to test
#     common_rates = [8000, 16000, 22050, 24000, 44100, 48000]
    
#     # Assume 16-bit samples (2 bytes per sample)
#     bytes_per_sample = 2
    
#     # Estimate channels (assume mono or stereo)
#     # This is very rough estimation
#     estimated_channels = 1
#     if data_length > 48000:  # If more than ~1 second at 48kHz mono
#         estimated_channels = 2 if (data_length % 4 == 0) else 1
    
#     # Estimate sample rate (very rough)
#     samples_per_channel = data_length // (bytes_per_sample * estimated_channels)
    
#     # Find closest common sample rate (assume ~1 second of audio)
#     estimated_sample_rate = min(common_rates, key=lambda x: abs(x - samples_per_channel))
    
#     return estimated_sample_rate, estimated_channels


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

    def __init__(self, websocket_connection, chunk_size: int = 8192):
        """
        Initialize the PlivoAudioStreamClient.

        Args:
            websocket_connection: An active WebSocket connection object
            chunk_size: Size of each audio chunk in bytes for streaming (default: 8192)
        """
        if websocket is None:
            raise PlivoRestError(
                "websocket-client library is required for audio streaming. "
                "Install it with: pip install websocket-client"
            )

        if not websocket_connection:
            raise InvalidRequestError("WebSocket connection is required")

        if chunk_size <= 0:
            raise InvalidRequestError("Chunk size must be positive")

        self._websocket = websocket_connection
        self._is_listening = False
        self._listener_thread = None
        self._event_handlers = {"media": None, "start": None, "end": None}
        self._chunk_size = chunk_size

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
        audio_data: bytes,
        sample_rate: int = 16000,
        content_type: str = "audio/x-l16", 
        target_format: str = "l16_16khz",
        original_channels: int = 1,
        enable_resampling: bool = True,
    ) -> None:
        """
        Send audio data to the WebSocket connection with chunking and resampling support.
        
        Automatically resamples audio to Plivo-supported formats:
        - 8kHz with mulaw (audio/x-mulaw) when target_format="mulaw_8khz"
        - 16kHz with l-16 (audio/x-l16) when target_format="l16_16khz"

        Args:
            audio_data: Raw audio bytes (assumed to be 16-bit PCM)
            sample_rate: Original audio sample rate (default: 16000)
            content_type: Audio content type - will be overridden if resampling is enabled
            target_format: Either "mulaw_8khz" or "l16_16khz" (default: "l16_16khz")
            original_channels: Number of channels in original audio (default: 1)
            enable_resampling: Whether to enable automatic resampling (default: True)

        Raises:
            PlivoAudioStreamError: If there's an error sending the audio data
            InvalidRequestError: If audio_data is invalid
            
        Note:
            Audio chunking size is configured in the constructor (default: 8192 bytes)
        """
        if not audio_data:
            raise InvalidRequestError("Audio data is required")

        if not isinstance(audio_data, bytes):
            raise InvalidRequestError("Audio data must be a bytes object")

        # Resample audio if enabled
        processed_audio_data = audio_data
        final_sample_rate = sample_rate
        final_content_type = content_type
        
        if enable_resampling:
            try:
                processed_audio_data, final_sample_rate, final_content_type = _resample_audio_for_plivo(
                    audio_data=audio_data,
                    original_sample_rate=sample_rate,
                    target_format=target_format,
                    original_channels=original_channels
                )
            except PlivoAudioStreamError:
                # Re-raise resampling errors
                raise
            except Exception as e:
                raise PlivoAudioStreamError(f"Failed to resample audio: {str(e)}")

        # Split audio data into chunks
        total_chunks = (len(processed_audio_data) + self._chunk_size - 1) // self._chunk_size
        
        for chunk_index in range(total_chunks):
            start_pos = chunk_index * self._chunk_size
            end_pos = min(start_pos + self._chunk_size, len(processed_audio_data))
            chunk = processed_audio_data[start_pos:end_pos]
            
            # encode the chunk to base64
            chunk_base64 = base64.b64encode(chunk).decode('utf-8')
            
            message = {
                "event": "playAudio",
                "media": {
                    "payload": chunk_base64,
                    "sampleRate": final_sample_rate,
                    "contentType": final_content_type,
                },
                "sequenceNumber": chunk_index,
                "totalChunks": total_chunks,
                "chunkIndex": chunk_index,
            }

            try:
                message_json = json.dumps(message)
                self._send_message(message_json)
            except Exception as e:
                raise PlivoAudioStreamError(f"Failed to send audio chunk {chunk_index}: {str(e)}")

    def send_checkpoint(self) -> None:
        """
        Send a checkpoint message to the WebSocket connection.
        """
        message = {
            "event": "checkpoint",
        }
        try:
            message_json = json.dumps(message)
            self._send_message(message_json)
        except Exception as e:
            raise PlivoAudioStreamError(f"Failed to send checkpoint message: {str(e)}")

    

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
                    # Receive message from WebSocket - handle different WebSocket implementations
                    message = self._receive_message()
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

    def _receive_message(self):
        """
        Receive a message from the WebSocket, handling different WebSocket implementations.

        Returns:
            The received message string

        Raises:
            PlivoAudioStreamError: If no compatible receive method is found
        """
        # Try different WebSocket receive methods based on the library

        # Method 1: websocket-client library
        if hasattr(self._websocket, "recv"):
            return self._websocket.recv()

        # Method 2: Some WebSocket implementations use 'receive'
        elif hasattr(self._websocket, "receive"):
            return self._websocket.receive()

        # Method 3: Some implementations use 'receive_text'
        elif hasattr(self._websocket, "receive_text"):
            return self._websocket.receive_text()

        # Method 4: Check if it's an asyncio WebSocket (websockets library)
        elif hasattr(self._websocket, "__aiter__"):
            raise PlivoAudioStreamError(
                "Asyncio WebSocket detected. This client requires a synchronous WebSocket. "
                "Use PlivoAsyncAudioStreamClient for asyncio WebSockets or websocket-client library for sync."
            )

        # Method 5: Try to get available methods for debugging
        else:
            available_methods = [
                method
                for method in dir(self._websocket)
                if not method.startswith("_")
                and callable(getattr(self._websocket, method))
            ]
            raise PlivoAudioStreamError(
                f"WebSocket object doesn't have a compatible receive method. "
                f"Available methods: {available_methods}. "
                f"Please ensure you're using a compatible WebSocket implementation like websocket-client."
            )

    def _send_message(self, message):
        """
        Send a message to the WebSocket, handling different WebSocket implementations.

        Args:
            message: The message string to send

        Raises:
            PlivoAudioStreamError: If no compatible send method is found
        """
        # Try different WebSocket send methods based on the library
        # Method 1: Some implementations use 'send_text'
        if hasattr(self._websocket, "send_text"):
            return self._websocket.send_text(message)

        # Method 2
        elif hasattr(self._websocket, "send"):
            return self._websocket.send(message)

        # Method 3: Some implementations use 'write'
        elif hasattr(self._websocket, "write"):
            return self._websocket.write(message)

        # Method 4: Check if it's an asyncio WebSocket (websockets library)
        elif hasattr(self._websocket, "__aiter__"):
            raise PlivoAudioStreamError(
                "Asyncio WebSocket detected. This client requires a synchronous WebSocket. "
                "Use PlivoAsyncAudioStreamClient for asyncio WebSockets or websocket-client library for sync."
            )

        # Method 5: No compatible send method found
        else:
            available_methods = [
                method
                for method in dir(self._websocket)
                if not method.startswith("_")
                and callable(getattr(self._websocket, method))
            ]
            raise PlivoAudioStreamError(
                f"WebSocket object doesn't have a compatible send method. "
                f"Available methods: {available_methods}. "
                f"Please ensure you're using a compatible WebSocket implementation like websocket-client."
            )

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


class PlivoAsyncAudioStreamClient:
    """
    Asyncio-based WebSocket audio streaming client for Plivo.

    This class provides an asynchronous abstraction layer over WebSocket connections
    for handling Plivo audio streaming. It's designed to work with asyncio-based
    WebSocket libraries like `websockets`.

    Example:
        import asyncio
        import websockets  # pip install websockets
        from plivo import PlivoAsyncAudioStreamClient

        async def main():
            async with websockets.connect("wss://your-endpoint") as ws:
                stream_client = PlivoAsyncAudioStreamClient(ws)

                @stream_client.onAudio
                async def handle_audio(data):
                    print("Received audio data:", data)

                @stream_client.onStart
                async def handle_start(data):
                    print("Stream started:", data)

                @stream_client.onEnd
                async def handle_end(data):
                    print("Stream ended:", data)

                # Start listening (runs in background)
                listen_task = asyncio.create_task(stream_client.start_listening())

                # Send audio data
                await stream_client.playAudio(base64_audio_data)

                # Wait for listening to complete or cancel it
                await listen_task

        asyncio.run(main())
    """

    def __init__(self, websocket_connection, chunk_size: int = 8192):
        """
        Initialize the PlivoAsyncAudioStreamClient.

        Args:
            websocket_connection: An active async WebSocket connection object
            chunk_size: Size of each audio chunk in bytes for streaming (default: 8192)
        """
        if not websocket_connection:
            raise InvalidRequestError("WebSocket connection is required")

        if chunk_size <= 0:
            raise InvalidRequestError("Chunk size must be positive")

        self._websocket = websocket_connection
        self._is_listening = False
        self._event_handlers = {"media": None, "start": None, "end": None}
        self._chunk_size = chunk_size

    def onAudio(self, handler: Callable[[Dict[str, Any]], Any]) -> Callable:
        """
        Register an async event handler for audio data events.

        This method is called when websocket receives data.event === 'media'

        Args:
            handler: Async function to call when audio data is received.
                    Should accept a dictionary containing the media data.

        Returns:
            The handler function (for decorator usage)
        """
        if not callable(handler):
            raise InvalidRequestError("Handler must be callable")

        self._event_handlers["media"] = handler
        return handler

    def onStart(self, handler: Callable[[Dict[str, Any]], Any]) -> Callable:
        """
        Register an async event handler for stream start events.

        This method is called when websocket receives data.event === 'start'

        Args:
            handler: Async function to call when stream starts.
                    Should accept a dictionary containing the start event data.

        Returns:
            The handler function (for decorator usage)
        """
        if not callable(handler):
            raise InvalidRequestError("Handler must be callable")

        self._event_handlers["start"] = handler
        return handler

    def onEnd(self, handler: Callable[[Dict[str, Any]], Any]) -> Callable:
        """
        Register an async event handler for stream end events.

        This method is called when websocket receives data.event === 'end'

        Args:
            handler: Async function to call when stream ends.
                    Should accept a dictionary containing the end event data.

        Returns:
            The handler function (for decorator usage)
        """
        if not callable(handler):
            raise InvalidRequestError("Handler must be callable")

        self._event_handlers["end"] = handler
        return handler

    async def playAudio(
        self,
        audio_data: Union[str, bytes],
        sample_rate: int = 16000,
        content_type: str = "audio/x-l16",
        target_format: str = "l16_16khz",
        original_channels: int = 1,
        enable_resampling: bool = True,
    ) -> None:
        """
        Send audio data to the WebSocket with chunking and resampling support.
        
        Automatically resamples audio to Plivo-supported formats:
        - 8kHz with mulaw (audio/x-mulaw) when target_format="mulaw_8khz"  
        - 16kHz with l-16 (audio/x-l16) when target_format="l16_16khz"

        Args:
            audio_data: Base64 encoded string or raw bytes (assumed to be 16-bit PCM if bytes)
            sample_rate: Original audio sample rate (default: 16000)
            content_type: Audio content type - will be overridden if resampling is enabled
            target_format: Either "mulaw_8khz" or "l16_16khz" (default: "l16_16khz")
            original_channels: Number of channels in original audio (default: 1)
            enable_resampling: Whether to enable automatic resampling (default: True)

        Raises:
            PlivoAudioStreamError: If there's an error sending the audio data
            InvalidRequestError: If audio_data is invalid
            
        Note:
            Audio chunking size is configured in the constructor (default: 8192 bytes)
        """
        if not audio_data:
            raise InvalidRequestError("Audio data is required")

        # Handle both bytes and base64 string input
        if isinstance(audio_data, bytes):
            # Raw bytes input
            raw_data = audio_data
        elif isinstance(audio_data, str):
            # Decode base64 to bytes
            try:
                raw_data = base64.b64decode(audio_data)
            except Exception as e:
                raise InvalidRequestError(f"Invalid base64 audio data: {str(e)}")
        else:
            raise InvalidRequestError("Audio data must be bytes or base64 encoded string")

        # Resample audio if enabled
        processed_audio_data = raw_data
        final_sample_rate = sample_rate
        final_content_type = content_type
        
        if enable_resampling:
            try:
                processed_audio_data, final_sample_rate, final_content_type = _resample_audio_for_plivo(
                    audio_data=raw_data,
                    original_sample_rate=sample_rate,
                    target_format=target_format,
                    original_channels=original_channels
                )
            except PlivoAudioStreamError:
                # Re-raise resampling errors
                raise
            except Exception as e:
                raise PlivoAudioStreamError(f"Failed to resample audio: {str(e)}")

        # Split audio data into chunks
        total_chunks = (len(processed_audio_data) + self._chunk_size - 1) // self._chunk_size
        
        for chunk_index in range(total_chunks):
            start_pos = chunk_index * self._chunk_size
            end_pos = min(start_pos + self._chunk_size, len(processed_audio_data))
            chunk = processed_audio_data[start_pos:end_pos]
            
            # encode the chunk to base64
            chunk_base64 = base64.b64encode(chunk).decode('utf-8')
            
            message = {
                "event": "playAudio",
                "media": {
                    "payload": chunk_base64,
                    "sampleRate": final_sample_rate,
                    "contentType": final_content_type,
                },
            }

            try:
                message_json = json.dumps(message)
                await self._send_message(message_json)
            except Exception as e:
                raise PlivoAudioStreamError(f"Failed to send audio chunk {chunk_index}: {str(e)}")

    async def start_listening(self) -> None:
        """
        Start listening for WebSocket messages.

        This method will continuously listen for incoming WebSocket messages
        and dispatch them to the appropriate event handlers based on the
        event type.

        Note: This is an async method that will run until the WebSocket is closed
        or an error occurs. Use asyncio.create_task() to run it in the background.

        Raises:
            PlivoAudioStreamError: If WebSocket is not connected or other errors
        """
        if not self._websocket:
            raise PlivoAudioStreamError("WebSocket connection not available")

        self._is_listening = True

        try:
            # Check if this is a websockets library WebSocket (has __aiter__)
            # or a FastAPI WebSocket (has receive_text method)
            if hasattr(self._websocket, "__aiter__"):
                # websockets library WebSocket - use async for
                async for message in self._websocket:
                    if not self._is_listening:
                        break
                    await self._process_message(message)

            elif hasattr(self._websocket, "receive_text"):
                # FastAPI WebSocket - use receive_text in a loop
                while self._is_listening:
                    try:
                        message = await self._websocket.receive_text()
                        await self._process_message(message)
                    except Exception as e:
                        # Handle WebSocket disconnect or other errors
                        if (
                            "websocket.disconnect" in str(e).lower()
                            or "disconnect" in str(e).lower()
                        ):
                            break
                        else:
                            print(
                                f"Error receiving message from FastAPI WebSocket: {str(e)}"
                            )
                            break

            elif hasattr(self._websocket, "receive"):
                # Generic WebSocket with receive method
                while self._is_listening:
                    try:
                        message = await self._websocket.receive()
                        # Handle different message formats
                        if isinstance(message, dict):
                            if message.get("type") == "websocket.receive":
                                text_message = message.get("text", "")
                                if text_message:
                                    await self._process_message(text_message)
                        elif isinstance(message, str):
                            await self._process_message(message)
                    except Exception as e:
                        print(
                            f"Error receiving message from generic WebSocket: {str(e)}"
                        )
                        break

            else:
                # Unknown WebSocket type
                available_methods = [
                    method
                    for method in dir(self._websocket)
                    if not method.startswith("_")
                    and callable(getattr(self._websocket, method))
                ]
                raise PlivoAudioStreamError(
                    f"Unknown WebSocket type. Available methods: {available_methods}. "
                    f"Expected WebSocket with __aiter__, receive_text, or receive methods."
                )

        except Exception as e:
            # Critical error, stop listening
            self._is_listening = False
            raise PlivoAudioStreamError(f"Async message listener failed: {str(e)}")
        finally:
            self._is_listening = False

    async def _process_message(self, message: str) -> None:
        """
        Process a received WebSocket message.

        Args:
            message: The message string to process
        """
        try:
            # Parse JSON message
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                # Log or handle invalid JSON, but don't crash
                return

            # Dispatch to appropriate handler based on event type
            event_type = data.get("event")
            handler = self._event_handlers.get(event_type)

            if handler:
                if asyncio.iscoroutinefunction(handler):
                    await handler(data)
                else:
                    # Handle sync handlers too
                    handler(data)

        except Exception as e:
            # Log error but don't crash the listener
            print(f"Error processing message: {str(e)}")

    def stop_listening(self) -> None:
        """
        Stop listening for WebSocket messages.

        This method will stop the message listener.
        """
        self._is_listening = False

    async def _send_message(self, message: str) -> None:
        """
        Send a message to the async WebSocket, handling different implementations.

        Args:
            message: The message string to send

        Raises:
            PlivoAudioStreamError: If no compatible send method is found
        """
        # Try different async WebSocket send methods

        # Method 2: Some implementations use 'send_text'
        if hasattr(self._websocket, "send_text"):
            await self._websocket.send_text(message)

        # Method 1: Most common - websockets library and others
        elif hasattr(self._websocket, "send"):
            await self._websocket.send(message)


        # Method 3: Some implementations use 'write'
        elif hasattr(self._websocket, "write"):
            await self._websocket.write(message)

        # Method 4: No compatible send method found
        else:
            available_methods = [
                method
                for method in dir(self._websocket)
                if not method.startswith("_")
                and callable(getattr(self._websocket, method))
            ]
            raise PlivoAudioStreamError(
                f"Async WebSocket object doesn't have a compatible send method. "
                f"Available methods: {available_methods}. "
                f"Please ensure you're using a compatible async WebSocket implementation."
            )

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

    async def close(self) -> None:
        """
        Close the WebSocket connection and stop listening.

        This method cleanly shuts down the async streaming client.
        """
        self.stop_listening()
        if self._websocket:
            try:
                if hasattr(self._websocket, "close"):
                    await self._websocket.close()
                elif hasattr(self._websocket, "aclose"):
                    await self._websocket.aclose()
            except Exception:
                # Ignore errors when closing
                pass

    async def __aenter__(self):
        """Async context manager entry"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
