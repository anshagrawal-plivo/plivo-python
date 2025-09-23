# Plivo Audio Streaming WebSocket Client

This document describes the new WebSocket-based audio streaming abstraction layer for the Plivo Python SDK.

## Overview

The `PlivoAudioStreamClient` provides a simple, event-driven interface for handling real-time audio streaming over WebSocket connections. It abstracts the complexity of WebSocket management and provides clean event handlers for different streaming events.

## Installation

The streaming functionality requires an additional dependency:

```bash
pip install websocket-client
```

## Quick Start

```python
import websocket
from plivo import PlivoAudioStreamClient

# Create WebSocket connection
ws = websocket.create_connection("wss://your-streaming-endpoint.example.com/audio")

# Create streaming client
stream_client = PlivoAudioStreamClient(ws)

# Set up event handlers
@stream_client.onAudio
def handle_audio(data):
    print(f"Received audio: {len(data['media']['payload'])} bytes")

@stream_client.onStart
def handle_start(data):
    print("Stream started:", data)

@stream_client.onEnd
def handle_end(data):
    print("Stream ended:", data)

# Start listening
stream_client.start_listening()

# Send audio data
import base64
audio_data = base64.b64encode(b"your_audio_data").decode('utf-8')
stream_client.playAudio(audio_data)
```

## API Reference

### PlivoAudioStreamClient

Main class for handling WebSocket audio streaming.

#### Constructor

```python
PlivoAudioStreamClient(websocket_connection)
```

**Parameters:**

- `websocket_connection`: An active WebSocket connection object

**Raises:**

- `PlivoRestError`: If websocket-client library is not installed
- `InvalidRequestError`: If websocket_connection is None

#### Event Handlers

##### onAudio(handler)

Register a handler for audio data events (when `data.event === 'media'`).

```python
@stream_client.onAudio
def handle_audio(data):
    media = data['media']
    payload = media['payload']  # Base64 encoded audio
    # Process audio data
```

##### onStart(handler)

Register a handler for stream start events (when `data.event === 'start'`).

```python
@stream_client.onStart
def handle_start(data):
    print("Stream started with data:", data)
```

##### onEnd(handler)

Register a handler for stream end events (when `data.event === 'end'`).

```python
@stream_client.onEnd
def handle_end(data):
    print("Stream ended with data:", data)
```

#### Methods

##### playAudio(audio_data, sample_rate=24000, content_type="audio/x-l16")

Send base64-encoded audio data to the WebSocket.

**Parameters:**

- `audio_data` (str): Base64 encoded audio data
- `sample_rate` (int): Audio sample rate (default: 24000)
- `content_type` (str): Audio content type (default: "audio/x-l16")

**Message Format:**

```json
{
  "event": "playAudio",
  "media": {
    "payload": "<base64_audio_data>",
    "sampleRate": 24000,
    "contentType": "audio/x-l16"
  }
}
```

##### start_listening()

Start listening for WebSocket messages in a background thread.

##### stop_listening()

Stop the message listener thread.

##### is_listening()

Returns `True` if currently listening for messages.

##### close()

Close the WebSocket connection and stop listening.

#### Context Manager Support

The client supports context manager usage for automatic cleanup:

```python
with PlivoAudioStreamClient(ws) as stream_client:
    # Set up handlers and use the client
    pass
# Automatically closed when exiting the context
```

## Examples

See `examples/audio_streaming.py` for complete usage examples including:

- Basic usage with event handlers
- Context manager usage
- File streaming example
- Error handling patterns

## Error Handling

The streaming client uses the following exception hierarchy:

- `PlivoRestError`: Base exception for all Plivo errors
- `PlivoAudioStreamError`: Specific to audio streaming errors
- `InvalidRequestError`: For invalid parameters or requests

## Threading

The client uses a background thread for listening to WebSocket messages. All event handlers are called from this background thread, so ensure thread-safety in your handlers if they interact with shared data.

## Limitations

- Requires `websocket-client` library
- Event handlers run in a background thread
- WebSocket connection management is left to the user
- No automatic reconnection (implement in your application if needed)

## Best Practices

1. **Always close the client** when done to free resources
2. **Use context managers** when possible for automatic cleanup
3. **Handle exceptions** in your event handlers to prevent crashes
4. **Keep handlers lightweight** to avoid blocking the message loop
5. **Implement reconnection logic** in your application for production use
