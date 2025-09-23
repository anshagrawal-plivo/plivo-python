# Plivo Audio Streaming WebSocket Client

This document describes the new WebSocket-based audio streaming abstraction layer for the Plivo Python SDK.

## Overview

The Plivo SDK provides two WebSocket audio streaming clients:

- **`PlivoAudioStreamClient`** - Synchronous client for use with `websocket-client` library
- **`PlivoAsyncAudioStreamClient`** - Asynchronous client for use with `websockets` library and asyncio

Both provide a simple, event-driven interface for handling real-time audio streaming over WebSocket connections. They abstract the complexity of WebSocket management and provide clean event handlers for different streaming events.

## Installation

The streaming functionality requires an additional dependency depending on your use case:

### For Synchronous Usage (PlivoAudioStreamClient)

```bash
pip install websocket-client
```

### For Asynchronous Usage (PlivoAsyncAudioStreamClient)

```bash
pip install websockets
```

You can install both if you want to use both sync and async clients:

```bash
pip install websocket-client websockets
```

## Quick Start

### Synchronous Client

```python
import websocket  # pip install websocket-client
from plivo import PlivoAudioStreamClient

# Create WebSocket connection using websocket-client library
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

### Asynchronous Client

```python
import asyncio
import websockets  # pip install websockets
from plivo import PlivoAsyncAudioStreamClient

async def main():
    # Create WebSocket connection using websockets library
    async with websockets.connect("wss://your-streaming-endpoint.example.com/audio") as ws:

        # Create async streaming client
        stream_client = PlivoAsyncAudioStreamClient(ws)

        # Set up async event handlers
        @stream_client.onAudio
        async def handle_audio(data):
            print(f"Received audio: {len(data['media']['payload'])} bytes")

        @stream_client.onStart
        async def handle_start(data):
            print("Stream started:", data)

        @stream_client.onEnd
        async def handle_end(data):
            print("Stream ended:", data)

        # Start listening (runs in background)
        listen_task = asyncio.create_task(stream_client.start_listening())

        # Send audio data
        import base64
        audio_data = base64.b64encode(b"your_audio_data").decode('utf-8')
        await stream_client.playAudio(audio_data)

        # Wait for events or cancel listening
        try:
            await asyncio.wait_for(listen_task, timeout=10.0)
        except asyncio.TimeoutError:
            listen_task.cancel()

# Run the async function
asyncio.run(main())
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

### PlivoAsyncAudioStreamClient

Asynchronous class for handling WebSocket audio streaming with asyncio.

#### Constructor

```python
PlivoAsyncAudioStreamClient(websocket_connection)
```

**Parameters:**

- `websocket_connection`: An active async WebSocket connection object (e.g., from `websockets.connect()`)

**Raises:**

- `InvalidRequestError`: If websocket_connection is None

#### Event Handlers

##### onAudio(handler)

Register an async handler for audio data events (when `data.event === 'media'`).

```python
@stream_client.onAudio
async def handle_audio(data):
    media = data['media']
    payload = media['payload']  # Base64 encoded audio
    # Process audio data asynchronously
```

##### onStart(handler)

Register an async handler for stream start events (when `data.event === 'start'`).

```python
@stream_client.onStart
async def handle_start(data):
    print("Stream started with data:", data)
```

##### onEnd(handler)

Register an async handler for stream end events (when `data.event === 'end'`).

```python
@stream_client.onEnd
async def handle_end(data):
    print("Stream ended with data:", data)
```

#### Methods

##### async playAudio(audio_data, sample_rate=24000, content_type="audio/x-l16")

Send base64-encoded audio data to the WebSocket asynchronously.

**Parameters:**

- `audio_data` (str): Base64 encoded audio data
- `sample_rate` (int): Audio sample rate (default: 24000)
- `content_type` (str): Audio content type (default: "audio/x-l16")

**Message Format:** Same JSON structure as sync client

##### async start_listening()

Start listening for WebSocket messages. This method runs until the WebSocket is closed.

**Usage:**

```python
# Run in background
listen_task = asyncio.create_task(stream_client.start_listening())

# Later, cancel if needed
listen_task.cancel()
```

##### stop_listening()

Stop the message listener.

##### is_listening()

Returns `True` if currently listening for messages.

##### async close()

Close the WebSocket connection and stop listening.

#### Async Context Manager Support

The async client supports async context manager usage:

```python
async with websockets.connect("wss://endpoint") as ws:
    async with PlivoAsyncAudioStreamClient(ws) as stream_client:
        # Set up handlers and use the client
        pass
    # Automatically closed when exiting the context
```

## Examples

See `examples/audio_streaming.py` for complete usage examples including:

- Basic synchronous usage with event handlers
- Asynchronous usage with asyncio
- Context manager usage for both sync and async
- File streaming examples
- Error handling patterns

## Error Handling

The streaming client uses the following exception hierarchy:

- `PlivoRestError`: Base exception for all Plivo errors
- `PlivoAudioStreamError`: Specific to audio streaming errors
- `InvalidRequestError`: For invalid parameters or requests

## Threading

The client uses a background thread for listening to WebSocket messages. All event handlers are called from this background thread, so ensure thread-safety in your handlers if they interact with shared data.

## Limitations

- Requires `websocket-client` library (not `websockets`)
- Only works with synchronous WebSocket implementations
- Event handlers run in a background thread
- WebSocket connection management is left to the user
- No automatic reconnection (implement in your application if needed)

## Troubleshooting

### "WebSocket object has no attribute 'recv'" Error

This error means you're using an incompatible WebSocket implementation with the synchronous client. The most common causes:

1. **Wrong library for sync client**: You might be using the `websockets` library with `PlivoAudioStreamClient`
2. **Wrong client for library**: You might be using an asyncio WebSocket with the sync client

**Solutions:**

#### For Synchronous Usage:

```bash
# Install websocket-client library
pip install websocket-client
```

```python
import websocket  # websocket-client library
from plivo import PlivoAudioStreamClient

ws = websocket.create_connection("wss://your-endpoint")
client = PlivoAudioStreamClient(ws)
```

#### For Asynchronous Usage:

```bash
# Install websockets library
pip install websockets
```

```python
import asyncio
import websockets  # websockets library
from plivo import PlivoAsyncAudioStreamClient

async def main():
    async with websockets.connect("wss://your-endpoint") as ws:
        client = PlivoAsyncAudioStreamClient(ws)
        # ... rest of async code

asyncio.run(main())
```

### Choosing the Right Client

**Use `PlivoAudioStreamClient` when:**

- Working with synchronous code
- Using `websocket-client` library
- Building simple scripts or traditional applications

**Use `PlivoAsyncAudioStreamClient` when:**

- Working with asyncio-based applications
- Using `websockets` library
- Building modern async applications or need to handle multiple streams concurrently

## Best Practices

1. **Always close the client** when done to free resources
2. **Use context managers** when possible for automatic cleanup
3. **Handle exceptions** in your event handlers to prevent crashes
4. **Keep handlers lightweight** to avoid blocking the message loop
5. **Implement reconnection logic** in your application for production use
