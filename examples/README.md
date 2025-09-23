# Plivo Python SDK Examples

This directory contains example code demonstrating various features of the Plivo Python SDK.

## Available Examples

### Audio Streaming Examples

- **`audio_streaming.py`** - Comprehensive examples of Plivo audio streaming with both synchronous and asynchronous WebSocket clients
- **`fastapi_streaming.py`** - FastAPI web application demonstrating real-time audio streaming with Plivo integration

### Authentication Examples

- **`JWT.py`** - JSON Web Token (JWT) authentication examples

## Running the Examples

### Basic Audio Streaming

```bash
# Install websocket dependencies
pip install websocket-client websockets

# Run the basic audio streaming examples
python audio_streaming.py
```

### FastAPI Audio Streaming

```bash
# Install FastAPI dependencies
pip install -r requirements-fastapi.txt

# Run the FastAPI server
python fastapi_streaming.py

# Then visit http://localhost:8000 for the web interface
```

### JWT Authentication

```bash
python JWT.py
```

## Features Demonstrated

### Audio Streaming (`audio_streaming.py`)

- **Synchronous WebSocket client** using `websocket-client` library
- **Asynchronous WebSocket client** using `websockets` library
- Event-driven audio handling with `@onAudio`, `@onStart`, `@onEnd` decorators
- Context manager usage for automatic cleanup
- File-based audio streaming
- Error handling and connection management

### FastAPI Integration (`fastapi_streaming.py`)

- **Real-time WebSocket endpoints** for bidirectional audio streaming
- **Actual Plivo abstraction layer integration** using `PlivoAsyncAudioStreamClient`
- **Real event handlers** with `@onAudio`, `@onStart`, `@onEnd` decorators
- **Abstraction layer methods** like `.playAudio()` for sending audio to Plivo
- **Web-based interface** for testing audio streaming
- **REST API endpoints** for programmatic access
- **Connection management** with proper lifecycle cleanup using `.close()`
- **Multiple client patterns** showing different integration approaches
- **Production-ready patterns** with proper error handling

### JWT Authentication (`JWT.py`)

- Token generation and validation
- Secure API authentication

## Dependencies

### Core Dependencies (already in main requirements.txt)

- `plivo` - Main Plivo SDK
- `requests` - HTTP client
- `six` - Python 2/3 compatibility

### Audio Streaming Dependencies

- `websocket-client` - For synchronous WebSocket connections
- `websockets` - For asynchronous WebSocket connections

### FastAPI Dependencies

- `fastapi` - Modern web framework
- `uvicorn` - ASGI server

## Usage Patterns

### Basic Audio Streaming

```python
import websocket
from plivo import PlivoAudioStreamClient

# Create WebSocket connection
ws = websocket.create_connection("wss://your-endpoint")

# Create streaming client
client = PlivoAudioStreamClient(ws)

# Set up event handlers
@client.onAudio
def handle_audio(data):
    print(f"Received audio: {len(data['media']['payload'])} bytes")

# Start listening and send audio
client.start_listening()
client.playAudio(base64_audio_data)
```

### Async Audio Streaming

```python
import asyncio
import websockets
from plivo import PlivoAsyncAudioStreamClient

async def main():
    async with websockets.connect("wss://your-endpoint") as ws:
        client = PlivoAsyncAudioStreamClient(ws)

        @client.onAudio
        async def handle_audio(data):
            print(f"Received audio: {len(data['media']['payload'])} bytes")

        listen_task = asyncio.create_task(client.start_listening())
        await client.playAudio(base64_audio_data)

asyncio.run(main())
```

### FastAPI Integration with Abstraction Layer

```python
from fastapi import FastAPI, WebSocket
from plivo import PlivoAsyncAudioStreamClient
import websockets

app = FastAPI()

@app.websocket("/ws/audio")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    # Connect to Plivo using abstraction layer
    plivo_ws = await websockets.connect("wss://your-plivo-endpoint")
    plivo_client = PlivoAsyncAudioStreamClient(plivo_ws)

    # Set up event handlers
    @plivo_client.onAudio
    async def handle_audio(data):
        # Forward audio from Plivo to FastAPI client
        await websocket.send_text(json.dumps({
            "type": "plivo_audio",
            "data": data
        }))

    @plivo_client.onStart
    async def handle_start(data):
        await websocket.send_text(json.dumps({
            "type": "stream_started"
        }))

    # Start listening to Plivo
    asyncio.create_task(plivo_client.start_listening())

    # Handle FastAPI client messages
    async for message in websocket.iter_text():
        data = json.loads(message)
        if data["type"] == "audio":
            # Send to Plivo using abstraction layer
            await plivo_client.playAudio(data["audio_data"])
```

## Troubleshooting

### WebSocket Library Issues

If you encounter WebSocket-related errors:

1. **For sync usage**: Install `websocket-client`

   ```bash
   pip install websocket-client
   ```

2. **For async usage**: Install `websockets`

   ```bash
   pip install websockets
   ```

3. **Wrong client for library**: Use `PlivoAudioStreamClient` with `websocket-client` and `PlivoAsyncAudioStreamClient` with `websockets`

### FastAPI Issues

1. **Missing dependencies**: Install FastAPI requirements

   ```bash
   pip install -r requirements-fastapi.txt
   ```

2. **Port conflicts**: Change the port in `uvicorn.run()` call

### Audio Format Issues

- Ensure audio data is **base64 encoded**
- Default format is `audio/x-l16` at `24000 Hz`
- Verify audio data before sending to avoid errors

## Contributing

When adding new examples:

1. Follow the existing code style and patterns
2. Include comprehensive error handling
3. Add documentation comments
4. Update this README with example descriptions
5. Include any additional dependencies in requirements files
