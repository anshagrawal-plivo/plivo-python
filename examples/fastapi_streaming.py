#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
FastAPI Audio Streaming Example with Plivo

This example demonstrates how to integrate Plivo's audio streaming capabilities
with FastAPI web applications. It shows both synchronous and asynchronous
approaches for handling real-time audio streaming.

Features:
- WebSocket endpoints for bidirectional audio streaming
- Integration with Plivo audio streaming clients
- Real-time audio relay between clients and Plivo
- Connection management and error handling
- Both sync and async implementation examples

Requirements:
    pip install fastapi uvicorn websockets websocket-client

Usage:
    python fastapi_streaming.py

    Then visit:
    - http://localhost:8000 for the web interface
    - ws://localhost:8000/ws/audio-stream for WebSocket connection
    - ws://localhost:8000/ws/plivo-bridge for Plivo bridge connection
"""

import asyncio
import base64
import json
import logging
import threading
import time
from typing import Dict, List, Optional

import websocket
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse
import uvicorn

# Import Plivo streaming clients
from plivo import PlivoAsyncAudioStreamClient, PlivoAudioStreamClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Plivo Audio Streaming with FastAPI", version="1.0.0")


# =====================================
# Connection Management Classes
# =====================================


class ConnectionManager:
    """Manages WebSocket connections for audio streaming"""

    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.client_connections: Dict[str, WebSocket] = {}

    async def connect(self, websocket: WebSocket, client_id: str = None):
        """Accept a new WebSocket connection"""
        await websocket.accept()
        self.active_connections.append(websocket)
        if client_id:
            self.client_connections[client_id] = websocket
        logger.info(
            f"New connection established. Total: {len(self.active_connections)}"
        )

    def disconnect(self, websocket: WebSocket, client_id: str = None):
        """Remove a WebSocket connection"""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        if client_id and client_id in self.client_connections:
            del self.client_connections[client_id]
        logger.info(f"Connection closed. Total: {len(self.active_connections)}")

    async def send_personal_message(self, message: str, websocket: WebSocket):
        """Send a message to a specific WebSocket"""
        try:
            await websocket.send_text(message)
        except Exception as e:
            logger.error(f"Error sending message: {e}")

    async def broadcast(self, message: str):
        """Broadcast a message to all connected clients"""
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                logger.error(f"Error broadcasting to connection: {e}")
                disconnected.append(connection)

        # Clean up disconnected clients
        for conn in disconnected:
            self.active_connections.remove(conn)


class PlivoBridge:
    """Bridges FastAPI WebSocket with Plivo streaming using actual abstraction layer"""

    def __init__(self):
        self.plivo_ws_url = None
        self.plivo_ws = None
        self.plivo_client = None
        self.is_connected = False
        self.connection_manager = ConnectionManager()
        self.client_websockets = {}  # Track client WebSockets

    async def connect_to_plivo(
        self, plivo_ws_url: str, client_websocket: WebSocket
    ) -> bool:
        """Connect to actual Plivo WebSocket endpoint using abstraction layer"""
        try:
            # Import websockets for async connection
            import websockets

            self.plivo_ws_url = plivo_ws_url

            # Create actual WebSocket connection to Plivo
            self.plivo_ws = await websockets.connect(plivo_ws_url)
            logger.info(f"Established WebSocket connection to: {plivo_ws_url}")

            # Create Plivo async streaming client
            self.plivo_client = PlivoAsyncAudioStreamClient(self.plivo_ws)
            logger.info("Created PlivoAsyncAudioStreamClient")

            # Set up event handlers using the abstraction layer
            await self.setup_plivo_handlers(client_websocket)

            # Start listening for Plivo messages
            asyncio.create_task(self.plivo_client.start_listening())
            logger.info("Started listening for Plivo messages")

            self.is_connected = True
            logger.info(f"Successfully connected to Plivo: {plivo_ws_url}")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to Plivo: {e}")
            self.is_connected = False
            if self.plivo_client:
                await self.plivo_client.close()
            return False

    async def setup_plivo_handlers(self, client_websocket: WebSocket):
        """Set up Plivo event handlers using the abstraction layer decorators"""

        @self.plivo_client.onAudio
        async def handle_plivo_audio(data):
            """Forward audio from Plivo to connected FastAPI clients"""
            try:
                media_data = data.get("media", {})
                payload = media_data.get("payload", "")
                sample_rate = media_data.get("sampleRate", "unknown")

                message = {
                    "type": "audio_from_plivo",
                    "event": "media",
                    "media": {
                        "payload": payload,
                        "sampleRate": sample_rate,
                        "contentType": media_data.get("contentType", "audio/x-l16"),
                    },
                    "size": len(payload),
                    "timestamp": time.time(),
                }

                await self.connection_manager.send_personal_message(
                    json.dumps(message), client_websocket
                )
                logger.info(f"Forwarded audio from Plivo: {len(payload)} bytes")

            except Exception as e:
                logger.error(f"Error forwarding Plivo audio: {e}")

        @self.plivo_client.onStart
        async def handle_plivo_start(data):
            """Handle Plivo stream start using abstraction layer"""
            logger.info("Plivo stream started")
            message = {
                "type": "plivo_stream_start",
                "event": "start",
                "data": data,
                "timestamp": time.time(),
            }
            await self.connection_manager.send_personal_message(
                json.dumps(message), client_websocket
            )

        @self.plivo_client.onEnd
        async def handle_plivo_end(data):
            """Handle Plivo stream end using abstraction layer"""
            logger.info("Plivo stream ended")
            message = {
                "type": "plivo_stream_end",
                "event": "end",
                "data": data,
                "timestamp": time.time(),
            }
            await self.connection_manager.send_personal_message(
                json.dumps(message), client_websocket
            )

    async def send_audio_to_plivo(
        self,
        audio_data: str,
        sample_rate: int = 24000,
        content_type: str = "audio/x-l16",
    ):
        """Send audio data to Plivo using the abstraction layer"""
        if not self.is_connected or not self.plivo_client:
            raise Exception("Not connected to Plivo")

        try:
            # Use the abstraction layer's playAudio method
            await self.plivo_client.playAudio(audio_data, sample_rate, content_type)
            logger.info(
                f"Sent audio to Plivo via abstraction layer: {len(audio_data)} chars"
            )
        except Exception as e:
            logger.error(f"Error sending audio to Plivo: {e}")
            raise

    async def disconnect(self):
        """Disconnect from Plivo using abstraction layer cleanup"""
        if self.plivo_client:
            await self.plivo_client.close()
        self.is_connected = False
        self.plivo_ws = None
        self.plivo_client = None
        logger.info("Disconnected from Plivo")


# Global instances
manager = ConnectionManager()
plivo_bridge = PlivoBridge()


# =====================================
# FastAPI Routes
# =====================================


@app.get("/")
async def get():
    """Serve a simple HTML interface for testing"""
    return HTMLResponse(
        content="""
<!DOCTYPE html>
<html>
<head>
    <title>Plivo Audio Streaming with FastAPI</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .container { max-width: 800px; margin: 0 auto; }
        .section { margin: 20px 0; padding: 20px; border: 1px solid #ddd; border-radius: 5px; }
        button { padding: 10px 20px; margin: 5px; cursor: pointer; }
        #messages { height: 300px; overflow-y: scroll; border: 1px solid #ccc; padding: 10px; background: #f9f9f9; }
        input[type="text"] { width: 300px; padding: 8px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Plivo Audio Streaming with FastAPI</h1>
        
        <div class="section">
            <h3>WebSocket Connection Patterns</h3>
            <h4>Outbound Pattern (FastAPI connects TO Plivo)</h4>
            <button onclick="connect()">Connect to Audio Stream</button>
            <button onclick="disconnect()">Disconnect</button>
            <span id="status">Disconnected</span>
            
            <h4>Incoming Pattern (External service connects TO FastAPI)</h4>
            <button onclick="connectIncoming()">Connect to Incoming Handler</button>
            <button onclick="disconnectIncoming()">Disconnect Incoming</button>
            <span id="incomingStatus">Disconnected</span>
        </div>
        
        <div class="section">
            <h3>Plivo Bridge</h3>
            <input type="text" id="plivoUrl" placeholder="Plivo WebSocket URL" value="wss://example.plivo.com/stream">
            <button onclick="connectPlivo()">Connect to Plivo</button>
        </div>
        
        <div class="section">
            <h3>Audio Controls</h3>
            <button onclick="sendDummyAudio()">Send Dummy Audio</button>
            <button onclick="startRecording()">Start Recording</button>
            <button onclick="stopRecording()">Stop Recording</button>
        </div>
        
        <div class="section">
            <h3>Messages</h3>
            <div id="messages"></div>
        </div>
    </div>

    <script>
        let ws = null;
        let incomingWs = null;
        let isRecording = false;
        
        function log(message) {
            const messages = document.getElementById('messages');
            const timestamp = new Date().toLocaleTimeString();
            messages.innerHTML += `<div>[${timestamp}] ${message}</div>`;
            messages.scrollTop = messages.scrollHeight;
        }
        
        function connect() {
            ws = new WebSocket("ws://localhost:8000/ws/audio-stream");
            
            ws.onopen = function(event) {
                document.getElementById('status').innerText = 'Connected';
                log('Connected to audio stream');
            };
            
            ws.onmessage = function(event) {
                const data = JSON.parse(event.data);
                log(`Received: ${data.type} - ${JSON.stringify(data).substring(0, 100)}...`);
            };
            
            ws.onclose = function(event) {
                document.getElementById('status').innerText = 'Disconnected';
                log('Disconnected from audio stream');
            };
            
            ws.onerror = function(error) {
                log(`WebSocket error: ${error}`);
            };
        }
        
        function disconnect() {
            if (ws) {
                ws.close();
                ws = null;
            }
        }
        
        function connectIncoming() {
            incomingWs = new WebSocket("ws://localhost:8000/ws/incoming-connection");
            
            incomingWs.onopen = function(event) {
                document.getElementById('incomingStatus').innerText = 'Connected';
                log('Connected to incoming handler (simulating external service connecting TO FastAPI)');
            };
            
            incomingWs.onmessage = function(event) {
                const data = JSON.parse(event.data);
                log(`Incoming pattern received: ${data.type} - ${JSON.stringify(data).substring(0, 100)}...`);
            };
            
            incomingWs.onclose = function(event) {
                document.getElementById('incomingStatus').innerText = 'Disconnected';
                log('Disconnected from incoming handler');
            };
            
            incomingWs.onerror = function(error) {
                log(`Incoming WebSocket error: ${error}`);
            };
        }
        
        function disconnectIncoming() {
            if (incomingWs) {
                incomingWs.close();
                incomingWs = null;
            }
        }
        
        function connectPlivo() {
            if (ws) {
                const plivoUrl = document.getElementById('plivoUrl').value;
                const message = {
                    type: 'connect_plivo',
                    plivo_url: plivoUrl
                };
                ws.send(JSON.stringify(message));
                log(`Attempting to connect to Plivo: ${plivoUrl}`);
            } else {
                log('Please connect to audio stream first');
            }
        }
        
        function sendDummyAudio() {
            if (ws) {
                const dummyAudio = btoa('dummy_audio_data_' + Date.now());
                const message = {
                    type: 'audio_data',
                    audio: dummyAudio,
                    sample_rate: 24000,
                    content_type: 'audio/x-l16'
                };
                ws.send(JSON.stringify(message));
                log('Sent dummy audio data');
            } else {
                log('Please connect first');
            }
        }
        
        function startRecording() {
            if (navigator.mediaDevices && navigator.mediaDevices.getUserMedia) {
                navigator.mediaDevices.getUserMedia({ audio: true })
                    .then(function(stream) {
                        log('Started recording from microphone');
                        isRecording = true;
                        // In a real implementation, you'd capture and send audio chunks
                        // This is just a placeholder
                    })
                    .catch(function(err) {
                        log(`Microphone access denied: ${err}`);
                    });
            } else {
                log('Microphone access not supported');
            }
        }
        
        function stopRecording() {
            isRecording = false;
            log('Stopped recording');
        }
    </script>
</body>
</html>
    """
    )


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "connections": len(manager.active_connections),
        "plivo_connected": plivo_bridge.is_connected,
    }


# =====================================
# WebSocket Endpoints
# =====================================


@app.websocket("/ws/audio-stream")
async def websocket_audio_stream(websocket: WebSocket):
    """
    Main WebSocket endpoint for audio streaming
    Handles bidirectional audio between clients and Plivo
    """
    client_id = f"client_{int(time.time() * 1000)}"
    await manager.connect(websocket, client_id)

    try:
        # Send welcome message
        welcome_msg = {
            "type": "connection_established",
            "client_id": client_id,
            "message": "Connected to Plivo audio streaming service",
            "timestamp": time.time(),
        }
        await manager.send_personal_message(json.dumps(welcome_msg), websocket)

        while True:
            # Receive message from client
            data = await websocket.receive_text()
            message = json.loads(data)

            message_type = message.get("type")

            if message_type == "connect_plivo":
                # Client wants to connect to Plivo using abstraction layer
                plivo_url = message.get("plivo_url", "wss://example.plivo.com/stream")
                success = await plivo_bridge.connect_to_plivo(plivo_url, websocket)

                response = {
                    "type": "plivo_connection_status",
                    "success": success,
                    "message": (
                        "Connected to Plivo via abstraction layer"
                        if success
                        else "Failed to connect to Plivo"
                    ),
                    "plivo_url": plivo_url,
                    "client_id": client_id,
                    "timestamp": time.time(),
                }
                await manager.send_personal_message(json.dumps(response), websocket)

            elif message_type == "audio_data":
                # Client sending audio data to be forwarded to Plivo
                audio_data = message.get("audio")
                sample_rate = message.get("sample_rate", 24000)
                content_type = message.get("content_type", "audio/x-l16")

                # Process and forward to Plivo
                await handle_client_audio(
                    audio_data, sample_rate, content_type, websocket
                )

            elif message_type == "ping":
                # Keep-alive ping
                pong_msg = {"type": "pong", "timestamp": time.time()}
                await manager.send_personal_message(json.dumps(pong_msg), websocket)

            else:
                # Unknown message type
                error_msg = {
                    "type": "error",
                    "message": f"Unknown message type: {message_type}",
                    "timestamp": time.time(),
                }
                await manager.send_personal_message(json.dumps(error_msg), websocket)

    except WebSocketDisconnect:
        logger.info(f"Client {client_id} disconnected")
    except Exception as e:
        logger.error(f"Error in audio stream for {client_id}: {e}")
    finally:
        # Clean up connections
        manager.disconnect(websocket, client_id)

        # Disconnect from Plivo if this was the last client
        if len(manager.active_connections) == 0 and plivo_bridge.is_connected:
            try:
                await plivo_bridge.disconnect()
                logger.info("Disconnected from Plivo - no more active clients")
            except Exception as e:
                logger.error(f"Error disconnecting from Plivo: {e}")


@app.websocket("/ws/plivo-bridge/{plivo_endpoint}")
async def websocket_plivo_bridge(websocket: WebSocket, plivo_endpoint: str):
    """
    Direct bridge WebSocket endpoint for Plivo integration
    This endpoint specifically handles Plivo WebSocket connections
    """
    await websocket.accept()

    try:
        # Decode the Plivo endpoint URL
        import urllib.parse

        plivo_url = urllib.parse.unquote(plivo_endpoint)

        logger.info(f"Creating Plivo bridge for: {plivo_url}")

        # Set up the bridge connection
        success = await setup_plivo_bridge(websocket, plivo_url)

        if not success:
            await websocket.send_text(
                json.dumps(
                    {"type": "error", "message": "Failed to establish Plivo bridge"}
                )
            )
            return

        # Keep the connection alive and handle messages
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)

            # Handle different message types from Plivo
            await handle_plivo_message(message, websocket)

    except WebSocketDisconnect:
        logger.info("Plivo bridge connection closed")
    except Exception as e:
        logger.error(f"Error in Plivo bridge: {e}")


@app.websocket("/ws/plivo-incoming")
async def websocket_plivo_incoming(websocket: WebSocket):
    """
    Handle incoming WebSocket connections from Plivo (or other services)

    This endpoint demonstrates the pattern where:
    1. Plivo makes a WebSocket connection TO FastAPI
    2. FastAPI receives the upgraded WebSocket connection
    3. FastAPI uses the received WebSocket directly with the abstraction layer

    This is different from the outbound pattern where FastAPI connects TO Plivo.
    """
    # Accept the incoming WebSocket connection upgrade
    await websocket.accept()

    logger.info("Received incoming WebSocket connection (could be from Plivo)")

    try:
        # Use the incoming WebSocket connection directly with the abstraction layer
        # No need to create a new connection - we use the one that was upgraded
        plivo_client = PlivoAsyncAudioStreamClient(websocket)

        logger.info("Created PlivoAsyncAudioStreamClient with incoming WebSocket")

        # Set up event handlers for the incoming connection
        @plivo_client.onAudio
        async def handle_incoming_audio(data):
            """Handle audio from the incoming WebSocket connection"""
            try:
                media_data = data.get("media", {})
                payload = media_data.get("payload", "")

                logger.info(
                    f"Received audio from incoming connection: {len(payload)} bytes"
                )

                # Broadcast to all connected FastAPI clients
                message = {
                    "type": "incoming_plivo_audio",
                    "event": "media",
                    "media": media_data,
                    "size": len(payload),
                    "timestamp": time.time(),
                }

                await manager.broadcast(json.dumps(message))

            except Exception as e:
                logger.error(f"Error handling incoming audio: {e}")

        @plivo_client.onStart
        async def handle_incoming_start(data):
            """Handle stream start from incoming connection"""
            logger.info("Incoming stream started")
            message = {
                "type": "incoming_stream_start",
                "event": "start",
                "data": data,
                "timestamp": time.time(),
            }
            await manager.broadcast(json.dumps(message))

        @plivo_client.onEnd
        async def handle_incoming_end(data):
            """Handle stream end from incoming connection"""
            logger.info("Incoming stream ended")
            message = {
                "type": "incoming_stream_end",
                "event": "end",
                "data": data,
                "timestamp": time.time(),
            }
            await manager.broadcast(json.dumps(message))

        # Start listening to the incoming connection
        # This will process messages from the incoming WebSocket
        await plivo_client.start_listening()

        logger.info("Started listening to incoming WebSocket connection")

    except WebSocketDisconnect:
        logger.info("Incoming WebSocket connection closed")
    except Exception as e:
        logger.error(f"Error handling incoming WebSocket: {e}")
    finally:
        # Clean up the client
        if "plivo_client" in locals():
            await plivo_client.close()
        logger.info("Cleaned up incoming WebSocket connection")


@app.websocket("/ws/incoming-connection")
async def websocket_incoming_connection(websocket: WebSocket):
    """
    Endpoint that demonstrates handling incoming WebSocket connections
    using the IncomingWebSocketHandler pattern.

    This shows how to:
    1. Accept an incoming WebSocket upgrade
    2. Use the upgraded connection directly with abstraction layer
    3. Process events and forward them appropriately
    """
    await websocket.accept()

    # Create handler instance for this connection (in production, you'd use a global one)
    handler = IncomingWebSocketHandler()

    try:
        # Handle the incoming connection using our abstraction layer
        await handler.handle_incoming_connection(websocket)

    except WebSocketDisconnect:
        logger.info("Incoming connection handler: client disconnected")
    except Exception as e:
        logger.error(f"Error in incoming connection handler: {e}")


# =====================================
# Audio Processing Functions
# =====================================


async def handle_client_audio(
    audio_data: str, sample_rate: int, content_type: str, client_ws: WebSocket
):
    """
    Process audio data from client and forward to Plivo
    """
    try:
        # Validate audio data
        if not audio_data:
            await client_ws.send_text(
                json.dumps({"type": "error", "message": "No audio data provided"})
            )
            return

        # Decode base64 audio to verify it's valid
        try:
            decoded_audio = base64.b64decode(audio_data)
            logger.info(f"Received audio data: {len(decoded_audio)} bytes")
        except Exception as e:
            await client_ws.send_text(
                json.dumps(
                    {"type": "error", "message": f"Invalid base64 audio data: {e}"}
                )
            )
            return

        # Forward to Plivo using the abstraction layer
        if plivo_bridge.is_connected:
            try:
                # Use the abstraction layer to send audio to Plivo
                await plivo_bridge.send_audio_to_plivo(
                    audio_data, sample_rate, content_type
                )
                logger.info(
                    f"Forwarded {len(decoded_audio)} bytes to Plivo via abstraction layer"
                )

                # Send confirmation back to client
                response = {
                    "type": "audio_forwarded_to_plivo",
                    "size": len(decoded_audio),
                    "sample_rate": sample_rate,
                    "content_type": content_type,
                    "method": "PlivoAsyncAudioStreamClient.playAudio",
                    "timestamp": time.time(),
                }
                await client_ws.send_text(json.dumps(response))
            except Exception as e:
                logger.error(f"Failed to send audio to Plivo: {e}")
                await client_ws.send_text(
                    json.dumps(
                        {
                            "type": "error",
                            "message": f"Failed to send audio to Plivo: {e}",
                        }
                    )
                )
        else:
            await client_ws.send_text(
                json.dumps({"type": "error", "message": "Not connected to Plivo"})
            )

    except Exception as e:
        logger.error(f"Error handling client audio: {e}")
        await client_ws.send_text(
            json.dumps({"type": "error", "message": f"Error processing audio: {e}"})
        )


async def setup_plivo_bridge(client_ws: WebSocket, plivo_url: str) -> bool:
    """
    Set up a bridge between FastAPI WebSocket and Plivo streaming
    """
    try:
        # In a real implementation, you would:
        # 1. Connect to the actual Plivo WebSocket endpoint
        # 2. Create a PlivoAsyncAudioStreamClient
        # 3. Set up event handlers
        # 4. Start listening

        # For demonstration, we'll simulate this:
        logger.info(f"Setting up Plivo bridge to: {plivo_url}")

        # Simulate successful connection
        await client_ws.send_text(
            json.dumps(
                {
                    "type": "plivo_bridge_established",
                    "plivo_url": plivo_url,
                    "timestamp": time.time(),
                }
            )
        )

        return True

    except Exception as e:
        logger.error(f"Failed to setup Plivo bridge: {e}")
        return False


async def handle_plivo_message(message: dict, websocket: WebSocket):
    """
    Handle messages received from Plivo through the bridge
    """
    try:
        message_type = message.get("type")

        if message_type == "audio_to_plivo":
            # Audio data to be sent to Plivo
            audio_data = message.get("audio_data")
            # Forward to Plivo using PlivoAsyncAudioStreamClient
            logger.info("Forwarding audio to Plivo")

        elif message_type == "get_status":
            # Status request
            status = {
                "type": "status_response",
                "plivo_connected": plivo_bridge.is_connected,
                "active_connections": len(manager.active_connections),
                "timestamp": time.time(),
            }
            await websocket.send_text(json.dumps(status))

        # Echo the message back for demonstration
        response = {
            "type": "message_processed",
            "original_message": message,
            "timestamp": time.time(),
        }
        await websocket.send_text(json.dumps(response))

    except Exception as e:
        logger.error(f"Error handling Plivo message: {e}")


# =====================================
# Real Plivo Integration Example
# =====================================


class IncomingWebSocketHandler:
    """
    Handler for incoming WebSocket connections that get upgraded by FastAPI

    This demonstrates the pattern where:
    1. External service (Plivo) connects TO FastAPI
    2. FastAPI receives the HTTP request and upgrades to WebSocket
    3. FastAPI uses the upgraded WebSocket directly with abstraction layer

    No need to create outbound connections - we work with the incoming connection.
    """

    def __init__(self):
        self.active_clients = {}
        self.connection_manager = ConnectionManager()

    async def handle_incoming_connection(
        self, websocket: WebSocket, client_id: str = None
    ):
        """
        Handle an incoming WebSocket connection using the abstraction layer

        Args:
            websocket: The WebSocket connection that was upgraded by FastAPI
            client_id: Optional identifier for this connection
        """
        if not client_id:
            client_id = f"incoming_{int(time.time() * 1000)}"

        logger.info(f"Handling incoming WebSocket connection: {client_id}")

        try:
            # Use the incoming WebSocket directly with abstraction layer
            plivo_client = PlivoAsyncAudioStreamClient(websocket)
            self.active_clients[client_id] = plivo_client

            # Set up event handlers using abstraction layer decorators
            @plivo_client.onAudio
            async def handle_audio(data):
                """Process audio from incoming connection"""
                media_data = data.get("media", {})
                payload = media_data.get("payload", "")

                logger.info(f"Processing audio from {client_id}: {len(payload)} bytes")

                # Forward to other systems or broadcast to web clients
                await self.process_incoming_audio(client_id, data)

            @plivo_client.onStart
            async def handle_start(data):
                """Handle stream start from incoming connection"""
                logger.info(f"Stream started for incoming client {client_id}")
                await self.process_stream_event(client_id, "start", data)

            @plivo_client.onEnd
            async def handle_end(data):
                """Handle stream end from incoming connection"""
                logger.info(f"Stream ended for incoming client {client_id}")
                await self.process_stream_event(client_id, "end", data)

            # Start listening to the incoming connection
            await plivo_client.start_listening()

        except Exception as e:
            logger.error(f"Error handling incoming connection {client_id}: {e}")
        finally:
            # Cleanup
            if client_id in self.active_clients:
                await self.active_clients[client_id].close()
                del self.active_clients[client_id]
            logger.info(f"Cleaned up incoming connection {client_id}")

    async def process_incoming_audio(self, client_id: str, data: dict):
        """Process audio received from incoming connection"""
        # Example: broadcast to web clients, save to file, forward to other services, etc.
        message = {
            "type": "processed_incoming_audio",
            "source_client": client_id,
            "data": data,
            "timestamp": time.time(),
        }
        await self.connection_manager.broadcast(json.dumps(message))

    async def process_stream_event(self, client_id: str, event_type: str, data: dict):
        """Process stream events from incoming connections"""
        message = {
            "type": f"processed_stream_{event_type}",
            "source_client": client_id,
            "data": data,
            "timestamp": time.time(),
        }
        await self.connection_manager.broadcast(json.dumps(message))

    async def send_to_client(
        self,
        client_id: str,
        audio_data: str,
        sample_rate: int = 24000,
        content_type: str = "audio/x-l16",
    ):
        """Send audio to a specific incoming client using abstraction layer"""
        if client_id not in self.active_clients:
            raise Exception(f"Client {client_id} not found")

        try:
            # Use abstraction layer to send audio back to the incoming connection
            await self.active_clients[client_id].playAudio(
                audio_data, sample_rate, content_type
            )
            logger.info(
                f"Sent audio to incoming client {client_id}: {len(audio_data)} chars"
            )
        except Exception as e:
            logger.error(f"Error sending audio to {client_id}: {e}")
            raise


class MultiClientPlivoBridge:
    """
    Advanced example showing how to manage multiple FastAPI clients with one Plivo connection
    """

    def __init__(self):
        self.plivo_client = None
        self.plivo_ws = None
        self.is_connected = False
        self.fastapi_clients = {}  # Track multiple FastAPI WebSocket clients

    async def connect_to_plivo(self, plivo_ws_url: str):
        """
        Connect to Plivo once and handle multiple FastAPI clients
        """
        try:
            import websockets

            # Connect to Plivo WebSocket
            self.plivo_ws = await websockets.connect(plivo_ws_url)

            # Create Plivo async client using abstraction layer
            self.plivo_client = PlivoAsyncAudioStreamClient(self.plivo_ws)

            # Set up event handlers to broadcast to all FastAPI clients
            @self.plivo_client.onAudio
            async def handle_plivo_audio(data):
                """Broadcast audio from Plivo to all connected FastAPI clients"""
                message = {
                    "type": "plivo_audio_broadcast",
                    "data": data,
                    "timestamp": time.time(),
                }
                await self.broadcast_to_clients(json.dumps(message))

            @self.plivo_client.onStart
            async def handle_plivo_start(data):
                """Broadcast stream start to all clients"""
                logger.info("Plivo stream started (multi-client)")
                message = {
                    "type": "plivo_stream_started_broadcast",
                    "data": data,
                    "timestamp": time.time(),
                }
                await self.broadcast_to_clients(json.dumps(message))

            @self.plivo_client.onEnd
            async def handle_plivo_end(data):
                """Broadcast stream end to all clients"""
                logger.info("Plivo stream ended (multi-client)")
                message = {
                    "type": "plivo_stream_ended_broadcast",
                    "data": data,
                    "timestamp": time.time(),
                }
                await self.broadcast_to_clients(json.dumps(message))

            # Start listening using abstraction layer
            asyncio.create_task(self.plivo_client.start_listening())

            self.is_connected = True
            logger.info(f"Multi-client Plivo bridge connected: {plivo_ws_url}")
            return True

        except Exception as e:
            logger.error(f"Failed to connect multi-client bridge: {e}")
            self.is_connected = False
            return False

    async def add_client(self, client_id: str, websocket: WebSocket):
        """Add a FastAPI client to receive Plivo data"""
        self.fastapi_clients[client_id] = websocket
        logger.info(
            f"Added client {client_id} to Plivo bridge. Total: {len(self.fastapi_clients)}"
        )

    async def remove_client(self, client_id: str):
        """Remove a FastAPI client"""
        if client_id in self.fastapi_clients:
            del self.fastapi_clients[client_id]
            logger.info(
                f"Removed client {client_id} from Plivo bridge. Total: {len(self.fastapi_clients)}"
            )

    async def broadcast_to_clients(self, message: str):
        """Broadcast message to all connected FastAPI clients"""
        disconnected_clients = []
        for client_id, websocket in self.fastapi_clients.items():
            try:
                await websocket.send_text(message)
            except Exception as e:
                logger.error(f"Error sending to client {client_id}: {e}")
                disconnected_clients.append(client_id)

        # Clean up disconnected clients
        for client_id in disconnected_clients:
            await self.remove_client(client_id)

    async def send_to_plivo(
        self,
        audio_data: str,
        sample_rate: int = 24000,
        content_type: str = "audio/x-l16",
    ):
        """
        Send audio data to Plivo using abstraction layer
        """
        if not self.is_connected or not self.plivo_client:
            raise Exception("Multi-client bridge not connected to Plivo")

        try:
            # Use abstraction layer to send audio
            await self.plivo_client.playAudio(audio_data, sample_rate, content_type)
            logger.info(
                f"Multi-client bridge sent audio to Plivo: {len(audio_data)} chars"
            )
        except Exception as e:
            logger.error(f"Error sending audio via multi-client bridge: {e}")
            raise

    async def disconnect(self):
        """
        Disconnect from Plivo and cleanup
        """
        if self.plivo_client:
            await self.plivo_client.close()
        self.is_connected = False
        self.fastapi_clients.clear()
        logger.info("Multi-client Plivo bridge disconnected")


# =====================================
# Synchronous Plivo Integration Example
# =====================================


class SyncPlivoIntegration:
    """
    Example of synchronous Plivo integration using threading
    """

    def __init__(self):
        self.plivo_client = None
        self.plivo_ws = None
        self.is_connected = False
        self.listener_thread = None

    def connect_to_plivo_sync(self, plivo_ws_url: str, message_callback):
        """
        Synchronous Plivo connection using websocket-client
        """
        try:
            # Create synchronous WebSocket connection
            self.plivo_ws = websocket.create_connection(plivo_ws_url)

            # Create sync Plivo client
            self.plivo_client = PlivoAudioStreamClient(self.plivo_ws)

            # Set up event handlers
            @self.plivo_client.onAudio
            def handle_plivo_audio(data):
                """Forward audio from Plivo to callback"""
                message = {
                    "type": "plivo_audio",
                    "data": data,
                    "timestamp": time.time(),
                }
                message_callback(message)

            @self.plivo_client.onStart
            def handle_plivo_start(data):
                """Handle Plivo stream start"""
                logger.info("Plivo stream started (sync)")
                message = {
                    "type": "plivo_stream_started",
                    "data": data,
                    "timestamp": time.time(),
                }
                message_callback(message)

            @self.plivo_client.onEnd
            def handle_plivo_end(data):
                """Handle Plivo stream end"""
                logger.info("Plivo stream ended (sync)")
                message = {
                    "type": "plivo_stream_ended",
                    "data": data,
                    "timestamp": time.time(),
                }
                message_callback(message)

            # Start listening in a separate thread
            self.plivo_client.start_listening()

            self.is_connected = True
            logger.info(f"Successfully connected to Plivo (sync): {plivo_ws_url}")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to Plivo (sync): {e}")
            self.is_connected = False
            return False

    def send_to_plivo_sync(
        self,
        audio_data: str,
        sample_rate: int = 24000,
        content_type: str = "audio/x-l16",
    ):
        """
        Send audio data to Plivo synchronously
        """
        if not self.is_connected or not self.plivo_client:
            raise Exception("Not connected to Plivo")

        try:
            self.plivo_client.playAudio(audio_data, sample_rate, content_type)
            logger.info(f"Sent audio to Plivo (sync): {len(audio_data)} chars")
        except Exception as e:
            logger.error(f"Error sending audio to Plivo (sync): {e}")
            raise

    def disconnect_sync(self):
        """
        Disconnect from Plivo synchronously
        """
        if self.plivo_client:
            self.plivo_client.close()
        self.is_connected = False
        logger.info("Disconnected from Plivo (sync)")


# =====================================
# Additional API Endpoints
# =====================================


@app.post("/api/plivo/connect")
async def api_connect_plivo(request_data: dict):
    """
    REST API endpoint to connect to Plivo
    """
    try:
        plivo_url = request_data.get("plivo_url")
        if not plivo_url:
            raise HTTPException(status_code=400, detail="plivo_url is required")

        # Connect to Plivo using abstraction layer (requires WebSocket context)
        # For REST API, we'll create a dummy WebSocket context
        from fastapi import WebSocket

        dummy_ws = None  # In practice, you'd need active WebSocket connection

        if dummy_ws:
            success = await plivo_bridge.connect_to_plivo(plivo_url, dummy_ws)
        else:
            success = False
            logger.warning("REST API Plivo connection requires active WebSocket client")

        return {
            "success": success,
            "message": (
                "Connected to Plivo via abstraction layer"
                if success
                else "Failed to connect - requires WebSocket client"
            ),
            "plivo_url": plivo_url,
            "note": "Use WebSocket endpoint for full Plivo integration",
            "timestamp": time.time(),
        }

    except Exception as e:
        logger.error(f"API connect error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/audio/send")
async def api_send_audio(request_data: dict):
    """
    REST API endpoint to send audio to Plivo
    """
    try:
        audio_data = request_data.get("audio_data")
        sample_rate = request_data.get("sample_rate", 24000)
        content_type = request_data.get("content_type", "audio/x-l16")

        if not audio_data:
            raise HTTPException(status_code=400, detail="audio_data is required")

        # Validate base64 audio data
        try:
            decoded = base64.b64decode(audio_data)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid base64 audio data")

        # Try to send to Plivo using abstraction layer if connected
        if plivo_bridge.is_connected:
            try:
                await plivo_bridge.send_audio_to_plivo(
                    audio_data, sample_rate, content_type
                )
                plivo_status = "sent_to_plivo"
                logger.info(
                    f"REST API sent audio to Plivo via abstraction layer: {len(decoded)} bytes"
                )
            except Exception as e:
                plivo_status = f"plivo_error: {e}"
                logger.error(f"REST API failed to send to Plivo: {e}")
        else:
            plivo_status = "plivo_not_connected"

        # Also broadcast to all connected FastAPI WebSocket clients
        message = {
            "type": "api_audio",
            "audio_data": audio_data,
            "sample_rate": sample_rate,
            "content_type": content_type,
            "size": len(decoded),
            "plivo_status": plivo_status,
            "timestamp": time.time(),
        }

        await manager.broadcast(json.dumps(message))

        return {
            "success": True,
            "message": "Audio processed via abstraction layer",
            "audio_size": len(decoded),
            "plivo_status": plivo_status,
            "clients_notified": len(manager.active_connections),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API send audio error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/status")
async def api_status():
    """
    Get current system status
    """
    return {
        "status": "running",
        "active_connections": len(manager.active_connections),
        "plivo_connected": plivo_bridge.is_connected,
        "uptime": time.time(),
        "endpoints": {
            "websocket_audio": "/ws/audio-stream",
            "websocket_plivo": "/ws/plivo-bridge/{plivo_endpoint}",
            "rest_api": {
                "connect": "/api/plivo/connect",
                "send_audio": "/api/audio/send",
                "status": "/api/status",
            },
        },
    }


# =====================================
# Application Startup
# =====================================


if __name__ == "__main__":
    logger.info("Starting Plivo FastAPI Audio Streaming Server")
    logger.info("Now using the Plivo Streaming Abstraction Layer!")
    logger.info("")
    logger.info("Features:")
    logger.info("- WebSocket audio streaming with PlivoAsyncAudioStreamClient")
    logger.info("- Real @onAudio, @onStart, @onEnd event handlers")
    logger.info("- Abstraction layer .playAudio() method integration")
    logger.info("- Real-time bidirectional audio relay")
    logger.info("- Context manager support with automatic cleanup")
    logger.info("- FastAPI WebSocket compatibility (automatic detection)")
    logger.info("- REST API endpoints")
    logger.info("")
    logger.info("Access the web interface at: http://localhost:8000")
    logger.info("")
    logger.info("WebSocket endpoints (two different patterns):")
    logger.info("  OUTBOUND PATTERN:")
    logger.info("    - ws://localhost:8000/ws/audio-stream (FastAPI connects TO Plivo)")
    logger.info("    - ws://localhost:8000/ws/plivo-bridge/{plivo_endpoint}")
    logger.info("")
    logger.info("  INCOMING PATTERN:")
    logger.info(
        "    - ws://localhost:8000/ws/plivo-incoming (Plivo connects TO FastAPI)"
    )
    logger.info(
        "    - ws://localhost:8000/ws/incoming-connection (general incoming handler)"
    )
    logger.info("")
    logger.info("Connection patterns:")
    logger.info("  1. Outbound: FastAPI creates connection using websockets.connect()")
    logger.info(
        "  2. Incoming: FastAPI receives WebSocket upgrade and uses it directly"
    )
    logger.info("")
    logger.info("Abstraction layer classes used:")
    logger.info("  - PlivoAsyncAudioStreamClient for async streaming")
    logger.info("  - PlivoAudioStreamClient for sync examples")
    logger.info("  - Both work with incoming OR outbound WebSocket connections")

    uvicorn.run(
        "fastapi_streaming:app",
        host="0.0.0.0",
        port=8000,
        log_level="info",
        reload=True,
    )