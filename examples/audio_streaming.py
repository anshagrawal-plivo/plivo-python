#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Example of using Plivo Audio Streaming WebSocket Clients

This example demonstrates how to use both PlivoAudioStreamClient (sync)
and PlivoAsyncAudioStreamClient (async) to handle real-time audio
streaming over WebSocket connections.

Requirements:
    # For synchronous examples:
    pip install websocket-client

    # For asynchronous examples:
    pip install websockets

    # For all examples:
    pip install websocket-client websockets

Usage:
    python audio_streaming.py
"""

import base64
import time
import asyncio
import websocket
from plivo import PlivoAudioStreamClient

# Import async components if available
try:
    import websockets
    from plivo import PlivoAsyncAudioStreamClient

    ASYNC_AVAILABLE = True
except ImportError:
    ASYNC_AVAILABLE = False
    print("Note: websockets library not available. Async examples will be skipped.")


def example_basic_usage():
    """Basic example of using the PlivoAudioStreamClient"""

    # Example WebSocket URL - replace with your actual streaming endpoint
    websocket_url = "wss://your-streaming-endpoint.example.com/audio"

    try:
        # Create WebSocket connection
        ws = websocket.create_connection(websocket_url)
        print(f"Connected to WebSocket: {websocket_url}")

        # Create the audio streaming client
        stream_client = PlivoAudioStreamClient(ws)

        # Define event handlers
        @stream_client.onAudio
        def handle_audio(data):
            """Handle incoming audio data"""
            print(
                f"Received audio data: {len(data.get('media', {}).get('payload', ''))} bytes"
            )
            # Process audio data here
            # For example, save to file, process with AI, etc.

        @stream_client.onStart
        def handle_start(data):
            """Handle stream start event"""
            print("Audio stream started:", data)

        @stream_client.onEnd
        def handle_end(data):
            """Handle stream end event"""
            print("Audio stream ended:", data)

        # Start listening for events
        stream_client.start_listening()
        print("Started listening for audio stream events...")

        # Example: Send some audio data
        # This would typically be real audio data from microphone, file, etc.
        example_audio_data = base64.b64encode(b"dummy_audio_data").decode("utf-8")

        for i in range(3):
            print(f"Sending audio chunk {i+1}...")
            stream_client.playAudio(example_audio_data)
            time.sleep(1)  # Wait between sends

        # Keep the connection alive for a while to receive events
        print("Waiting for events... (press Ctrl+C to stop)")
        time.sleep(10)

    except KeyboardInterrupt:
        print("\nStopping audio stream...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Clean up
        if "stream_client" in locals():
            stream_client.close()
        print("Audio stream client closed.")


def example_context_manager():
    """Example using the client as a context manager"""

    websocket_url = "wss://your-streaming-endpoint.example.com/audio"

    try:
        ws = websocket.create_connection(websocket_url)

        # Use as context manager for automatic cleanup
        with PlivoAudioStreamClient(ws) as stream_client:

            @stream_client.onAudio
            def handle_audio(data):
                media_data = data.get("media", {})
                payload = media_data.get("payload", "")
                sample_rate = media_data.get("sampleRate", "unknown")
                print(f"Audio: {len(payload)} bytes at {sample_rate} Hz")

            @stream_client.onStart
            def handle_start(data):
                print("Stream started, ready to receive audio")

            @stream_client.onEnd
            def handle_end(data):
                print("Stream ended")

            stream_client.start_listening()

            # Simulate some work
            time.sleep(5)

        # Client is automatically closed when exiting the context
        print("Client automatically closed")

    except Exception as e:
        print(f"Error: {e}")


def example_file_streaming():
    """Example of streaming audio from a file"""

    websocket_url = "wss://your-streaming-endpoint.example.com/audio"

    try:
        ws = websocket.create_connection(websocket_url)
        stream_client = PlivoAudioStreamClient(ws)

        @stream_client.onStart
        def handle_start(data):
            print("Stream ready - starting file playback")

            # Example of reading and streaming an audio file
            try:
                # This is a dummy example - replace with actual audio file reading
                with open("example_audio.wav", "rb") as audio_file:
                    chunk_size = 1024  # Read in chunks
                    while True:
                        audio_chunk = audio_file.read(chunk_size)
                        if not audio_chunk:
                            break

                        # Encode as base64 and send
                        encoded_audio = base64.b64encode(audio_chunk).decode("utf-8")
                        stream_client.playAudio(encoded_audio)

                        # Small delay between chunks to simulate real-time streaming
                        time.sleep(0.1)

            except FileNotFoundError:
                print("Audio file not found - using dummy data instead")
                dummy_data = base64.b64encode(b"dummy_audio_chunk").decode("utf-8")
                stream_client.playAudio(dummy_data)

        stream_client.start_listening()

        # Wait for the stream to process
        time.sleep(10)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if "stream_client" in locals():
            stream_client.close()


# ========================
# ASYNC EXAMPLES
# ========================


async def example_async_basic_usage():
    """Basic example of using the PlivoAsyncAudioStreamClient"""

    if not ASYNC_AVAILABLE:
        print("Skipping async example - websockets library not available")
        return

    # Example WebSocket URL - replace with your actual streaming endpoint
    websocket_url = "wss://your-streaming-endpoint.example.com/audio"

    try:
        # Create async WebSocket connection
        async with websockets.connect(websocket_url) as ws:
            print(f"Connected to async WebSocket: {websocket_url}")

            # Create the async audio streaming client
            stream_client = PlivoAsyncAudioStreamClient(ws)

            # Define async event handlers
            @stream_client.onAudio
            async def handle_audio(data):
                """Handle incoming audio data asynchronously"""
                print(
                    f"Received audio data: {len(data.get('media', {}).get('payload', ''))} bytes"
                )
                # Process audio data here asynchronously
                # For example, save to file, process with AI, etc.

            @stream_client.onStart
            async def handle_start(data):
                """Handle stream start event"""
                print("Async audio stream started:", data)

            @stream_client.onEnd
            async def handle_end(data):
                """Handle stream end event"""
                print("Async audio stream ended:", data)

            # Start listening for events (runs in background)
            listen_task = asyncio.create_task(stream_client.start_listening())
            print("Started listening for async audio stream events...")

            # Example: Send some audio data
            example_audio_data = base64.b64encode(b"dummy_audio_data").decode("utf-8")

            for i in range(3):
                print(f"Sending async audio chunk {i+1}...")
                await stream_client.playAudio(example_audio_data)
                await asyncio.sleep(1)  # Async sleep between sends

            # Wait for events with timeout
            try:
                await asyncio.wait_for(listen_task, timeout=5.0)
            except asyncio.TimeoutError:
                print("Timeout reached, cancelling listen task")
                listen_task.cancel()

    except Exception as e:
        print(f"Async example error: {e}")


async def example_async_context_manager():
    """Example using the async client as a context manager"""

    if not ASYNC_AVAILABLE:
        print(
            "Skipping async context manager example - websockets library not available"
        )
        return

    websocket_url = "wss://your-streaming-endpoint.example.com/audio"

    try:
        # Use nested async context managers for automatic cleanup
        async with websockets.connect(websocket_url) as ws:
            async with PlivoAsyncAudioStreamClient(ws) as stream_client:

                @stream_client.onAudio
                async def handle_audio(data):
                    media_data = data.get("media", {})
                    payload = media_data.get("payload", "")
                    sample_rate = media_data.get("sampleRate", "unknown")
                    print(f"Async Audio: {len(payload)} bytes at {sample_rate} Hz")

                @stream_client.onStart
                async def handle_start(data):
                    print("Async stream started, ready to receive audio")

                @stream_client.onEnd
                async def handle_end(data):
                    print("Async stream ended")

                # Start listening in background
                listen_task = asyncio.create_task(stream_client.start_listening())

                # Simulate some async work
                await asyncio.sleep(2)

                # Cancel the listening task
                listen_task.cancel()

        # Both WebSocket and client are automatically closed
        print("Async client automatically closed")

    except Exception as e:
        print(f"Async context manager error: {e}")


async def example_async_file_streaming():
    """Example of streaming audio from a file asynchronously"""

    if not ASYNC_AVAILABLE:
        print(
            "Skipping async file streaming example - websockets library not available"
        )
        return

    websocket_url = "wss://your-streaming-endpoint.example.com/audio"

    try:
        async with websockets.connect(websocket_url) as ws:
            stream_client = PlivoAsyncAudioStreamClient(ws)

            @stream_client.onStart
            async def handle_start(data):
                print("Async stream ready - starting file playback")

                # Example of reading and streaming an audio file asynchronously
                try:
                    # This is a dummy example - replace with actual async file reading
                    # You might use aiofiles for async file operations
                    import aiofiles

                    async with aiofiles.open("example_audio.wav", "rb") as audio_file:
                        chunk_size = 1024  # Read in chunks
                        while True:
                            audio_chunk = await audio_file.read(chunk_size)
                            if not audio_chunk:
                                break

                            # Encode as base64 and send
                            encoded_audio = base64.b64encode(audio_chunk).decode(
                                "utf-8"
                            )
                            await stream_client.playAudio(encoded_audio)

                            # Small delay between chunks to simulate real-time streaming
                            await asyncio.sleep(0.1)

                except (FileNotFoundError, ImportError):
                    print(
                        "Audio file not found or aiofiles not available - using dummy data instead"
                    )
                    dummy_data = base64.b64encode(b"dummy_audio_chunk").decode("utf-8")
                    await stream_client.playAudio(dummy_data)

            # Start listening
            listen_task = asyncio.create_task(stream_client.start_listening())

            # Wait for the stream to process with timeout
            try:
                await asyncio.wait_for(listen_task, timeout=10.0)
            except asyncio.TimeoutError:
                listen_task.cancel()

    except Exception as e:
        print(f"Async file streaming error: {e}")


if __name__ == "__main__":
    print("Plivo Audio Streaming Examples")
    print("==============================")

    print("\n1. Basic Usage Example:")
    try:
        example_basic_usage()
    except Exception as e:
        print(f"Basic example failed: {e}")

    print("\n2. Context Manager Example:")
    try:
        example_context_manager()
    except Exception as e:
        print(f"Context manager example failed: {e}")

    print("\n3. File Streaming Example:")
    try:
        example_file_streaming()
    except Exception as e:
        print(f"File streaming example failed: {e}")

    # Async examples
    if ASYNC_AVAILABLE:
        print("\n" + "=" * 40)
        print("ASYNC EXAMPLES")
        print("=" * 40)

        print("\n4. Async Basic Usage Example:")
        try:
            asyncio.run(example_async_basic_usage())
        except Exception as e:
            print(f"Async basic example failed: {e}")

        print("\n5. Async Context Manager Example:")
        try:
            asyncio.run(example_async_context_manager())
        except Exception as e:
            print(f"Async context manager example failed: {e}")

        print("\n6. Async File Streaming Example:")
        try:
            asyncio.run(example_async_file_streaming())
        except Exception as e:
            print(f"Async file streaming example failed: {e}")
    else:
        print("\n" + "=" * 40)
        print("ASYNC EXAMPLES SKIPPED")
        print("=" * 40)
        print("Install 'websockets' library to run async examples:")
        print("pip install websockets")

    print("\nAll examples completed!")
