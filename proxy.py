'''
proxy.py
'''
import asyncio
import websockets
import json

async def handle_browser_client(websocket):
    """Handle WebSocket connections from browsers"""
    try:
        # Connect to MiniKafka broker
        reader, writer = await asyncio.open_connection('127.0.0.1', 9092)
        
        async def forward_to_kafka(message):
            """Send messages to Kafka"""
            try:
                writer.write(json.dumps(message).encode() + b'\n')
                await writer.drain()
            except ConnectionError:
                print("Kafka connection lost")
                raise

        async def forward_to_browser():
            """Forward messages to browser"""
            try:
                while True:
                    data = await reader.readuntil(b'\n')
                    try:
                        await websocket.send(data.decode())
                    except websockets.ConnectionClosed:
                        print("Browser disconnected")
                        break
            except (ConnectionResetError, asyncio.IncompleteReadError):
                print("Kafka connection closed")

        # Process messages in both directions
        try:
            async for message in websocket:
                await forward_to_kafka(json.loads(message))
            
            # Start forwarding after initial setup
            await forward_to_browser()
            
        except websockets.ConnectionClosed:
            print("Browser client disconnected normally")
        except json.JSONDecodeError:
            print("Received invalid JSON")
        finally:
            writer.close()
            await writer.wait_closed()
            
    except ConnectionRefusedError:
        print("Failed to connect to Kafka broker at 127.0.0.1:9092")
        await websocket.close(1011, "Backend unavailable")

async def main():
    async with websockets.serve(
        handle_browser_client,
        "localhost",
        8765,
        ping_interval=30,
        ping_timeout=30,
        close_timeout=10
    ):
        print("WebSocket proxy running on ws://localhost:8765")
        await asyncio.Future()  # Run forever

if __name__ == "__main__":
    asyncio.run(main())
'''
is this correct 
mini.py should be  running on 127.0.0.1:9092
demo.html should be running on localhost.8000 via http.server
proxy.py should be running on localhost:8765
'''
