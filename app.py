import asyncio
import json
import os
from collections import defaultdict
import pickle
from typing import Dict, List, Callable
import logging
import platform
'''
% echo '{"action":"produce","topic":"test","value":"hello"}' | nc localhost 9092
'''

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MiniKafka")

class MiniKafkaBroker:
    def __init__(self, storage_path: str = "minikafka_data"):
        self.topics: Dict[str, List[bytes]] = defaultdict(list)
        self.subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self.storage_path = storage_path
        self.load_state()

    def load_state(self):
        """Load persisted messages from disk"""
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, 'rb') as f:
                    self.topics = pickle.load(f)
                logger.info("Loaded persisted state from disk")
            except Exception as e:
                logger.error(f"Failed to load state: {e}")

    def save_state(self):
        """Persist messages to disk"""
        try:
            with open(self.storage_path, 'wb') as f:
                pickle.dump(dict(self.topics), f)  # Convert defaultdict to dict
            logger.info("Saved state to disk")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    async def produce(self, topic: str, message: bytes):
        """Produce a message to a topic"""
        self.topics[topic].append(message)
        logger.info(f"Produced message to topic {topic}")
        self.save_state()
        
        # Notify subscribers
        for callback in self.subscribers[topic]:
            try:
                await callback(message)
            except Exception as e:
                logger.error(f"Subscriber callback failed: {e}")

    def subscribe(self, topic: str, callback: Callable):
        """Subscribe to a topic"""
        self.subscribers[topic].append(callback)
        logger.info(f"Subscribed to topic {topic}")

    def unsubscribe(self, topic: str, callback: Callable):
        """Unsubscribe from a topic"""
        if callback in self.subscribers[topic]:
            self.subscribers[topic].remove(callback)
            logger.info(f"Unsubscribed from topic {topic}")

    async def consume(self, topic: str, offset: int = 0) -> List[bytes]:
        """Consume messages from a topic starting at offset"""
        messages = self.topics[topic][offset:]
        logger.info(f"Consumed {len(messages)} messages from topic {topic} at offset {offset}")
        return messages

async def handle_client(broker: MiniKafkaBroker, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Handle client connections"""
    client_callbacks = []  # Track callbacks for cleanup
    
    try:
        while True:
            # Read until newline to get one JSON message at a time
            data = await reader.readuntil(b'\n')
            if not data:
                break
                
            try:
                message = json.loads(data.decode().strip())
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON received: {e}")
                error_response = json.dumps({"status": "error", "message": "Invalid JSON"}).encode() + b'\n'
                writer.write(error_response)
                await writer.drain()
                continue
                
            action = message.get("action")
            
            if action == "produce":
                topic = message.get("topic")
                data_msg = message.get("data", "")
                if not topic:
                    writer.write(json.dumps({"status": "error", "message": "Missing topic"}).encode() + b'\n')
                else:
                    await broker.produce(topic, data_msg.encode())
                    writer.write(json.dumps({"status": "ok"}).encode() + b'\n')
            
            elif action == "consume":
                topic = message.get("topic")
                if not topic:
                    writer.write(json.dumps({"status": "error", "message": "Missing topic"}).encode() + b'\n')
                else:
                    messages = await broker.consume(topic, message.get("offset", 0))
                    writer.write(json.dumps({
                        "status": "ok",
                        "messages": [m.decode() for m in messages]
                    }).encode() + b'\n')
            
            elif action == "subscribe":
                topic = message.get("topic")
                if not topic:
                    writer.write(json.dumps({"status": "error", "message": "Missing topic"}).encode() + b'\n')
                else:
                    async def callback(msg: bytes):
                        try:
                            if writer.is_closing():
                                return
                            response = json.dumps({
                                "status": "subscription",
                                "topic": topic,
                                "data": msg.decode()
                            }).encode() + b'\n'
                            writer.write(response)
                            await writer.drain()
                        except Exception as e:
                            logger.error(f"Failed to send subscription message: {e}")
                    
                    broker.subscribe(topic, callback)
                    client_callbacks.append((topic, callback))  # Track for cleanup
                    writer.write(json.dumps({"status": "subscribed"}).encode() + b'\n')
            
            else:
                writer.write(json.dumps({"status": "error", "message": "Unknown action"}).encode() + b'\n')
            
            await writer.drain()
            
    except asyncio.IncompleteReadError:
        logger.info("Client disconnected")
    except Exception as e:
        logger.error(f"Client error: {e}")
    finally:
        # Clean up subscriptions
        for topic, callback in client_callbacks:
            broker.unsubscribe(topic, callback)
        
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()

async def start_server(broker: MiniKafkaBroker, host: str = "127.0.0.1", port: int = 9092):
    """Start the TCP server"""
    server = await asyncio.start_server(
        lambda r, w: handle_client(broker, r, w), host, port)
    logger.info(f"Server started on {host}:{port}")
    async with server:
        await server.serve_forever()

async def example_client(host: str = "127.0.0.1", port: int = 9092):
    """Example client demonstrating usage with retry mechanism"""
    max_retries = 5
    retry_delay = 1  # seconds
    
    for attempt in range(max_retries):
        try:
            reader, writer = await asyncio.open_connection(host, port)
            break  # Connection successful, exit retry loop
        except ConnectionRefusedError:
            if attempt < max_retries - 1:
                logger.info(f"Connection attempt {attempt + 1} failed, retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to {host}:{port} after {max_retries} attempts")
                return
    
    try:
        # Produce a message
        writer.write(json.dumps({
            "action": "produce",
            "topic": "test-topic",
            "data": "Hello, MiniKafka!"
        }).encode() + b'\n')
        await writer.drain()
        
        # Read response for produce
        try:
            data = await asyncio.wait_for(reader.readuntil(b'\n'), timeout=5.0)
            if data:
                logger.info(f"Produce response: {json.loads(data.decode().strip())}")
        except (asyncio.TimeoutError, json.JSONDecodeError) as e:
            logger.error(f"Failed to read produce response: {e}")
        
        # Small delay to ensure server processes produce
        await asyncio.sleep(0.1)
        
        # Consume messages
        writer.write(json.dumps({
            "action": "consume",
            "topic": "test-topic",
            "offset": 0
        }).encode() + b'\n')
        await writer.drain()
        
        try:
            data = await asyncio.wait_for(reader.readuntil(b'\n'), timeout=5.0)
            if data:
                print("Consumed:", json.loads(data.decode().strip()))
            else:
                logger.error("No data received for consume action")
        except (asyncio.TimeoutError, json.JSONDecodeError) as e:
            logger.error(f"Failed to parse consume response: {e}")
        
        # Subscribe to topic
        writer.write(json.dumps({
            "action": "subscribe",
            "topic": "test-topic"
        }).encode() + b'\n')
        await writer.drain()
        
        try:
            data = await asyncio.wait_for(reader.readuntil(b'\n'), timeout=5.0)
            if data:
                logger.info(f"Subscribe response: {json.loads(data.decode().strip())}")
        except (asyncio.TimeoutError, json.JSONDecodeError) as e:
            logger.error(f"Failed to read subscribe response: {e}")
        
        # Produce another message to see subscription in action
        reader2, writer2 = await asyncio.open_connection(host, port)
        writer2.write(json.dumps({
            "action": "produce",
            "topic": "test-topic",
            "data": "Subscription test!"
        }).encode() + b'\n')
        await writer2.drain()
        
        try:
            data = await asyncio.wait_for(reader.readuntil(b'\n'), timeout=5.0)
            if data:
                print("Subscribed message:", json.loads(data.decode().strip()))
            else:
                logger.error("No data received for subscription")
        except (asyncio.TimeoutError, json.JSONDecodeError) as e:
            logger.error(f"Failed to parse subscription response: {e}")
        
        writer2.close()
        await writer2.wait_closed()
    
    finally:
        writer.close()
        await writer.wait_closed()

if platform.system() == "Emscripten":
    async def main():
        broker = MiniKafkaBroker()
        await start_server(broker)
    asyncio.ensure_future(main())
else:
    if __name__ == "__main__":
        async def main():
            broker = MiniKafkaBroker()
            # Run server and example client concurrently for demo
            server_task = asyncio.create_task(start_server(broker))
            
            # Give server time to start
            await asyncio.sleep(0.5)
            
            # Run example client
            '''
            try:
                await example_client()
            except Exception as e:
                logger.error(f"Example client failed: {e}")
            
            # Cancel server task (in real usage, server would run indefinitely)
            server_task.cancel()
            '''
            try:
                await server_task
            except asyncio.CancelledError:
                pass
        
        asyncio.run(main())