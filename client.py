import asyncio
import json

async def produce_message(host: str, port: int, topic: str, message: str):
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(json.dumps({
        "action": "produce",
        "topic": topic,
        "data": message
    }).encode() + b'\n')
    await writer.drain()
    writer.close()
    await writer.wait_closed()

async def consume_messages(host: str, port: int, topic: str, offset: int = 0):
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(json.dumps({
        "action": "consume",
        "topic": topic,
        "offset": offset
    }).encode() + b'\n')
    await writer.drain()
    data = await reader.readuntil(b'\n')
    print(json.loads(data.decode().strip()))
    writer.close()
    await writer.wait_closed()

if __name__ == "__main__":
    asyncio.run(produce_message("127.0.0.1", 9092, "test-topic", "Hello from client!"))
    asyncio.run(consume_messages("127.0.0.1", 9092, "test-topic"))