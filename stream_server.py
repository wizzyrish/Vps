import asyncio
import json
import subprocess
from websockets.server import serve

class CommandStreamer:
    def __init__(self):
        self.active_processes = {}

    async def execute_command(self, command, websocket):
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE
        )

        self.active_processes[websocket.id] = process

        async def read_stream(stream, stream_type):
            while True:
                line = await stream.readline()
                if not line:
                    break
                await websocket.send(json.dumps({
                    'type': stream_type,
                    'output': line.decode('utf-8')
                }))

        await asyncio.gather(
            read_stream(process.stdout, 'stdout'),
            read_stream(process.stderr, 'stderr')
        )

        await websocket.send(json.dumps({
            'type': 'exit',
            'code': await process.wait()
        }))

        del self.active_processes[websocket.id]

    async def terminate_process(self, websocket_id):
        if websocket_id in self.active_processes:
            self.active_processes[websocket_id].terminate()

async def handler(websocket):
    streamer = CommandStreamer()
    try:
        async for message in websocket:
            data = json.loads(message)
            if data['type'] == 'execute':
                await streamer.execute_command(data['command'], websocket)
            elif data['type'] == 'terminate':
                await streamer.terminate_process(websocket.id)
    finally:
        await streamer.terminate_process(websocket.id)

async def main():
    async with serve(handler, "localhost", 8765):
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())
