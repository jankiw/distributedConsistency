import asyncio
import os
from multiprocessing.connection import Connection, wait
from multiprocessing.dummy import connection

import vars


class ServiceCoordinator:
    end: bool
    user_count: int

    data: dict
    connections: list

    def __init__(self, user_count):
        self.end = False
        self.user_count = user_count
        self.data = {
            "op_count": 0,
            "total_time": 0.0
        }
        self.connections = []

    def start(self):
        asyncio.run(self._message_listener())

    async def _message_listener(self):
        while True:
            ready = wait(self.connections)
            for connection in ready:
                msg = connection.recv()
                asyncio.create_task(self._recv_msg(msg, connection))
            await asyncio.sleep(0.01)

    async def _recv_msg(self, msg, connection: Connection):
        try:
            msg_type = msg[vars.MESSAGE_TYPE]

            match msg_type:
                case vars.COORDINATOR_FINISH:
                    body = msg[vars.MESSAGE_BODY]
                    self.user_finish(body)
                case vars.COORDINATOR_IS_END:
                    connection.send(self.end)


        except asyncio.CancelledError:
            return

    def add_connection(self, connection: Connection):
        self.connections.append(connection)

    def user_finish(self, result):
        self.data["op_count"] += result["op_count"]
        self.data["total_time"] += result["total_time"]

        self.user_count -= 1
        if self.user_count <= 0:
            self.end = True
            print(self.get_data(), flush=True)

    def get_data(self) -> dict:
        self.data["avg_time"] = self.data["total_time"] / self.data["op_count"]
        return self.data

    def log(self, msg: str):
        print("coordinator " + msg, flush=True)