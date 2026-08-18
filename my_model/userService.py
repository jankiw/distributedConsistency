import asyncio
import os
import time
from multiprocessing.connection import Connection, wait

import numpy as np

from my_model import coordinator
import multiprocessing as mp


import vars

GLOBAL_GROUP: int = -1

def _get_id_number(node_id) -> int:
    return int(node_id.split('-')[1])

class UserService:
    node_neighbors: list
    write_clock: dict
    read_clock: dict
    session_guarantees: dict
    session_network_range: str
    session_id = None
    coordinator: Connection
    local_groups = None
    current_group: int = None
    rng = None

    total_time: float
    op_count: int

    id: str
    neighbours: dict

    def __init__(self, id: str, local_groups, connection: Connection):
        self.id = id
        self.neighbours = {}
        self.i = 0
        self.local_groups = local_groups
        self.rng = np.random.default_rng(_get_id_number(self.id))
        self.coordinator = connection

        self.total_time = 0.0
        self.op_count = 0


    def add_neighbour(self, id: str, latency: float, connection: Connection):
        self.neighbours[id] = {
            "latency": latency,
            "connection": connection
        }

    def start(self):
        asyncio.run(self._start())

    async def _start(self):
        if self.i == 0:
            await self._first_step()
        while True:
            #self.log(str(self.i))
            await asyncio.sleep(0.05)
            self.i += 1
            if self.i in [5,10,15]:
                self._end_session()
                self._start_session()

            await self._send_op(vars.WRITE_OP, "my value")

            if self.i >= 20:
                self.coordinator.send({
                    vars.MESSAGE_TYPE: vars.COORDINATOR_FINISH,
                    vars.MESSAGE_BODY: {"total_time": self.total_time, "op_count": self.op_count, "avg_time": self.total_time / self.op_count}
                })
                return
        return

    async def _send_op(self, op_type: str, value: str):
        start_time = time.time()

        neighbor: str
        if self.current_group == GLOBAL_GROUP:
            neighbor = self.rng.choice(list(self.neighbours.keys()))
        else:
            neighbor = self.rng.choice(self.local_groups[self.current_group])
        req_clock: dict = {}
        op: dict = {
            vars.ID: self.id + "_" + str(self.i),
            vars.TYPE: op_type,
            vars.VALUE: value
        }
        self.log("sent to " + neighbor + " op " + str(op[vars.ID]))
        is_write: bool = vars.is_write(op)
        if is_write or self.session_guarantees.__contains__(vars.READ_YOUR_WRITES):
            if self.write_clock.get(self.session_id) is not None:
                req_clock[self.id] = self.write_clock.get(self.session_id)

        if (is_write and self.session_guarantees.__contains__(vars.WRITES_FOLLOW_READS)) or (not is_write and self.session_guarantees.__contains__(vars.MONOTONIC_READS)):
            for key in self.read_clock:
                req_clock[key] = max(vars.coalesce(req_clock.get(key), 0), self.read_clock.get(key))

        body: dict = {
            vars.ID: self.session_id,
            vars.OPERATION: op,
            vars.NETWORK_RANGE: vars.GLOBAL_RANGE,
            vars.VECTOR_CLOCK: req_clock,
            "time": time.time()
        }
        msg = {vars.MESSAGE_BODY: body, vars.MESSAGE_TYPE: vars.USER_TASK}
        # self.logger.info(msg)
        self.neighbours[neighbor]["connection"].send(msg)

        senders = []
        for key in self.neighbours:
            senders.append(self.neighbours[key]["connection"])

        ready = await asyncio.to_thread(wait, senders)
        for connection in ready:
            msg = connection.recv()
            self._recv_msg(msg)

        end_time = time.time()
        self.total_time += end_time - start_time
        self.op_count += 1

    def _recv_msg(self, msg):
        self.log("received confirmation " + str(self.i))
        body: dict = msg[vars.MESSAGE_BODY]
        res: bool = body[vars.RESULT]
        op: dict = body[vars.OPERATION]


        if not res:
            return

        if vars.is_write(op):
            self.write_clock[self.id] = vars.coalesce(self.write_clock.get(self.id), 0) + 1
        else:
            v: dict = body[vars.VECTOR_CLOCK]
            for key in v:
                self.read_clock[key] = max(vars.coalesce(self.read_clock.get(key), 0), v.get(key))

    def _start_session(self):
        self.write_clock = {}
        self.read_clock = {}
        self.session_id = self.id + "_" + str(self.i)
        self.session_guarantees = {
            vars.READ_YOUR_WRITES: self.rng.choice([True, False]),
            vars.WRITES_FOLLOW_READS: self.rng.choice([True, False]),
            vars.MONOTONIC_READS: self.rng.choice([True, False]),
            vars.MONOTONIC_WRITES: self.rng.choice([True, False])
        }
        self.session_network_range = self.rng.choice([vars.GLOBAL_RANGE, vars.LOCAL_RANGE])
        if self.session_network_range == vars.GLOBAL_RANGE:
            self.current_group = GLOBAL_GROUP
        else:
            self.current_group = self.rng.choice(range(len(self.local_groups)))

    def _end_session(self):
        self.write_clock = {}
        self.read_clock = {}
        self.session_guarantees = {}
        self.session_network_range = ""
        self.session_id = None
        self.current_group = None

    async def _first_step(self):

        self._start_session()

    def log(self, msg: str):
        print(self.id + " " + msg, flush=True)