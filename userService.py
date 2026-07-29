import asyncio
import os
import random
import uuid

os.environ["RAY_DEDUP_LOGS"] = "0"
import ray
from eclypse.remote.service import Service
from ray.actor import ActorHandle

import coordinator
import vars


class UserService(Service):
    node_neighbors: list
    write_clock: dict
    read_clock: dict
    session_guarantees: dict
    session_network_range: str
    session_id = None
    coordinator: ActorHandle

    def __init__(self, service_id: str):
        super().__init__(service_id, store_step=True)

        self.i = 0


    async def step(self):
        if self.i == 0:
            await self._first_step()
        while True:
            await self._send_op(vars.WRITE_OP, "my value")
            await asyncio.sleep(1)
            self.i += 1
            if self.i == 10:
                await self.coordinator.user_finish.remote()
                break
        return 1

    async def _send_op(self, op_type: str, value: str):
        neighbor: str = random.choice(self.node_neighbors)
        req_clock: dict = {}
        op: dict = {
            vars.ID: uuid.uuid4(),
            vars.TYPE: op_type,
            vars.VALUE: value
        }
        self.logger.info("sent to " + neighbor + " op " + str(op[vars.ID]))
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
            vars.VECTOR_CLOCK: req_clock
        }
        msg = {vars.MESSAGE_BODY: body, vars.MESSAGE_TYPE: vars.USER_TASK}
        # self.logger.info(msg)
        confirm = self.mpi.send([neighbor], msg)
        if asyncio.iscoroutine(confirm):
            await confirm

        while True:
            try:
                msg = await self.mpi.recv()
                if msg:
                    self._recv_msg(msg)
                    break
            except asyncio.CancelledError:
                break

    def _recv_msg(self, msg):
        self.logger.info("received confirmation " + str(self.i))
        body: dict = msg[vars.MESSAGE_BODY]
        res: bool = body[vars.RESULT]
        op: dict = body[vars.OPERATION]

        # self.logger.info(msg)

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
        self.session_id = uuid.uuid4()
        self.session_guarantees = {
            vars.READ_YOUR_WRITES: True,
            vars.WRITES_FOLLOW_READS: True,
            vars.MONOTONIC_READS: True,
            vars.MONOTONIC_WRITES: True
        }
        self.session_network_range = vars.GLOBAL_RANGE

    def _end_session(self):
        self.write_clock = {}
        self.read_clock = {}
        self.session_guarantees = {}
        self.session_network_range = ""
        self.session_id = None

    async def _first_step(self):
        try:
            self.coordinator = coordinator.ServiceCoordinator.options(name=vars.COORDINATOR_NAME,
                                                                      namespace=vars.COORDINATOR_NAMESPACE).remote(
                user_count=vars.USER_COUNT)
        except:
            self.coordinator = ray.get_actor(vars.COORDINATOR_NAME, namespace=vars.COORDINATOR_NAMESPACE)

        self.node_neighbors = await self.mpi.get_neighbors()
        self._start_session()