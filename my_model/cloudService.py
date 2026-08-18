import asyncio
import copy
import os
import time
from asyncio import Task
from multiprocessing.connection import Connection, wait

from my_model import coordinator, fogService
import multiprocessing as mp



from eclypse.remote.service import Service
from multiprocessing import Lock

import vars
from collections import deque

from vars import coalesce


class CloudService:
    local_neighbors: list = None
    global_neighbors: list = None
    vector_clock = None
    history: list = None
    queue: deque = None
    op_assocs: dict = None
    fog_clocks: dict = None
    fog_contacts: dict = None

    coordinator: Connection

    vector_lock = None
    history_lock = None
    queue_lock = None
    fog_lock = None

    id: str
    neighbours: dict

#======================================================================================================================

    def __init__(self, id: str, local_neighbors, connection: Connection):
        self.id = id
        self.neighbours = {}
        self.i = 0
        self.local_neighbors = local_neighbors
        self.coordinator = connection

        self.vector_clock = {}
        self.history = []
        self.queue = deque()
        self.op_assocs = {}
        self.fog_clocks = {}
        self.fog_contacts = {}

        self.vector_lock = Lock()
        self.history_lock = Lock()
        self.queue_lock = Lock()
        self.fog_lock = Lock()


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
            # self.i += 1
            await self._empty_queue()
            await asyncio.sleep(vars.FOG_QUEUE_TIMER)
            self.coordinator.send({
                vars.MESSAGE_TYPE: vars.COORDINATOR_IS_END
            })
            end = self.coordinator.recv()
            if end:
                await asyncio.sleep(vars.SIMULATION_END_CLEANUP_TIME)
                tasks = asyncio.all_tasks()
                for task in tasks:
                    task.cancel()
                await asyncio.sleep(vars.SIMULATION_END_CLEANUP_TIME)
                break
        return

    async def _first_step(self):

        self.global_neighbors = []
        for neighbor in self.neighbours:
            if "cloud" in neighbor:
                self.global_neighbors.append(neighbor)
        asyncio.create_task(self._message_listener())

# ======================================================================================================================

    async def _message_listener(self):
        senders = []
        for key in self.neighbours:
            senders.append(self.neighbours[key]["connection"])
        while True:
            ready = await asyncio.to_thread(wait, senders)
            for connection in ready:
                msg = connection.recv()
                asyncio.create_task(self._recv_msg(msg))

    async def _send_msg(self, msg_type: int, body, recipients: list):
        msg = {vars.MESSAGE_BODY: body, vars.MESSAGE_TYPE: msg_type}
        #self.log(str(msg))

        for recipient in recipients:
            self.neighbours[recipient]["connection"].send(msg)

    async def _recv_msg(self, msg):
        try:
            msg_type = msg[vars.MESSAGE_TYPE]
            body = msg[vars.MESSAGE_BODY]

            #self.log(str(msg))

            match msg_type:

                case vars.USER_TASK:
                    await self._handle_user_task(body)

                case vars.CLOUD_TASK:
                    await self._handle_cloud_task(body)

                case vars.FOG_TASK:
                    await self._handle_fog_task(body)

                case vars.CREATE_CLUSTER:
                    await self._handle_create_cluster(body)

                case vars.LEADER_CHANGE:
                    await self._handle_change_leader(body)

                case vars.TASK_REQUEST:
                    await self._handle_task_request(body)

        except asyncio.CancelledError:
            return

# ======================================================================================================================

    async def _handle_task_request(self, body: dict):
        fog_id: str = body[vars.FOG_ID]
        req_clock: dict = body[vars.VECTOR_CLOCK]
        queue: deque = deque()
        while True:
            await self._wait_for_req_clock(req_clock)
            with self.fog_lock:
                with self.history_lock:
                    for key in req_clock:
                        if req_clock.get(key) > vars.coalesce(self.fog_clocks[fog_id].get(key), 0):
                            for op_id in self.history:
                                timestamp_clock = self.op_assocs[op_id][vars.VECTOR_CLOCK]
                                session_id = self.op_assocs[op_id][vars.ID]
                                if session_id == key and req_clock.get(key) > coalesce(timestamp_clock.get(key), 0) >= coalesce(self.fog_clocks[fog_id].get(key), 0):
                                    queue.append(op_id)
                            self.fog_clocks[fog_id][key] = req_clock[key]
            if len(queue) <= 0:
                break

            while len(queue) > 0:
                operation_id = queue.pop()
                timestamp_clock = self.op_assocs[operation_id][vars.VECTOR_CLOCK]
                with self.fog_lock:
                    await self._send_msg(
                        vars.CLOUD_TASK,
                        {
                            vars.ID: self.op_assocs[operation_id][vars.ID],
                            vars.OPERATION: self.op_assocs[operation_id][vars.OPERATION],
                            vars.VECTOR_CLOCK: timestamp_clock
                        },
                        [self.fog_contacts[fog_id]]
                    )
                for key in timestamp_clock:
                    req_clock[key] = max(timestamp_clock.get(key), coalesce(req_clock.get(key), 0))

    async def _handle_change_leader(self, body: dict):
        address: str = body[vars.ID]
        cluster_id = body[vars.FOG_ID]

        with self.fog_lock:
            if address != "":
                self.fog_contacts[cluster_id] = address
            else:
                del self.fog_clocks[cluster_id]
                del self.fog_contacts[cluster_id]


    async def _handle_create_cluster(self, body: dict):
        address: str = body[vars.ID]
        cluster_id = fogService.get_fog_id(address)
        with self.fog_lock:
            self.fog_clocks[cluster_id] = {}
            self.fog_contacts[cluster_id] = address

            await self._send_msg(
                vars.CREATE_CLUSTER,
                {vars.FOG_ID: cluster_id},
                [address]
            )

    async def _handle_cloud_task(self, body: dict):

        session_id: str = body[vars.ID]
        op: dict = body[vars.OPERATION]
        req_clock: dict = body[vars.VECTOR_CLOCK]

        await self._wait_for_req_clock(req_clock)
        self._perform_operation(op)
        with self.vector_lock:
            self.op_assocs[op[vars.ID]] = {
                vars.OPERATION: op,
                vars.ID: session_id,
                vars.VECTOR_CLOCK: copy.deepcopy(self.vector_clock)
            }
            self.vector_clock[session_id] = vars.coalesce(self.vector_clock.get(session_id), 0) + 1
        with self.history_lock:
            self.history.append(op[vars.ID])

    async def _handle_fog_task(self, body: dict):

        fog_id: str = body[vars.FOG_ID]
        session_id: str = body[vars.ID]
        op: dict = body[vars.OPERATION]
        req_clock: dict = body[vars.VECTOR_CLOCK]
        network_range: str = body[vars.NETWORK_RANGE]

        await self._wait_for_req_clock(req_clock)
        self._perform_operation(op)
        del body[vars.FOG_ID]
        with self.queue_lock:
            self.queue.append(body)

        with self.vector_lock:
            self.op_assocs[op[vars.ID]] = {
                vars.OPERATION: op,
                vars.ID: session_id,
                vars.VECTOR_CLOCK: copy.deepcopy(self.vector_clock)
            }
            self.vector_clock[session_id] = vars.coalesce(self.vector_clock.get(session_id), 0) + 1
        with self.fog_lock:
            self.fog_clocks[fog_id][session_id] = vars.coalesce(self.fog_clocks[fog_id].get(session_id), 0) + 1

        with self.history_lock:
            self.history.append(op[vars.ID])

    async def _handle_user_task(self, body: dict):

        session_id: str = body[vars.ID]
        op: dict = body[vars.OPERATION]
        req_clock: dict = body[vars.VECTOR_CLOCK]
        network_range: str = body[vars.NETWORK_RANGE]
        #self.logger.info(time.time() - body["time"])

        await self._wait_for_req_clock(req_clock)
        self._perform_operation(op)
        response_body = {
            vars.OPERATION: op,
            vars.RESULT: True
        }
        if vars.is_write(op):
            with self.vector_lock:
                self.op_assocs[op[vars.ID]] = {
                    vars.OPERATION: op,
                    vars.ID: session_id,
                    vars.VECTOR_CLOCK: copy.deepcopy(self.vector_clock)
                }
                self.vector_clock[session_id] = vars.coalesce(self.vector_clock.get(session_id), 0) + 1
            with self.history_lock:
                self.history.append(op[vars.ID])
            with self.queue_lock:
                self.queue.append(body)
        else:
            with self.vector_lock:
                response_body[vars.VECTOR_CLOCK] = copy.deepcopy(self.vector_clock)
        await self._send_msg(vars.TASK_CONFIRM, response_body, [vars.get_addr_from_session_id(session_id)])

# ======================================================================================================================

    async def _empty_queue(self):
        queue_copy: deque
        with self.queue_lock:
            queue_copy = copy.deepcopy(self.queue)
            self.queue.clear()

        for i in range(len(queue_copy)):
            body = queue_copy.pop()
            network_range = body[vars.NETWORK_RANGE]
            recipients: list
            if network_range == vars.LOCAL_RANGE:
                recipients = self.local_neighbors
            else:
                recipients = self.global_neighbors
            del body[vars.NETWORK_RANGE]
            await  self._send_msg(
                vars.CLOUD_TASK,
                body,
                recipients
            )

    def _perform_operation(self, op: dict):
        # self.logger.info(str(op[vars.ID]) + " performed on node " + self.id)
        pass

    async def _wait_for_req_clock(self, req_clock: dict):
        while not self._check_req_clock(req_clock):
            await asyncio.sleep(vars.CLOCK_WAIT_TIME)

    def _check_req_clock(self, req_clock: dict) -> bool:
        with self.vector_lock:
            for key in req_clock:
                if vars.coalesce(self.vector_clock.get(key), 0) < req_clock.get(key):
                    return False
            return True


    def log(self, msg: str):
        print(self.id + " " + msg, flush=True)