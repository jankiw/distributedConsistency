import asyncio
import copy
import os

os.environ["RAY_DEDUP_LOGS"] = "0"
import ray
from pandas.io.formats import console
from ray.actor import ActorHandle

import coordinator
import fogService

from eclypse.remote.service import Service
from multiprocessing import Lock

import vars
from collections import deque

from vars import coalesce

_vector_lock = None
_history_lock = None
_queue_lock = None
_fog_lock = None


def _set_vector_lock() -> None:
    global _vector_lock
    _vector_lock = Lock()


def _get_vector_lock():
    global _vector_lock
    return _vector_lock


def _set_history_lock() -> None:
    global _history_lock
    _history_lock = Lock()


def _get_history_lock():
    global _history_lock
    return _history_lock


def _set_queue_lock() -> None:
    global _queue_lock
    _queue_lock = Lock()


def _get_queue_lock():
    global _queue_lock
    return _queue_lock

def _set_fog_lock() -> None:
    global _fog_lock
    _fog_lock = Lock()


def _get_fog_lock():
    global _fog_lock
    return _fog_lock


class CloudService(Service):
    local_neighbors: list = None
    global_neighbors: list = None
    vector_clock = None
    history: list = None
    queue: deque = None
    op_assocs: dict = None
    fog_clocks: dict = None
    fog_contacts: dict = None
    coordinator: ActorHandle

#======================================================================================================================

    def __init__(self, service_id: str, local_neighbors):
        super().__init__(service_id, store_step=True)
        self.i = 0
        self.local_neighbors = local_neighbors

    async def step(self):
        if self.i == 0:
            await self._first_step()
        while True:
            # self.i += 1
            await self._empty_queue()
            await asyncio.sleep(vars.FOG_QUEUE_TIMER)
            if await self.coordinator.is_end.remote():
                break
        return 1

    async def _first_step(self):
        self.vector_clock = {}
        self.history = []
        self.queue = deque()
        self.op_assocs = {}
        self.fog_clocks = {}
        self.fog_contacts = {}
        _set_vector_lock()
        _set_queue_lock()
        _set_history_lock()
        _set_fog_lock()

        try:
            self.coordinator = coordinator.ServiceCoordinator.options(name=vars.COORDINATOR_NAME,
                                                                      namespace=vars.COORDINATOR_NAMESPACE).remote(
                user_count=vars.USER_COUNT)
        except:
            self.coordinator = ray.get_actor(vars.COORDINATOR_NAME, namespace=vars.COORDINATOR_NAMESPACE)

        node_neighbors = await self.mpi.get_neighbors()
        self.global_neighbors = []
        for neighbor in node_neighbors:
            if "cloud" in neighbor:
                self.global_neighbors.append(neighbor)
        asyncio.create_task(self._message_listener())

# ======================================================================================================================

    async def _message_listener(self):
        while True:
            try:
                msg = await self.mpi.recv()
                if msg:
                    asyncio.create_task(self._recv_msg(msg))
            except asyncio.CancelledError:
                break

    async def _send_msg(self, msg_type: int, body, recipients: list):
        msg = {vars.MESSAGE_BODY: body, vars.MESSAGE_TYPE: msg_type}
        # self.logger.info(msg)
        # self.logger.info(recipients)

        confirm = self.mpi.send(recipients, msg)
        if asyncio.iscoroutine(confirm):
            await confirm

    async def _recv_msg(self, msg):
        try:
            sender = msg["sender_id"]
            msg_type = msg[vars.MESSAGE_TYPE]
            body = msg[vars.MESSAGE_BODY]

            # self.logger.info(msg)

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
            with _get_fog_lock():
                with _get_history_lock():
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
                with _get_fog_lock():
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

        with _get_fog_lock():
            if address != "":
                self.fog_contacts[cluster_id] = address
            else:
                del self.fog_clocks[cluster_id]
                del self.fog_contacts[cluster_id]


    async def _handle_create_cluster(self, body: dict):
        address: str = body[vars.ID]
        cluster_id = fogService.get_fog_id(address)
        with _get_fog_lock():
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
        with _get_vector_lock():
            self.op_assocs[op[vars.ID]] = {
                vars.OPERATION: op,
                vars.ID: session_id,
                vars.VECTOR_CLOCK: copy.deepcopy(self.vector_clock)
            }
            self.vector_clock[session_id] = vars.coalesce(self.vector_clock.get(session_id), 0) + 1
        with _get_history_lock():
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
        with _get_queue_lock():
            self.queue.append(body)

        with _get_vector_lock():
            self.op_assocs[op[vars.ID]] = {
                vars.OPERATION: op,
                vars.ID: session_id,
                vars.VECTOR_CLOCK: copy.deepcopy(self.vector_clock)
            }
            self.vector_clock[session_id] = vars.coalesce(self.vector_clock.get(session_id), 0) + 1
        with _get_fog_lock():
            self.fog_clocks[fog_id][session_id] = vars.coalesce(self.fog_clocks[fog_id].get(session_id), 0) + 1

        with _get_history_lock():
            self.history.append(op[vars.ID])

    async def _handle_user_task(self, body: dict):

        session_id: str = body[vars.ID]
        op: dict = body[vars.OPERATION]
        req_clock: dict = body[vars.VECTOR_CLOCK]
        network_range: str = body[vars.NETWORK_RANGE]

        await self._wait_for_req_clock(req_clock)
        self._perform_operation(op)
        response_body = {
            vars.OPERATION: op,
            vars.RESULT: True
        }
        if vars.is_write(op):
            with _get_vector_lock():
                self.op_assocs[op[vars.ID]] = {
                    vars.OPERATION: op,
                    vars.ID: session_id,
                    vars.VECTOR_CLOCK: copy.deepcopy(self.vector_clock)
                }
                self.vector_clock[session_id] = vars.coalesce(self.vector_clock.get(session_id), 0) + 1
            with _get_history_lock():
                self.history.append(op[vars.ID])
            with _get_queue_lock():
                self.queue.append(body)
        else:
            with _get_vector_lock():
                response_body[vars.VECTOR_CLOCK] = copy.deepcopy(self.vector_clock)
        await self._send_msg(vars.TASK_CONFIRM, response_body, [vars.get_addr_from_session_id(session_id)])

# ======================================================================================================================

    async def _empty_queue(self):
        queue_copy: deque
        with _get_queue_lock():
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
        with _get_vector_lock():
            for key in req_clock:
                if vars.coalesce(self.vector_clock.get(key), 0) < req_clock.get(key):
                    return False
            return True
