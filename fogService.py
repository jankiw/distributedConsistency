import asyncio
import copy
import os
from collections import deque

os.environ["RAY_DEDUP_LOGS"] = "0"
import ray
from ray.actor import ActorHandle

import coordinator
import vars

from eclypse.remote.service import Service
from multiprocessing import Lock

from rraft import Config, MemStorage, RawNode, ConfState, default_logger, InMemoryRawNode, Ready
from rraft.rraft import Message

_raft_node: InMemoryRawNode = None
_raft_lock = None
_vector_lock = None
_history_lock = None
_queue_lock = None

def _set_raft_node(value) -> None:
    global _raft_node
    _raft_node = value

def _get_raft_node() -> InMemoryRawNode:
    global _raft_node
    return _raft_node

def _set_raft_lock() -> None:
    global _raft_lock
    _raft_lock = Lock()

def _get_raft_lock():
    global _raft_lock
    return _raft_lock

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

#service node can be 0 but raft_node does not accept id = 0
def _get_id_number(node_id) -> int:
    return int(node_id.split('-')[2]) + 1

#service node can be 0 but raft_node does not accept id = 0
def _get_string_number(edge_id: str, raft_id: int) -> str:
    split_id = edge_id.split('-')
    split_id[2] = str(raft_id - 1)
    return "-".join(split_id)

def get_fog_id(node_id) -> int:
    return int(node_id.split('-')[1])


class FogService(Service):
    i = None
    cloud_node = None
    edge_neighbors = None
    vector_clock = None
    raft_leader: str = None
    history: list = None
    queue: deque = None
    op_assocs: dict = None
    coordinator: ActorHandle

# ======================================================================================================================

    def __init__(self, service_id: str):
        super().__init__(service_id, store_step=True)
        self.i = 0

    async def step(self):
        if self.i == 0:
            await self._first_step()
        while True:
            #self.i += 1

            await self._empty_queue()
            await asyncio.sleep(vars.FOG_QUEUE_TIMER)
            if await self.coordinator.is_end.remote():
                break
        return 1

    async def _first_step(self):
        self.edge_neighbors = []
        self.vector_clock = {}
        self.history = []
        self.queue = deque()
        self.op_assocs = {}
        _set_queue_lock()
        _set_history_lock()
        _set_vector_lock()

        try:
            self.coordinator = coordinator.ServiceCoordinator.options(name=vars.COORDINATOR_NAME, namespace = vars.COORDINATOR_NAMESPACE).remote(user_count=vars.USER_COUNT)
        except:
            self.coordinator = ray.get_actor(vars.COORDINATOR_NAME, namespace = vars.COORDINATOR_NAMESPACE)

        node_neighbors = await self.mpi.get_neighbors()
        for neighbor in node_neighbors:
            if "edge" in neighbor:
                self.edge_neighbors.append(neighbor)
            elif "cloud" in neighbor:
                self.cloud_node = neighbor
        await self._create_raft_node()
        asyncio.create_task(self._message_listener())


    async def _create_raft_node(self):
        logger = default_logger()
        voters: list[int] = [_get_id_number(self.id)]
        for neighbor in self.edge_neighbors:
            voters.append(_get_id_number(neighbor))
        raft_storage = MemStorage.new_with_conf_state(ConfState(voters=voters, learners=[]))
        raft_config = Config(id=_get_id_number(self.id), election_tick=10, heartbeat_tick=3)
        raft_node = InMemoryRawNode(raft_config, raft_storage, logger)

        split_id: list = self.id.split('-')
        split_id[2] = '0'
        self.raft_leader = '-'.join(split_id)

        if _get_id_number(self.id) == 1:
            raft_node.campaign()
            await self._send_msg(
                vars.CREATE_CLUSTER,
                {vars.ID: self.id},
                [self.cloud_node]
            )
        _set_raft_node(raft_node)
        _set_raft_lock()
        asyncio.create_task(self._run_raft())

# ======================================================================================================================

    async def _run_raft(self):
        while True:
            try:
                with _get_raft_lock():
                    _get_raft_node().tick()
                    if _get_raft_node().has_ready():
                        ready: Ready = _get_raft_node().ready()
                        messages = ready.take_messages()
                        messages += ready.persisted_messages()
                        for message in messages:
                            recipients = [_get_string_number(self.id, message.get_to())]
                            await self._send_msg(vars.RAFT_MSG, message.encode(), recipients)
                        _get_raft_node().advance(ready.make_ref())
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break

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
        # if msg[vars.MESSAGE_TYPE] != vars.RAFT_MSG:
        #     self.logger.info(msg)
        #     self.logger.info(recipients)

        confirm = self.mpi.send(recipients, msg)
        if asyncio.iscoroutine(confirm):
            await confirm

    async def _recv_msg(self, msg):
        try:
            sender = msg["sender_id"]
            msg_type = msg[vars.MESSAGE_TYPE]
            body = msg[vars.MESSAGE_BODY]

            # if msg[vars.MESSAGE_TYPE] != vars.RAFT_MSG:
            #     self.logger.info(msg)

            match msg_type:
                case vars.RAFT_MSG:
                    body = Message.decode(body)
                    with _get_raft_lock():
                        _get_raft_node().step(body)

                case vars.USER_TASK:
                    await self._handle_user_task(body)

                case vars.CLOUD_TASK:
                    await self._handle_cloud_task(body)

                # case vars.CREATE_CLUSTER:


        except asyncio.CancelledError:
            return

# ======================================================================================================================

    async def _handle_cloud_task(self, body: dict):
        if self.raft_leader != self.id:
            await self._send_msg(
                vars.USER_TASK,
                body,
                [self.raft_leader]
            )
            return

        user_id: str = body[vars.ID]
        op: dict = body[vars.OPERATION]
        req_clock: dict = body[vars.VECTOR_CLOCK]

        await self._wait_for_req_clock(req_clock)
        self._perform_operation(op)

        with _get_vector_lock():
            self.op_assocs[op[vars.ID]] = {
                vars.OPERATION: op,
                vars.ID: user_id,
                vars.VECTOR_CLOCK: copy.deepcopy(self.vector_clock)
            }
            self.vector_clock[user_id] = vars.coalesce(self.vector_clock.get(user_id), 0) + 1
        with _get_history_lock():
            self.history.append(op[vars.ID])

    async def _handle_user_task(self, body: dict):
        if self.raft_leader != self.id:
            await self._send_msg(
                vars.USER_TASK,
                body,
                [self.raft_leader]
            )
            return

        user_id: str = body[vars.ID]
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
                    vars.ID: user_id,
                    vars.VECTOR_CLOCK: copy.deepcopy(self.vector_clock)
                }
                self.vector_clock[user_id] = vars.coalesce(self.vector_clock.get(user_id), 0) + 1
            with _get_history_lock():
                self.history.append(op[vars.ID])
            with _get_queue_lock():
                self.queue.append(body)
        else:
            with _get_vector_lock():
                response_body[vars.VECTOR_CLOCK] = copy.deepcopy(self.vector_clock)
        await self._send_msg(vars.TASK_CONFIRM, response_body, [user_id])

# ======================================================================================================================

    def _perform_operation(self, op: dict):
        # with _get_raft_lock():
        #     _get_raft_node().propose([], )
        # self.logger.info(str(op[vars.ID]) + " performed on node " + self.id)
        a = 1

    async def _wait_for_req_clock(self, req_clock: dict):
        if not self._check_req_clock(req_clock):
            await self._send_msg(
                vars.TASK_REQUEST,
                {
                    vars.FOG_ID: get_fog_id(self.id),
                    vars.VECTOR_CLOCK: req_clock
                },
                [self.cloud_node]
            )
            while not self._check_req_clock(req_clock):
                await asyncio.sleep(0.05)

    def _check_req_clock(self, req_clock: dict) -> bool:
        with _get_vector_lock():
            for key in req_clock:
                if vars.coalesce(self.vector_clock.get(key), 0) < req_clock.get(key):
                    return False
            return True


    async def _empty_queue(self):
        queue_copy: deque
        with _get_queue_lock():
            queue_copy = copy.deepcopy(self.queue)
            self.queue.clear()

        for i in range(len(queue_copy)):
            body = queue_copy.pop()
            body[vars.FOG_ID] = get_fog_id(self.id)
            await  self._send_msg(
                vars.FOG_TASK,
                body,
                [self.cloud_node]
            )