import asyncio
import copy
import json
import os
import time
from asyncio import Task
from collections import deque
from multiprocessing.connection import Connection, wait

from my_model import coordinator


import vars

from eclypse.remote.service import Service
import multiprocessing as mp
from multiprocessing import Lock

from rraft import Config, MemStorage, ConfState, default_logger, InMemoryRawNode, Ready, EntryType
from rraft.rraft import Message

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


class FogService:
    i = None
    cloud_node = None
    edge_neighbors = None
    vector_clock = None
    raft_leader: str = None
    history: list = None
    queue: deque = None
    op_assocs: dict = None
    coordinator: Connection
    raft_node: InMemoryRawNode
    raft_storage: MemStorage
    raft_lock = None
    vector_lock = None
    history_lock = None
    queue_lock = None

    id: str
    neighbours: dict

# ======================================================================================================================

    def __init__(self, id: str, connection: Connection):
        self.id = id
        self.neighbours = {}
        self.i = 0
        self.coordinator = connection

        self.edge_neighbors = []
        self.vector_clock = {}
        self.history = []
        self.queue = deque()
        self.op_assocs = {}
        self.queue_lock = Lock()
        self.history_lock = Lock()
        self.vector_lock = Lock()
        self.raft_lock = Lock()


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
            #self.i += 1
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

        for neighbor in self.neighbours:
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
        raft_config = Config(id=_get_id_number(self.id), election_tick=100000, heartbeat_tick=10)
        raft_node = InMemoryRawNode(raft_config, raft_storage, logger)

        split_id: list = self.id.split('-')
        split_id[2] = '0'
        self.raft_leader = '-'.join(split_id)

        if _get_id_number(self.id) == 1:
            raft_node.campaign()
            self.log(self.id)
            await self._send_msg(
                vars.CREATE_CLUSTER,
                {vars.ID: self.id},
                [self.cloud_node]
            )
        self.raft_node = raft_node
        self.raft_storage = raft_storage
        asyncio.create_task(self._run_raft())

# ======================================================================================================================

    async def _run_raft(self):
        while True:
            try:
                with self.raft_lock:
                    self.raft_node.tick()
                    if self.raft_node.has_ready():

                        ready: Ready = self.raft_node.ready()

                        entries = ready.take_entries()

                        if len(entries) > 0:
                            #self.logger.info(entries)
                            self.raft_storage.wl().append(entries)

                        hardstate = ready.hs()
                        if hardstate:
                            self.raft_storage.wl().set_hardstate(hardstate)

                        snapshot = ready.snapshot()
                        if snapshot:
                            self.raft_storage.wl().apply_snapshot(snapshot)

                        messages = ready.take_messages()
                        messages += ready.take_persisted_messages()
                        for message in messages:
                            recipients = [_get_string_number(self.id, message.get_to())]
                            asyncio.create_task(self._send_msg(vars.RAFT_MSG, message.encode(), recipients))

                        committed_entries = ready.take_committed_entries()
                        for entry in committed_entries:
                            if entry.get_entry_type() == EntryType.EntryNormal and entry.get_data():
                                msg = json.loads(entry.get_data().decode())
                                asyncio.create_task(self._raft_handle_task(msg))

                        self.raft_node.advance(ready.make_ref())

            except asyncio.CancelledError:
                break

            await asyncio.sleep(0.01)

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
        if self.id == "edge-3-0" and msg_type != vars.RAFT_MSG:
            self.log("sent" + str(msg))

        # if msg[vars.MESSAGE_TYPE] != vars.RAFT_MSG:
        #     self.log("sending "+str(msg) + " to " + str(recipients))

        for recipient in recipients:
            self.neighbours[recipient]["connection"].send(msg)

    async def _recv_msg(self, msg):
        try:
            msg_type = msg[vars.MESSAGE_TYPE]
            body = msg[vars.MESSAGE_BODY]

            if self.id == "edge-3-0" and msg_type != vars.RAFT_MSG:
                self.log("rec" + str(msg))

            # if msg[vars.MESSAGE_TYPE] != vars.RAFT_MSG:
            #     self.log("received " + str(msg))

            match msg_type:
                case vars.RAFT_MSG:
                    body = Message.decode(body)
                    with self.raft_lock:
                        self.raft_node.step(body)

                case vars.USER_TASK:
                    asyncio.create_task(self._handle_user_task(msg))

                case vars.CLOUD_TASK:
                    asyncio.create_task(self._handle_cloud_task(msg))

                # case vars.CREATE_CLUSTER:


        except asyncio.CancelledError:
            return

    async def _raft_handle_task(self, msg):
        msg_type = msg[vars.MESSAGE_TYPE]
        body = msg[vars.MESSAGE_BODY]
        if self.id == "edge-3-0":
            self.log(str(msg))

        match msg_type:

            case vars.USER_TASK:
                asyncio.create_task(self._raft_handle_user_task(body))

            case vars.CLOUD_TASK:
                asyncio.create_task(self._raft_handle_cloud_task(body))


# ======================================================================================================================

    async def _handle_cloud_task(self, msg: dict):
        body = msg[vars.MESSAGE_BODY]
        while True:
            with self.raft_lock:
                leader_id = self.raft_node.get_raft().get_leader_id()
                if leader_id != 0:
                    leader_node = _get_string_number(self.id, leader_id)

                    if leader_node != self.id:
                        asyncio.create_task(self._send_msg(
                            vars.CLOUD_TASK,
                            body,
                            [leader_node]
                        ))
                        return
                    raft_msg: bytes = json.dumps(msg).encode()
                    self.raft_node.propose([], raft_msg)
                    return

            await asyncio.sleep(0.05)


    async def _handle_user_task(self, msg: dict):
        body = msg[vars.MESSAGE_BODY]
        op: dict = body[vars.OPERATION]
        #self.logger.info(time.time() - body["time"])
        if vars.is_write(op):
            while True:
                with self.raft_lock:
                    leader_id = self.raft_node.get_raft().get_leader_id()
                    if leader_id != 0:
                        leader_node = _get_string_number(self.id, leader_id)

                        if leader_node != self.id:
                            asyncio.create_task(self._send_msg(
                                vars.USER_TASK,
                                body,
                                [leader_node]
                            ))
                            return
                        # self.logger.info(_get_raft_node().get_raft().get_state())
                        raft_msg: bytes = json.dumps(msg).encode()
                        self.raft_node.propose([], raft_msg)
                        return

                await asyncio.sleep(0.01)

        else:
            asyncio.create_task(self._raft_handle_user_task(body))


    #in both places it is used with raft lock already in use
    async def _raft_handle_user_task(self, data):
        session_id: str = data[vars.ID]
        op: dict = data[vars.OPERATION]
        req_clock: dict = data[vars.VECTOR_CLOCK]
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
                self.queue.append(data)
        else:
            with self.vector_lock:
                response_body[vars.VECTOR_CLOCK] = copy.deepcopy(self.vector_clock)
        with self.raft_lock:
            if (not vars.is_write(op)) or (self.raft_leader == self.id):
                asyncio.create_task(self._send_msg(vars.TASK_CONFIRM, response_body, [vars.get_addr_from_session_id(session_id)]))

    async def _raft_handle_cloud_task(self, data):
        session_id: str = data[vars.ID]
        op: dict = data[vars.OPERATION]
        req_clock: dict = data[vars.VECTOR_CLOCK]
        # self.log(str(req_clock))
        # with self.vector_lock:
        #     self.log(str(self.vector_clock))
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

# ======================================================================================================================

    def _perform_operation(self, op: dict):
        # self.logger.info(str(op[vars.ID]) + " performed on node " + self.id)
        pass

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
                await asyncio.sleep(vars.CLOCK_WAIT_TIME)

    def _check_req_clock(self, req_clock: dict) -> bool:
        with self.vector_lock:
            for key in req_clock:
                if vars.coalesce(self.vector_clock.get(key), 0) < req_clock.get(key):
                    return False
            return True


    async def _empty_queue(self):
        queue_copy: deque
        with self.queue_lock:
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

    def log(self, msg: str):
        print(self.id + ": " + msg, flush=True)