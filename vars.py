from pickle import GLOBAL

RAFT_MSG = 1
USER_TASK = 2
FOG_TASK = 3
CLOUD_TASK = 4
TASK_CONFIRM = 5
TASK_REQUEST = 6
LEADER_CHANGE = 7
CREATE_CLUSTER = 8

ID = "id"
OPERATION = "op"
NETWORK_RANGE = "range"
WRITE_VECTOR_CLOCK = "write_clock"
VECTOR_CLOCK = "clock"
RESULT = "res"
MESSAGE_BODY = "body"
MESSAGE_TYPE = "type"
TYPE = "type"
VALUE = "value"
FOG_ID = "fog_id"

WRITE_OP = "w"
READ_OP = "r"
SEPARATOR = "-"

LOCAL_RANGE = "local"
GLOBAL_RANGE = "global"

MONOTONIC_WRITES = "mw"
MONOTONIC_READS = "mr"
WRITES_FOLLOW_READS = "wfr"
READ_YOUR_WRITES = "ryw"

USER_COUNT = 4
FOG_QUEUE_TIMER = 1
CLOUD_QUEUE_TIMER = 1

COORDINATOR_NAME = "service_coordinator"
COORDINATOR_NAMESPACE = "g"

def is_write(op: dict) -> bool:
    if op[TYPE] == WRITE_OP:
        return True
    return False

def coalesce(var, value):
    if var is None:
        return value

    return var

def get_addr_from_session_id(op_id) -> str:
    return op_id.split('_')[0]