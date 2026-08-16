import os

os.environ["RAY_DEDUP_LOGS"] = "0"
import ray

@ray.remote
class ServiceCoordinator:
    end: bool
    user_count: int

    data: dict

    def __init__(self, user_count):
        self.end = False
        self.user_count = user_count
        self.data = {
            "op_count": 0,
            "total_time": 0.0
        }

    def user_finish(self):
        self.user_count -= 1
        if self.user_count <= 0:
            self.end = True

    def is_end(self):
        return self.end

    def post_data(self, dict):
        self.data["op_count"] += dict["op_count"]
        self.data["total_time"] += dict["total_time"]

    def get_data(self) -> dict:
        self.data["avg_time"] = self.data["total_time"] / self.data["op_count"]
        return self.data