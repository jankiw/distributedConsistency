import os

os.environ["RAY_DEDUP_LOGS"] = "0"
import ray

@ray.remote
class ServiceCoordinator:
    end: bool
    user_count: int

    def __init__(self, user_count):
        self.end = False
        self.user_count = user_count

    def user_finish(self):
        self.user_count -= 1
        if self.user_count <= 0:
            self.end = True

    def is_end(self):
        return self.end