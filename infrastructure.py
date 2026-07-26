from cmath import inf

from eclypse.graph import Infrastructure, Application
from eclypse.graph.assets.defaults import cpu, ram, latency, bandwidth, availability, storage, gpu

from cloudService import CloudService
from fogService import FogService
from userService import UserService


class MyInfrastructure:
    infrastructure: Infrastructure
    application: Application
    cloud_count: int
    fog_cluster_count: int
    fog_cluster_counts: list
    fog_node_cloud_parents: list
    user_count: int

    def generate_new_infrastructure(self) -> None:
        self.infrastructure = Infrastructure(
            infrastructure_id="my-infra",
            node_assets={"cpu": cpu(), "ram": ram(), "availability": availability(), "storage": storage(), "gpu": gpu()},
            edge_assets={"latency": latency(), "bandwidth": bandwidth()},
        )

        self.application = Application(
            application_id="app",
            node_assets={"cpu": cpu(), "ram": ram(), "availability": availability(), "storage": storage(), "gpu": gpu()},
            edge_assets={"latency": latency(), "bandwidth": bandwidth()}
        )

        self.cloud_count = 0
        self.fog_cluster_count = 0
        self.fog_cluster_counts = []
        self.fog_node_cloud_parents = []
        self.user_count = 0

        self.add_cloud_node()
        self.add_cloud_node()
        self.add_fog_cluster(0)
        self.add_fog_cluster(0)
        self.add_fog_cluster(1)
        self.add_fog_node(1)
        self.add_fog_node(1)
        self.add_fog_node(1)

        self.add_user_node()
        self.add_user_node()
        self.add_user_node()
        self.add_user_node()


    def get_infrastructure(self) -> Infrastructure:
        return self.infrastructure

    def get_application(self) -> Application:
        return self.application

    def add_fog_cluster(self, cloud_num: int) -> None:
        self.fog_cluster_counts.append(0)
        self.fog_node_cloud_parents.append(cloud_num)

        self.add_fog_node(self.fog_cluster_count)

        self.fog_cluster_count += 1

    def add_cloud_node(self) -> None:
        cloud_num: int = self.cloud_count
        cloud_name: str = self.get_cloud_node_name(cloud_num)

        self.infrastructure.add_node(cloud_name, cpu=4.0, ram=8.0, availability=1.0, storage=1.0, gpu=1.0)
        self.application.add_service(CloudService(cloud_name), cpu=1.0, ram=1.0, availability=1.0, storage=1.0, gpu=1.0)
        for i in range(cloud_num):
            self.add_cloud_edge(cloud_num, i)
        self.cloud_count += 1

    def add_fog_node(self, cluster_num: int) -> None:
        fog_num: int = self.fog_cluster_counts[cluster_num]
        fog_name: str = self.get_fog_node_name(cluster_num, fog_num)
        cloud_num: int = self.fog_node_cloud_parents[cluster_num]

        self.infrastructure.add_node(fog_name, cpu=4.0, ram=8.0, availability=1.0, storage=1.0, gpu=1.0)
        self.application.add_service(FogService(fog_name), cpu=1.0, ram=1.0, availability=1.0, storage=1.0, gpu=1.0)
        for i in range(fog_num):
            self.add_fog_edge(cluster_num, fog_num, i)
        self.add_mixed_edge(cluster_num, fog_num, cloud_num)

        self.fog_cluster_counts[cluster_num] += 1

    def add_user_node(self) -> None:
        user_num: int = self.user_count
        user_name: str = self.get_user_node_name(user_num)

        self.infrastructure.add_node(user_name, cpu=4.0, ram=8.0, availability=1.0, storage=1.0, gpu=1.0)
        self.application.add_service(UserService(user_name), cpu=1.0, ram=1.0, availability=1.0, storage=1.0, gpu=1.0)

        for i in range(self.cloud_count):
            self.add_user_edge(user_num, self.get_cloud_node_name(i))
        for i in range(self.fog_cluster_count):
            for j in range(self.fog_cluster_counts[i]):
                self.add_user_edge(user_num, self.get_fog_node_name(i, j))
        self.user_count += 1

    def add_fog_edge(self, cluster_num: int, fog_num_1: int, fog_num_2: int) -> None:
        self.infrastructure.add_edge(
            self.get_fog_node_name(cluster_num, fog_num_1), self.get_fog_node_name(cluster_num, fog_num_2),
            latency=10.0,
            bandwidth=100.0,
            symmetric=True
        )
        self.application.add_edge(
            self.get_fog_node_name(cluster_num, fog_num_1), self.get_fog_node_name(cluster_num, fog_num_2),
            latency=15.0,
            bandwidth=10.0,
            symmetric=True
        )

    def add_mixed_edge(self, cluster_num: int, fog_num: int, cloud_num: int) -> None:
        self.infrastructure.add_edge(
            self.get_fog_node_name(cluster_num, fog_num), self.get_cloud_node_name(cloud_num),
            latency=10.0,
            bandwidth=100.0,
            symmetric=True
        )
        self.application.add_edge(
            self.get_fog_node_name(cluster_num, fog_num), self.get_cloud_node_name(cloud_num),
            latency=15.0,
            bandwidth=10.0,
            symmetric=True
        )

    def add_cloud_edge(self, cloud_num_1: int, cloud_num_2: int) -> None:
        self.infrastructure.add_edge(
            self.get_cloud_node_name(cloud_num_1), self.get_cloud_node_name(cloud_num_2),
            latency=10.0,
            bandwidth=100.0,
            symmetric=True
        )
        self.application.add_edge(
            self.get_cloud_node_name(cloud_num_1), self.get_cloud_node_name(cloud_num_2),
            latency=15.0,
            bandwidth=10.0,
            symmetric=True
        )

    def add_user_edge(self,user_id: int, other_node: str) -> None:
        self.infrastructure.add_edge(
            self.get_user_node_name(user_id), other_node,
            latency=10.0,
            bandwidth=100.0,
            symmetric=True
        )
        self.application.add_edge(
            self.get_user_node_name(user_id), other_node,
            latency=15.0,
            bandwidth=10.0,
            symmetric=True
        )

    @staticmethod
    def get_cloud_node_name(num: int) -> str:
        return "cloud-" + str(num)

    @staticmethod
    def get_fog_node_name(cluster: int, fog: int) -> str:
        return "edge-" + str(cluster) + "-" + str(fog)

    @staticmethod
    def get_user_node_name(num: int) -> str:
        return "user-" + str(num)

