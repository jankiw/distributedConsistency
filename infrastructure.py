from math import sqrt, ceil

from eclypse.graph import Infrastructure, Application
from eclypse.graph.assets.defaults import cpu, ram, latency, bandwidth, availability, storage, gpu

from my_model.cloudService import CloudService
from my_model.fogService import FogService
from my_model.userService import UserService
import vars


class MyInfrastructure:
    infrastructure: Infrastructure
    application: Application
    cloud_count: int
    fog_cluster_count: int
    fog_cluster_counts: list
    fog_node_cloud_parents: list
    user_count: int
    model_type: str
    local_groups: list
    local_group_cloud_ids: list

    def generate_new_infrastructure(self, model_type: str, infrastructure_type: int) -> None:
        self.model_type = model_type

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

        self.local_groups = []
        self.local_group_cloud_ids = []

        match infrastructure_type:
            case vars.INFRASTRUCTURE_1:
                self.create_infrastructure_1()


    def create_infrastructure_1(self):

        self.create_region([
            [2, 4],
            [2]
        ])
        self.create_region([
            [3],
            []
        ])
        self.create_region([
            [],
            [6]
        ])
        self.create_region([
            [3],
            [2]
        ])

        self.create_region_adjacency()

        for i in range(vars.USER_COUNT):
            self.add_user_node()



    def create_region_adjacency(self):
        region_adjacency = []

        for i in range(len(self.local_group_cloud_ids)):
            region_adjacency.append([])
            for j in range(len(self.local_group_cloud_ids)):
                diff = abs(j - i)
                diff = min(diff, len(self.local_group_cloud_ids) - diff)
                diff = ceil(sqrt(diff))
                if diff == 0:
                    diff = 1
                region_adjacency[i].append(diff)

        print(region_adjacency)
        self.add_cloud_edges(region_adjacency)

    def create_region(self, cluster_numbers):
        group = []
        start_cloud_num: int = self.cloud_count
        for i in cluster_numbers:
            group.append(start_cloud_num)
            start_cloud_num += 1

        full_group = []

        for i in cluster_numbers:
            self.create_cloud_with_clusters(i, full_group, group)

        self.local_groups.append(full_group)
        self.local_group_cloud_ids.append(group)

    def create_cloud_with_clusters(self, cluster_numbers: list, full_group, group):
        full_group.append(self.add_cloud_node(self.get_local_neighbours(group, self.cloud_count)))
        for i in cluster_numbers:
            full_group.append(self.add_fog_cluster())
            for j in range(i - 1):
                full_group.append(self.add_fog_node())

    def get_infrastructure(self) -> Infrastructure:
        return self.infrastructure

    def get_application(self) -> Application:
        return self.application

    def add_fog_cluster(self) -> str:
        cloud_num = self.cloud_count - 1
        self.fog_cluster_counts.append(0)
        self.fog_node_cloud_parents.append(cloud_num)

        self.fog_cluster_count += 1

        result = self.add_fog_node()

        return result

    def add_cloud_node(self, local_group_neighbors) -> str:
        cloud_name: str = self.get_cloud_node_name(self.cloud_count)

        self.infrastructure.add_node(cloud_name, cpu=4.0, ram=8.0, availability=1.0, storage=1.0, gpu=1.0)

        match self.model_type:
            case vars.MY_MODEL:
                self.application.add_service(CloudService(cloud_name, local_group_neighbors), cpu=1.0, ram=1.0, availability=1.0, storage=1.0, gpu=1.0)

        # for i in range(cloud_num):
        #     self.add_cloud_edge(cloud_num, i)
        self.cloud_count += 1

        return cloud_name

    def add_fog_node(self) -> str:
        cluster_num: int = self.fog_cluster_count - 1
        fog_num: int = self.fog_cluster_counts[cluster_num]
        fog_name: str = self.get_fog_node_name(cluster_num, fog_num)
        cloud_num: int = self.fog_node_cloud_parents[cluster_num]
        self.infrastructure.add_node(fog_name, cpu=4.0, ram=8.0, availability=1.0, storage=1.0, gpu=1.0)

        match self.model_type:
            case vars.MY_MODEL:
                self.application.add_service(FogService(fog_name), cpu=1.0, ram=1.0, availability=1.0, storage=1.0, gpu=1.0)

        for i in range(fog_num):
            self.add_fog_edge(cluster_num, fog_num, i)
        self.add_mixed_edge(cluster_num, fog_num, cloud_num)

        self.fog_cluster_counts[cluster_num] += 1

        return fog_name

    def add_user_node(self) -> str:
        user_num: int = self.user_count
        user_name: str = self.get_user_node_name(user_num)

        self.infrastructure.add_node(user_name, cpu=4.0, ram=8.0, availability=1.0, storage=1.0, gpu=1.0)

        match self.model_type:
            case vars.MY_MODEL:
                self.application.add_service(UserService(user_name, self.local_groups), cpu=1.0, ram=1.0, availability=1.0, storage=1.0, gpu=1.0)

        for i in range(self.cloud_count):
            self.add_user_cloud_edge(user_num, self.get_cloud_node_name(i))

        for i in range(self.fog_cluster_count):
            for j in range(self.fog_cluster_counts[i]):
                self.add_user_fog_edge(user_num, self.get_fog_node_name(i, j))
        self.user_count += 1

        return user_name

    def add_cloud_edges(self, group_adjacency):
        for i in range(len(self.local_group_cloud_ids)):
            for a in self.local_group_cloud_ids[i]:
                for b in self.local_group_cloud_ids[i]:
                    if b > a:
                        self.add_cloud_edge(a, b, group_adjacency[i][i])

        for i in range(len(self.local_group_cloud_ids)):
            for j in range(len(self.local_group_cloud_ids)):
                if j > i:
                    for a in self.local_group_cloud_ids[i]:
                        for b in self.local_group_cloud_ids[j]:
                            self.add_cloud_edge(a, b, group_adjacency[i][j])



    def add_fog_edge(self, cluster_num: int, fog_num_1: int, fog_num_2: int) -> None:
        self.infrastructure.add_edge(
            self.get_fog_node_name(cluster_num, fog_num_1), self.get_fog_node_name(cluster_num, fog_num_2),
            latency=vars.FOG_FOG_LATENCY,
            bandwidth=100.0,
            symmetric=True
        )
        self.application.add_edge(
            self.get_fog_node_name(cluster_num, fog_num_1), self.get_fog_node_name(cluster_num, fog_num_2),
            latency=vars.FOG_FOG_LATENCY,
            bandwidth=100.0,
            symmetric=True
        )

    def add_mixed_edge(self, cluster_num: int, fog_num: int, cloud_num: int) -> None:
        self.infrastructure.add_edge(
            self.get_fog_node_name(cluster_num, fog_num), self.get_cloud_node_name(cloud_num),
            latency=vars.FOG_CLOUD_LATENCY,
            bandwidth=100.0,
            symmetric=True
        )
        self.application.add_edge(
            self.get_fog_node_name(cluster_num, fog_num), self.get_cloud_node_name(cloud_num),
            latency=vars.FOG_CLOUD_LATENCY,
            bandwidth=100.0,
            symmetric=True
        )

    def add_cloud_edge(self, cloud_num_1: int, cloud_num_2: int, adjacency: float) -> None:
        self.infrastructure.add_edge(
            self.get_cloud_node_name(cloud_num_1), self.get_cloud_node_name(cloud_num_2),
            latency= adjacency * vars.CLOUD_CLOUD_LATENCY,
            bandwidth=100.0,
            symmetric=True
        )
        self.application.add_edge(
            self.get_cloud_node_name(cloud_num_1), self.get_cloud_node_name(cloud_num_2),
            latency=adjacency * vars.CLOUD_CLOUD_LATENCY,
            bandwidth=100.0,
            symmetric=True
        )

    def add_user_fog_edge(self,user_id: int, other_node: str) -> None:
        self.infrastructure.add_edge(
            self.get_user_node_name(user_id), other_node,
            latency=vars.USER_FOG_LATENCY,
            bandwidth=100.0,
            symmetric=True
        )
        self.application.add_edge(
            self.get_user_node_name(user_id), other_node,
            latency=vars.USER_FOG_LATENCY,
            bandwidth=100.0,
            symmetric=True
        )

    def add_user_cloud_edge(self,user_id: int, other_node: str) -> None:
        self.infrastructure.add_edge(
            self.get_user_node_name(user_id), other_node,
            latency=vars.USER_CLOUD_LATENCY,
            bandwidth=100.0,
            symmetric=True
        )
        self.application.add_edge(
            self.get_user_node_name(user_id), other_node,
            latency=vars.USER_CLOUD_LATENCY,
            bandwidth=100.0,
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

    @staticmethod
    def get_local_neighbours(neighbour_list, own_id) -> list:
        result = []
        for neighbour in neighbour_list:
            if neighbour == own_id:
                continue
            result.append(MyInfrastructure.get_cloud_node_name(neighbour))
        return result

