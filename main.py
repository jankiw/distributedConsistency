import asyncio

from eclypse.simulation import Simulation

from my_model.coordinator import ServiceCoordinator
from infrastructure import MyInfrastructure
import vars
import os

#has to be before any ray import or might not work
os.environ["RAY_DEDUP_LOGS"] = "0"




def run_simulation(model: str, infrastructure) -> None:
    my_infrastructure: MyInfrastructure = MyInfrastructure()
    my_infrastructure.generate_new_infrastructure(model, infrastructure)


    processes, coordinator_process = my_infrastructure.start_services()

    for p in processes:
        p.join()

    coordinator_process.kill()



if __name__ == "__main__":
    run_simulation(vars.MY_MODEL, vars.INFRASTRUCTURE_1)