import asyncio

from eclypse.simulation import Simulation

from SimulationConfig import get_config
from my_model.coordinator import ServiceCoordinator
from infrastructure import MyInfrastructure
import vars
import os

#has to be before any ray import or might not work
os.environ["RAY_DEDUP_LOGS"] = "0"

from ray.actor import ActorHandle
import ray

async def wait_for_end():
    my_coordinator: ActorHandle
    try:
        my_coordinator = ServiceCoordinator.options(name=vars.COORDINATOR_NAME,
                                                             namespace=vars.COORDINATOR_NAMESPACE).remote(
            user_count=vars.USER_COUNT)
    except:
        my_coordinator = ray.get_actor(vars.COORDINATOR_NAME, namespace=vars.COORDINATOR_NAMESPACE)

    while True:
        if await my_coordinator.is_end.remote():
            await asyncio.sleep(2 * vars.SIMULATION_END_CLEANUP_TIME)
            return my_coordinator
        await asyncio.sleep(0.1)



def run_simulation(model: str, infrastructure) -> None:
    my_infrastructure: MyInfrastructure = MyInfrastructure()
    my_infrastructure.generate_new_infrastructure(model, infrastructure)

    simulation = Simulation(my_infrastructure.get_infrastructure(), simulation_config=get_config())
    simulation.register(my_infrastructure.get_application())

    simulation.start()
    simulation.step()

    loop = asyncio.get_event_loop()
    coordinator: ActorHandle = loop.run_until_complete(wait_for_end())

    simulation.stop()

    result = loop.run_until_complete(coordinator.get_data.remote())
    print(result)

    # application_frame = simulation.report.application()
    # service_frame = simulation.report.service()
    #
    # print(application_frame.head())
    # print(service_frame.head())


run_simulation(vars.MY_MODEL, vars.INFRASTRUCTURE_1)