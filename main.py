import asyncio

from eclypse.simulation import Simulation

from SimulationConfig import get_config
from coordinator import ServiceCoordinator
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
            return
        await asyncio.sleep(0.1)

myInfrastructure: MyInfrastructure = MyInfrastructure()
myInfrastructure.generate_new_infrastructure()

simulation = Simulation(myInfrastructure.get_infrastructure(), simulation_config=get_config())
simulation.register(myInfrastructure.get_application())

simulation.start()
simulation.step()

loop = asyncio.get_event_loop()
forecast = loop.run_until_complete(wait_for_end())
loop.close()

simulation.stop()


application_frame = simulation.report.application()
service_frame = simulation.report.service()

print(application_frame.head())
print(service_frame.head())
