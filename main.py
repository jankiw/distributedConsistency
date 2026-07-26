from eclypse.simulation import Simulation


from SimulationConfig import get_config
from infrastructure import MyInfrastructure
import os
os.environ["RAY_DEDUP_LOGS"] = "0"

myInfrastructure: MyInfrastructure = MyInfrastructure()
myInfrastructure.generate_new_infrastructure()

simulation = Simulation(myInfrastructure.get_infrastructure(), simulation_config=get_config())
simulation.register(myInfrastructure.get_application())
simulation.run()

application_frame = simulation.report.application()
service_frame = simulation.report.service()

print(application_frame.head())
print(service_frame.head())
