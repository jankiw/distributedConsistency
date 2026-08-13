from eclypse.simulation import SimulationConfig

from strategy import MyStrategy

MAX_STEPS: int = 30

def get_config() -> SimulationConfig:
    config = SimulationConfig(
        remote=True,
        seed=42,
        max_steps=MAX_STEPS,
        # step_every_ms=10,
        include_default_metrics=True,
        log_level= "INFO",

        default_strategy=MyStrategy(),
    )
    return config
