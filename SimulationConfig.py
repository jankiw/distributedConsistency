import logging

from eclypse.simulation import SimulationConfig

from strategy import MyStrategy

MAX_STEPS: int = 100

def get_config() -> SimulationConfig:
    config = SimulationConfig(
        remote=True,
        seed=42,
        max_steps=MAX_STEPS,
        step_every_ms=500,
        include_default_metrics=True,
        log_level= "INFO",

        default_strategy=MyStrategy(),
    )
    return config
