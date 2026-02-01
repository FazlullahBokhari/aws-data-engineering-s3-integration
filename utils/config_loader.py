import yaml
from pathlib import Path

def load_config(env: str):
    config_path = Path(__file__).parent.parent / "config" / f"{env}.yml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)