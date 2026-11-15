"""Configuration loading. Paths in config.yaml are relative to the project root."""
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parent.parent


def load_config(path=None, root=None):
    root = Path(root) if root else ROOT
    path = Path(path) if path else ROOT / "config" / "config.yaml"
    with open(path, encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    with open(ROOT / "config" / "schema.yaml", encoding="utf-8") as fh:
        cfg["schema"] = yaml.safe_load(fh)
    cfg["root"] = root
    return cfg


def resolve(cfg, key):
    """Absolute Path for a configured path key."""
    return Path(cfg["root"]) / cfg["paths"][key]


def parse_size(text):
    """'100' -> 100, '10k' -> 10_000, '100k' -> 100_000, '1m' -> 1_000_000."""
    t = str(text).strip().lower().replace("_", "").replace(",", "")
    mult = 1
    if t.endswith("k"):
        mult, t = 1_000, t[:-1]
    elif t.endswith("m"):
        mult, t = 1_000_000, t[:-1]
    return int(float(t) * mult)
