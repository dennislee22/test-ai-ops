import os, sys, argparse, logging, logging.handlers
import re, subprocess
from pathlib import Path

_HERE = Path(__file__).resolve().parent.parent

_pre = argparse.ArgumentParser(add_help=False)
_pre.add_argument("--model-dir", default=None)
_pre.add_argument("--embed-dir", default=None)
_pre.add_argument("--ingest",    default=None)
_pre.add_argument("--force",     action="store_true")
_pre.add_argument("--port",      type=int, default=8000)
_pre.add_argument("--host",      default="0.0.0.0")
_pre.add_argument("--reload",    action="store_true")
ARGS, _ = _pre.parse_known_args()

_env_file = _HERE / "env"
if _env_file.exists():
    from dotenv import load_dotenv
    load_dotenv(_env_file)

os.environ.setdefault("LLM_MODEL",       "Qwen/Qwen3-8B")
os.environ.setdefault("EMBED_MODEL",     "nomic-ai/nomic-embed-text-v1.5")
os.environ.setdefault("KUBECONFIG_PATH", "~/kubeconfig")
os.environ.setdefault("LANCEDB_DIR",     str(_HERE / "lancedb"))
os.environ.setdefault("LOG_LEVEL",       "DEBUG")

if ARGS.model_dir: os.environ["LLM_MODEL"] = ARGS.model_dir
if ARGS.embed_dir: os.environ["EMBED_MODEL"] = ARGS.embed_dir

LLM_MODEL       = os.getenv("LLM_MODEL", "Qwen/Qwen3-8B").strip()
EMBED_MODEL     = os.getenv("EMBED_MODEL", "nomic-ai/nomic-embed-text-v1.5").strip()
LANCEDB_DIR     = os.getenv("LANCEDB_DIR", str(_HERE / "lancedb"))

ENABLE_FALLBACK_ROUTING: bool = True

# --- GPU AUTO-DETECTION ---
def _detect_gpu_count() -> int:
    explicit = os.getenv("NUM_GPU")
    if explicit is not None:
        return int(explicit)
    try:
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            timeout=5, stderr=subprocess.DEVNULL)
        return len([l for l in out.decode().strip().splitlines() if l.strip()])
    except Exception:
        pass
    return 0

NUM_GPU        = _detect_gpu_count()
MAX_NEW_TOKENS = int(os.getenv("MAX_NEW_TOKENS", "4096"))
LLM_TIMEOUT    = int(os.getenv("LLM_TIMEOUT", "0")) or (900 if NUM_GPU == 0 else 300)

def _read_cluster_server() -> str:
    try:
        import yaml as _y
        kc = os.path.expanduser(os.getenv("KUBECONFIG_PATH", "~/kubeconfig"))
        if not Path(kc).exists(): return "unknown"
        with open(kc) as f: cfg = _y.safe_load(f)
        clusters = cfg.get("clusters", [])
        if clusters:
            server = clusters[0].get("cluster", {}).get("server", "")
            if server: return re.sub(r'^https?://', '', server).strip()
    except Exception: pass
    return "unknown"

CLUSTER_SERVER = _read_cluster_server()

# --- LOGGING ---
_LOG_DIR = _HERE / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_LEVEL   = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
_FMT_CON = "%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s"
_FMT_FIL = "%(asctime)s  %(levelname)-8s  [%(name)s]  %(filename)s:%(lineno)d  %(message)s"
_DATE    = "%Y-%m-%d %H:%M:%S"
_cfg_set: set = set()

def get_logger(name: str) -> logging.Logger:
    if name in _cfg_set: return logging.getLogger(name)
    log = logging.getLogger(name)
    log.setLevel(_LEVEL)
    if not log.handlers:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(_LEVEL)
        ch.setFormatter(logging.Formatter(_FMT_CON, datefmt=_DATE))
        log.addHandler(ch)
        fh = logging.handlers.RotatingFileHandler(_LOG_DIR / "app.log", maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
        fh.setLevel(_LEVEL)
        fh.setFormatter(logging.Formatter(_FMT_FIL, datefmt=_DATE))
        log.addHandler(fh)
        log.propagate = False
    _cfg_set.add(name)
    return log

for _noisy in ["httpx", "httpcore", "urllib3", "kubernetes.client", "langchain", "langsmith", "watchfiles", "lancedb"]:
    logging.getLogger(_noisy).setLevel(logging.WARNING)

logger   = get_logger("app")
_log_rag = get_logger("rag")
_log_ag  = get_logger("agent")
