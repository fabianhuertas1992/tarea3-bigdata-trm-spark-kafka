#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
BASE="$PWD"
# Solo procesos Java iniciados con los archivos de configuración de este proyecto.
python3 - "$BASE" <<'PY'
import os,signal,sys
from pathlib import Path
base=sys.argv[1]
for p in Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:args=(p/'cmdline').read_bytes().decode().split('\0')
 except (OSError,UnicodeError):continue
 owned=any(x in args for x in [base+'/runtime/server.properties',base+'/runtime/zookeeper.properties'])
 if owned and args and Path(args[0]).name=='java':
  print('Deteniendo Kafka o ZooKeeper del proyecto, PID',p.name);os.kill(int(p.name),signal.SIGTERM)
PY
