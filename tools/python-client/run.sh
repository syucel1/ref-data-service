#!/usr/bin/env bash
# Regenerates the Python protobuf classes from the contract and runs the sample client.
# Usage: tools/python-client/run.sh [--host H] [--port P] [--types CLIENT ACCOUNT ...]
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
proto="$root/refdata-contract/src/main/proto"
mkdir -p "$here/gen"
"$root/tools/protoc" -I "$proto" --python_out="$here/gen" \
    "$proto/refdata/protocol/v1/protocol.proto" "$proto/refdata/schema/v1/entities.proto"
python3 -c 'import google.protobuf' 2>/dev/null || { echo "python 'protobuf' package missing: pip install protobuf" >&2; exit 2; }
exec python3 "$here/refdata_client.py" "$@"
