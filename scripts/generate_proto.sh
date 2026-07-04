#!/usr/bin/env bash
# Generate Python gRPC stubs from proto/lattice.proto
# Run from the project root: ./scripts/generate_proto.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$SCRIPT_DIR/.."

echo "Generating gRPC stubs..."
cd "$ROOT"

python -m grpc_tools.protoc \
  -I proto \
  --python_out=lattice/proto_gen \
  --grpc_python_out=lattice/proto_gen \
  proto/lattice.proto

# Fix relative imports in generated files (grpcio-tools uses absolute imports
# in newer versions; patch for compatibility)
if grep -q "^import lattice_pb2" lattice/proto_gen/lattice_pb2_grpc.py 2>/dev/null; then
  sed -i.bak 's/^import lattice_pb2/from lattice.proto_gen import lattice_pb2/' \
    lattice/proto_gen/lattice_pb2_grpc.py
  rm -f lattice/proto_gen/lattice_pb2_grpc.py.bak
fi

echo "Done. Generated files in lattice/proto_gen/"
ls -la lattice/proto_gen/
