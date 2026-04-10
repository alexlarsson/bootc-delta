#!/bin/bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
OCI_DELTA="$PROJECT_DIR/oci-delta"

for cmd in podman jq; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "Error: $cmd is required"
        exit 1
    fi
done

PASS=0
FAIL=0

pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1 — $2"; FAIL=$((FAIL + 1)); }

check_diff_ids() {
    local name="$1" expected="$2" actual="$3"
    if [ "$expected" = "$actual" ]; then
        pass "$name"
    else
        fail "$name" "diff_ids mismatch"
        echo "    expected: $(echo "$expected" | tr '\n' ' ')"
        echo "    actual:   $(echo "$actual" | tr '\n' ' ')"
    fi
}

get_diff_ids() {
    local ref="$1" dir cleanup=""
    if [ -d "$ref" ]; then
        dir="$ref"
    else
        dir=$(mktemp -d)
        cleanup="$dir"
        tar -xf "$ref" -C "$dir"
    fi
    local mhash chash
    mhash=$(jq -r '.manifests[0].digest' "$dir/index.json" | cut -d: -f2)
    chash=$(jq -r '.config.digest' "$dir/blobs/sha256/$mhash" | cut -d: -f2)
    jq -r '.rootfs.diff_ids[]' "$dir/blobs/sha256/$chash"
    [ -n "$cleanup" ] && rm -rf "$cleanup"
    return 0
}

get_diff_ids_storage() {
    podman inspect --format '{{range .RootFS.Layers}}{{.}}
{{end}}' "$1" | grep -v '^$'
}

extract_rootfs() {
    local oci_dir="$1" outdir="$2"
    mkdir -p "$outdir"
    local mhash
    mhash=$(jq -r '.manifests[0].digest' "$oci_dir/index.json" | cut -d: -f2)
    for lhash in $(jq -r '.layers[].digest' "$oci_dir/blobs/sha256/$mhash" | cut -d: -f2); do
        tar -xzf "$oci_dir/blobs/sha256/$lhash" -C "$outdir"
    done
}

create_random_file() {
    local path="$1" size_kb="${2:-64}"
    dd if=/dev/urandom bs=1024 count="$size_kb" of="$path" 2>/dev/null
}

create_versioned_pair() {
    local dir_v1="$1" dir_v2="$2" name="$3"
    for i in $(seq 1 200); do echo "${name}-shared-prefix-line-$i"; done > "$dir_v1/version.txt"
    echo "${name}-version-1-unique-content" >> "$dir_v1/version.txt"
    for i in $(seq 1 200); do echo "${name}-shared-suffix-line-$i"; done >> "$dir_v1/version.txt"
    cp "$dir_v1/version.txt" "$dir_v2/version.txt"
    sed -i "s/${name}-version-1-unique-content/${name}-version-2-unique-content/" "$dir_v2/version.txt"
}

# ============================================================
# Setup
# ============================================================

echo "=== Setting up test environment ==="

TEST_DIR=$(mktemp -d /var/tmp/oci-delta-test-XXXXXX)
cleanup() {
    podman image rm --all --force 2>/dev/null || true
    buildah unshare rm -rf "$TEST_DIR" 2>/dev/null || true
}
trap cleanup EXIT
echo "  Test directory: $TEST_DIR"

STORAGE_ROOT="$TEST_DIR/storage"
STORAGE_RUN="$TEST_DIR/run"
STORAGE_DRIVER="overlay"

export CONTAINERS_STORAGE_CONF="$TEST_DIR/storage.conf"
cat > "$CONTAINERS_STORAGE_CONF" <<EOF
[storage]
driver = "$STORAGE_DRIVER"
graphroot = "$STORAGE_ROOT"
runroot = "$STORAGE_RUN"
EOF

podman() {
    command podman --root "$STORAGE_ROOT" --runroot "$STORAGE_RUN" --storage-driver "$STORAGE_DRIVER" "$@"
}

echo "  Building oci-delta..."
make -C "$PROJECT_DIR" build -s

# ============================================================
# Create test data and images
# ============================================================

echo "=== Creating test data ==="

TD="$TEST_DIR/testdata"
mkdir -p "$TD"/{layer1,layer2-v1,layer2-v2,layer3-v1,layer3-v2}

echo "layer1-file1" > "$TD/layer1/file1.txt"
create_random_file "$TD/layer1/data.bin" 32

create_random_file "$TD/layer2-v1/shared.bin" 64
cp "$TD/layer2-v1/shared.bin" "$TD/layer2-v2/shared.bin"
create_versioned_pair "$TD/layer2-v1" "$TD/layer2-v2" "layer2"

create_random_file "$TD/layer3-v1/shared.bin" 64
cp "$TD/layer3-v1/shared.bin" "$TD/layer3-v2/shared.bin"
create_versioned_pair "$TD/layer3-v1" "$TD/layer3-v2" "layer3"

# image1: layer1 + layer2-v1 + layer3-v1
cat > "$TD/Containerfile.1" <<'EOF'
FROM scratch
COPY layer1/ /layer1/
COPY layer2-v1/ /layer2/
COPY layer3-v1/ /layer3/
EOF

# image2: layer1 + layer2-v2 + layer3-v1 (layer1, layer3 reused)
cat > "$TD/Containerfile.2" <<'EOF'
FROM scratch
COPY layer1/ /layer1/
COPY layer2-v2/ /layer2/
COPY layer3-v1/ /layer3/
EOF

# image3: layer1 + layer2-v2 + layer3-v2 (layer1, layer2 reused)
cat > "$TD/Containerfile.3" <<'EOF'
FROM scratch
COPY layer1/ /layer1/
COPY layer2-v2/ /layer2/
COPY layer3-v2/ /layer3/
EOF

echo "=== Building test images ==="
cd "$TD"
podman build --timestamp 0 -q -f Containerfile.1 -t localhost/testimage1:latest . >/dev/null
podman build --timestamp 0 -q -f Containerfile.2 -t localhost/testimage2:latest . >/dev/null
podman build --timestamp 0 -q -f Containerfile.3 -t localhost/testimage3:latest . >/dev/null

echo "=== Exporting images ==="
IMAGES="$TEST_DIR/images"
mkdir -p "$IMAGES"
podman save --format oci-archive -o "$IMAGES/image1.oci-archive" localhost/testimage1:latest
podman save --format oci-archive -o "$IMAGES/image2.oci-archive" localhost/testimage2:latest
podman save --format oci-archive -o "$IMAGES/image3.oci-archive" localhost/testimage3:latest

for i in 1 2 3; do
    mkdir -p "$IMAGES/image${i}-oci"
    tar -xf "$IMAGES/image${i}.oci-archive" -C "$IMAGES/image${i}-oci"
done

REF_DIFFIDS_1=$(get_diff_ids "$IMAGES/image1.oci-archive")
REF_DIFFIDS_2=$(get_diff_ids "$IMAGES/image2.oci-archive")
REF_DIFFIDS_3=$(get_diff_ids "$IMAGES/image3.oci-archive")

echo "  Image 1 diff_ids: $(echo "$REF_DIFFIDS_1" | tr '\n' ' ')"
echo "  Image 2 diff_ids: $(echo "$REF_DIFFIDS_2" | tr '\n' ' ')"
echo "  Image 3 diff_ids: $(echo "$REF_DIFFIDS_3" | tr '\n' ' ')"

# Verify layer reuse assumptions
diffids1=($REF_DIFFIDS_1)
diffids2=($REF_DIFFIDS_2)
diffids3=($REF_DIFFIDS_3)

if [ "${diffids1[0]}" != "${diffids2[0]}" ]; then
    echo "ERROR: image1 and image2 do not share layer 1 — test setup broken"
    exit 1
fi
if [ "${diffids1[2]}" != "${diffids2[2]}" ]; then
    echo "ERROR: image1 and image2 do not share layer 3 — test setup broken"
    exit 1
fi
if [ "${diffids2[1]}" != "${diffids3[1]}" ]; then
    echo "ERROR: image2 and image3 do not share layer 2 — test setup broken"
    exit 1
fi
echo "  Layer reuse verified"

# ============================================================
# Create deltas
# ============================================================

echo ""
echo "=== Test: Delta creation ==="

DELTAS="$TEST_DIR/deltas"
mkdir -p "$DELTAS"

echo "  Creating delta 1→2 (oci-archive → oci-archive)..."
if $OCI_DELTA create "$IMAGES/image1.oci-archive" "$IMAGES/image2.oci-archive" "$DELTAS/delta12.oci-archive"; then
    pass "create delta 1→2 (oci-archive → oci-archive)"
else
    fail "create delta 1→2 (oci-archive → oci-archive)" "exit code $?"
fi

echo "  Creating delta 1→2 (oci dir → oci dir)..."
if $OCI_DELTA create "oci:$IMAGES/image1-oci" "oci:$IMAGES/image2-oci" "oci:$DELTAS/delta12-oci"; then
    pass "create delta 1→2 (oci dir → oci dir)"
else
    fail "create delta 1→2 (oci dir → oci dir)" "exit code $?"
fi

echo "  Creating delta 2→3 (containers-storage → oci-archive)..."
if $OCI_DELTA create "containers-storage:localhost/testimage2:latest" "containers-storage:localhost/testimage3:latest" "$DELTAS/delta23.oci-archive"; then
    pass "create delta 2→3 (containers-storage → oci-archive)"
else
    fail "create delta 2→3 (containers-storage → oci-archive)" "exit code $?"
fi

# ============================================================
# Apply tests
# ============================================================

echo ""
echo "=== Test: Apply delta ==="

OUTPUTS="$TEST_DIR/outputs"
mkdir -p "$OUTPUTS"

extract_rootfs "$IMAGES/image1-oci" "$TEST_DIR/image1-rootfs"

echo "  Applying delta 1→2 with --directory → oci-archive..."
if $OCI_DELTA apply --directory "$TEST_DIR/image1-rootfs" "$DELTAS/delta12.oci-archive" "$OUTPUTS/recon2-dir.oci-archive"; then
    actual=$(get_diff_ids "$OUTPUTS/recon2-dir.oci-archive")
    check_diff_ids "apply delta 1→2 (--directory → oci-archive)" "$REF_DIFFIDS_2" "$actual"
else
    fail "apply delta 1→2 (--directory → oci-archive)" "exit code $?"
fi

echo "  Applying delta 1→2 with --container-storage → oci dir..."
if $OCI_DELTA apply --container-storage "$TEST_DIR/storage" "$DELTAS/delta12.oci-archive" "oci:$OUTPUTS/recon2-cs-oci"; then
    actual=$(get_diff_ids "$OUTPUTS/recon2-cs-oci")
    check_diff_ids "apply delta 1→2 (--container-storage → oci dir)" "$REF_DIFFIDS_2" "$actual"
else
    fail "apply delta 1→2 (--container-storage → oci dir)" "exit code $?"
fi

echo "  Applying delta 1→2 from oci dir delta with --directory → oci-archive..."
if $OCI_DELTA apply --directory "$TEST_DIR/image1-rootfs" "oci:$DELTAS/delta12-oci" "$OUTPUTS/recon2-ocidir.oci-archive"; then
    actual=$(get_diff_ids "$OUTPUTS/recon2-ocidir.oci-archive")
    check_diff_ids "apply delta 1→2 (oci dir delta, --directory)" "$REF_DIFFIDS_2" "$actual"
else
    fail "apply delta 1→2 (oci dir delta, --directory)" "exit code $?"
fi

# ============================================================
# Import + chain test
# ============================================================

echo ""
echo "=== Test: Import delta (chaining) ==="

podman rmi localhost/testimage2:latest localhost/testimage3:latest >/dev/null 2>&1 || true

echo "  Importing delta 1→2..."
if IMPORT2_ID=$($OCI_DELTA import --tag localhost/recon2:latest "$DELTAS/delta12.oci-archive"); then
    echo "    Imported as ${IMPORT2_ID:0:16}"
    actual=$(get_diff_ids_storage localhost/recon2:latest)
    check_diff_ids "import delta 1→2" "$REF_DIFFIDS_2" "$actual"
else
    fail "import delta 1→2" "exit code $?"
fi

echo "  Importing delta 2→3 (chained, source = imported image2)..."
if IMPORT3_ID=$($OCI_DELTA import --tag localhost/recon3:latest "$DELTAS/delta23.oci-archive"); then
    echo "    Imported as ${IMPORT3_ID:0:16}"
    actual=$(get_diff_ids_storage localhost/recon3:latest)
    check_diff_ids "import delta 2→3 (chained)" "$REF_DIFFIDS_3" "$actual"
else
    fail "import delta 2→3 (chained)" "exit code $?"
fi

# ============================================================
# Summary
# ============================================================

echo ""
echo "=== Results ==="
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
[ "$FAIL" -eq 0 ] && echo "  All tests passed!" || echo "  Some tests FAILED!"
exit $((FAIL > 0 ? 1 : 0))
