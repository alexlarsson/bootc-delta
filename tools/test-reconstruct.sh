#!/bin/bash

set -e

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <image1.oci-archive> <image2.oci-archive>"
    echo ""
    echo "Creates a delta from image1 to image2, reconstructs image2, and validates diff_ids match."
    echo "An ostree repo is created from image1 and used as the delta source."
    echo ""
    echo "Arguments:"
    echo "  image1.oci-archive  Path to old OCI archive"
    echo "  image2.oci-archive  Path to new OCI archive"
    exit 1
fi

IMAGE1="$1"
IMAGE2="$2"

if [ ! -f "$IMAGE1" ]; then
    echo "Error: $IMAGE1 not found"
    exit 1
fi

if [ ! -f "$IMAGE2" ]; then
    echo "Error: $IMAGE2 not found"
    exit 1
fi

TMPDIR=$(mktemp -d -t oci-delta-test.XXXXXX)
trap 'rm -rf "$TMPDIR"' EXIT

DELTA_FILE="$TMPDIR/test.oci-delta"
RECONSTRUCTED="$TMPDIR/reconstructed.oci-archive"
OSTREE_REPO="$TMPDIR/ostree-repo"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "==> Creating ostree repo from $IMAGE1"
python3 "$SCRIPT_DIR/create-ostree-repo.py" "$IMAGE1" "$OSTREE_REPO"

echo ""
echo "==> Creating delta: $IMAGE1 -> $IMAGE2"
./oci-delta create -verbose "$IMAGE1" "$IMAGE2" "$DELTA_FILE"

if [ ! -f "$DELTA_FILE" ]; then
    echo "Error: Delta file was not created"
    exit 1
fi

DELTA_SIZE=$(stat -f%z "$DELTA_FILE" 2>/dev/null || stat -c%s "$DELTA_FILE")
echo "Delta size: $DELTA_SIZE bytes"

echo ""
echo "==> Applying delta to reconstruct image"
./oci-delta apply -repo "$OSTREE_REPO" "$DELTA_FILE" "$RECONSTRUCTED"

if [ ! -f "$RECONSTRUCTED" ]; then
    echo "Error: Reconstructed archive was not created"
    exit 1
fi

echo ""
echo "==> Validating reconstruction"

ORIGINAL_DIR="$TMPDIR/original"
RECONSTRUCTED_DIR="$TMPDIR/reconstructed"

mkdir -p "$ORIGINAL_DIR" "$RECONSTRUCTED_DIR"

echo "Extracting original image..."
tar -xf "$IMAGE2" -C "$ORIGINAL_DIR"

echo "Extracting reconstructed image..."
tar -xf "$RECONSTRUCTED" -C "$RECONSTRUCTED_DIR"

extract_diff_ids() {
    local image_dir="$1"
    local index_file="$image_dir/index.json"

    if [ ! -f "$index_file" ]; then
        echo "Error: index.json not found in $image_dir"
        return 1
    fi

    local manifest_digests=$(jq -r '.manifests[].digest' "$index_file" | sed 's/sha256://')

    local all_diff_ids=""
    for digest in $manifest_digests; do
        local manifest_file="$image_dir/blobs/sha256/$digest"
        if [ ! -f "$manifest_file" ]; then
            continue
        fi

        local config_digest=$(jq -r '.config.digest' "$manifest_file" | sed 's/sha256://')
        local config_file="$image_dir/blobs/sha256/$config_digest"

        if [ ! -f "$config_file" ]; then
            continue
        fi

        local diff_ids=$(jq -r '.rootfs.diff_ids[]' "$config_file")
        all_diff_ids="$all_diff_ids"$'\n'"$diff_ids"
    done

    echo "$all_diff_ids" | grep -v '^$' | sort
}

echo "Extracting diff_ids from original..."
ORIGINAL_DIFF_IDS=$(extract_diff_ids "$ORIGINAL_DIR")

echo "Extracting diff_ids from reconstructed..."
RECONSTRUCTED_DIFF_IDS=$(extract_diff_ids "$RECONSTRUCTED_DIR")

if [ -z "$ORIGINAL_DIFF_IDS" ]; then
    echo "Error: Could not extract diff_ids from original image"
    exit 1
fi

if [ -z "$RECONSTRUCTED_DIFF_IDS" ]; then
    echo "Error: Could not extract diff_ids from reconstructed image"
    exit 1
fi

echo ""
echo "Original diff_ids:"
echo "$ORIGINAL_DIFF_IDS" | sed 's/^/  /'

echo ""
echo "Reconstructed diff_ids:"
echo "$RECONSTRUCTED_DIFF_IDS" | sed 's/^/  /'

if [ "$ORIGINAL_DIFF_IDS" = "$RECONSTRUCTED_DIFF_IDS" ]; then
    echo ""
    echo "✓ Validation successful: All diff_ids match!"
    exit 0
else
    echo ""
    echo "✗ Validation failed: diff_ids do not match"
    echo ""
    echo "Differences:"
    diff <(echo "$ORIGINAL_DIFF_IDS") <(echo "$RECONSTRUCTED_DIFF_IDS") || true
    exit 1
fi
