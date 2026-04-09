#!/usr/bin/env python3
"""
Synthetic bootc OCI image test for oci-delta.

Creates minimal OCI images replicating real bootc layer structure, then
tests delta creation and application against all corner cases:

  1. Identical layer (same digest+diff_id) is skipped entirely
  2. Recompressed layer (same diff_id, different digest) is skipped via diff_id
  3. One new delta layer uses objects from multiple old layers as sources,
     matched via hardlink basenames (e.g. usr/lib64/libfoo.so -> ostree object)
"""

import gzip
import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path


# --- OCI / layer helpers ---

def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_digest(data: bytes) -> str:
    return "sha256:" + sha256_hex(data)


def compress_gzip(data: bytes, level: int = 6) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(filename='', mtime=0, fileobj=buf, mode='wb', compresslevel=level) as gz:
        gz.write(data)
    return buf.getvalue()


def synthetic_bytes(seed: str, size: int = 8192) -> bytes:
    """Deterministic pseudo-random bytes from seed, large enough for useful bsdiff."""
    out = bytearray()
    h = hashlib.sha256(seed.encode()).digest()
    while len(out) < size:
        h = hashlib.sha256(h).digest()
        out.extend(h)
    return bytes(out[:size])


def patch_bytes(data: bytes, offset: int = 500) -> bytes:
    """Flip 64 bytes to simulate an updated file with mostly identical content."""
    b = bytearray(data)
    for i in range(64):
        b[offset + i] ^= 0xA5
    return bytes(b)


def ostree_path(content: bytes) -> str:
    """Real ostree content-addresses objects: path = sha256(content)."""
    h = sha256_hex(content)
    return f"sysroot/ostree/repo/objects/{h[:2]}/{h[2:]}.file"


def make_layer_tar(files: dict, hardlinks: dict = None) -> bytes:
    """
    Build an uncompressed tar layer.
      files:     {path: bytes}
      hardlinks: {link_path: target_path}  — as real bootc does for usr/ paths
    """
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode='w') as t:
        for path, content in files.items():
            info = tarfile.TarInfo(name=path)
            info.size = len(content)
            t.addfile(info, io.BytesIO(content))
        for link, target in (hardlinks or {}).items():
            info = tarfile.TarInfo(name=link)
            info.type = tarfile.LNKTYPE
            info.linkname = target
            t.addfile(info)
    return buf.getvalue()


def write_oci_archive(path: str, layer_pairs: list) -> None:
    """
    Write an OCI archive (uncompressed tar).
    layer_pairs: [(uncompressed_tar, compressed_tar), ...]
    """
    blobs = {}
    layer_descs = []
    diff_ids = []

    for uncompressed, compressed in layer_pairs:
        diff_id = sha256_digest(uncompressed)
        diff_ids.append(diff_id)
        d = sha256_digest(compressed)
        blobs[d] = compressed
        layer_descs.append({
            "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
            "digest": d, "size": len(compressed),
        })

    config = json.dumps({
        "architecture": "amd64", "os": "linux",
        "rootfs": {"type": "layers", "diff_ids": diff_ids},
    }, separators=(',', ':')).encode()
    config_d = sha256_digest(config)
    blobs[config_d] = config

    manifest = json.dumps({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                   "digest": config_d, "size": len(config)},
        "layers": layer_descs,
    }, separators=(',', ':')).encode()
    manifest_d = sha256_digest(manifest)
    blobs[manifest_d] = manifest

    index = json.dumps({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [{"mediaType": "application/vnd.oci.image.manifest.v1+json",
                       "digest": manifest_d, "size": len(manifest)}],
    }, separators=(',', ':')).encode()

    oci_layout = json.dumps({"imageLayoutVersion": "1.0.0"}, separators=(',', ':')).encode()

    with tarfile.open(path, 'w') as t:
        def add(name, data):
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            t.addfile(info, io.BytesIO(data))
        add('oci-layout', oci_layout)
        add('index.json', index)
        for d_str, data in blobs.items():
            _, h = d_str.split(':', 1)
            add(f'blobs/sha256/{h}', data)


def get_diff_ids(archive_path: str) -> list:
    with tarfile.open(archive_path, 'r') as t:
        index = json.load(t.extractfile('index.json'))
        mh = index['manifests'][0]['digest'].split(':')[1]
        manifest = json.load(t.extractfile(f'blobs/sha256/{mh}'))
        ch = manifest['config']['digest'].split(':')[1]
        config = json.load(t.extractfile(f'blobs/sha256/{ch}'))
        return config['rootfs']['diff_ids']


def get_layer_digests(archive_path: str) -> list:
    with tarfile.open(archive_path, 'r') as t:
        index = json.load(t.extractfile('index.json'))
        mh = index['manifests'][0]['digest'].split(':')[1]
        manifest = json.load(t.extractfile(f'blobs/sha256/{mh}'))
        return [l['digest'] for l in manifest['layers']]


def extract_ostree_objects(archive_path: str, dest_dir: str) -> None:
    """Extract sysroot/ostree/repo/objects/ from all layers into dest_dir."""
    with tarfile.open(archive_path, 'r') as outer:
        index = json.load(outer.extractfile('index.json'))
        mh = index['manifests'][0]['digest'].split(':')[1]
        manifest = json.load(outer.extractfile(f'blobs/sha256/{mh}'))
        for layer_desc in manifest['layers']:
            lh = layer_desc['digest'].split(':')[1]
            layer_gz = outer.extractfile(f'blobs/sha256/{lh}').read()
            layer_bytes = gzip.decompress(layer_gz)
            with tarfile.open(fileobj=io.BytesIO(layer_bytes), mode='r') as lt:
                for member in lt.getmembers():
                    if member.isfile() and member.name.startswith('sysroot/ostree/repo/objects/'):
                        dest = os.path.join(dest_dir, member.name)
                        os.makedirs(os.path.dirname(dest), exist_ok=True)
                        with lt.extractfile(member) as f, open(dest, 'wb') as out:
                            out.write(f.read())


# --- Test harness ---

def check(condition: bool, message: str) -> None:
    if condition:
        print(f"  PASS: {message}")
    else:
        print(f"  FAIL: {message}")
        sys.exit(1)


def run(cmd: list, **kwargs) -> subprocess.CompletedProcess:
    result = subprocess.run(cmd, capture_output=True, text=True, **kwargs)
    if result.returncode != 0:
        print(f"  ERROR running {' '.join(cmd)}:")
        print(result.stderr)
        sys.exit(1)
    return result


def main():
    script_dir = Path(__file__).parent
    oci_delta = script_dir.parent / 'oci-delta'
    if not oci_delta.exists():
        print(f"ERROR: {oci_delta} not found — run 'make build' first", file=sys.stderr)
        sys.exit(1)

    # ------------------------------------------------------------------ #
    # Synthetic file contents                                              #
    # ------------------------------------------------------------------ #

    # Layer 1 (base): in both images, identical compressed bytes
    libc   = synthetic_bytes("libc.so.6",   8192)
    libm   = synthetic_bytes("libm.so.6",   6144)

    # Layer 2 (pkg_a): recompressed between old and new — same diff_id, different digest
    libssl    = synthetic_bytes("libssl.so.1",    8192)
    libcrypto = synthetic_bytes("libcrypto.so.1", 10240)

    # Layers 3+4 (pkg_b, pkg_c): old sources — new image combines their
    # updated versions into one layer, matching via hardlink basenames
    libz    = synthetic_bytes("libz.so.1",    5120)
    libbz2  = synthetic_bytes("libbz2.so.1",  4096)
    liblzma = synthetic_bytes("liblzma.so.5", 6144)
    libpcre = synthetic_bytes("libpcre.so.3", 7168)

    libz_new    = patch_bytes(libz,    500)
    libbz2_new  = patch_bytes(libbz2,  300)
    liblzma_new = patch_bytes(liblzma, 700)
    libpcre_new = patch_bytes(libpcre, 400)

    # ------------------------------------------------------------------ #
    # Old image                                                            #
    # ------------------------------------------------------------------ #

    # Layer 1: base (will be identical in new image)
    libc_path = ostree_path(libc)
    libm_path = ostree_path(libm)
    old_l1_tar = make_layer_tar(
        {libc_path: libc, libm_path: libm},
        {"usr/lib64/libc.so.6": libc_path, "usr/lib64/libm.so.6": libm_path},
    )
    old_l1_gz = compress_gzip(old_l1_tar)

    # Layer 2: pkg_a, compressed at level 1
    ssl_path    = ostree_path(libssl)
    crypto_path = ostree_path(libcrypto)
    old_l2_tar = make_layer_tar(
        {ssl_path: libssl, crypto_path: libcrypto},
        {"usr/lib64/libssl.so.1": ssl_path, "usr/lib64/libcrypto.so.1": crypto_path},
    )
    old_l2_gz = compress_gzip(old_l2_tar, level=1)

    # Layer 3: pkg_b (z and bz2) — source for multi-layer delta
    z_path   = ostree_path(libz)
    bz2_path = ostree_path(libbz2)
    old_l3_tar = make_layer_tar(
        {z_path: libz, bz2_path: libbz2},
        {"usr/lib64/libz.so.1": z_path, "usr/lib64/libbz2.so.1": bz2_path},
    )
    old_l3_gz = compress_gzip(old_l3_tar)

    # Layer 4: pkg_c (lzma and pcre) — second source for multi-layer delta
    lzma_path = ostree_path(liblzma)
    pcre_path = ostree_path(libpcre)
    old_l4_tar = make_layer_tar(
        {lzma_path: liblzma, pcre_path: libpcre},
        {"usr/lib64/liblzma.so.5": lzma_path, "usr/lib64/libpcre.so.3": pcre_path},
    )
    old_l4_gz = compress_gzip(old_l4_tar)

    old_layers = [
        (old_l1_tar, old_l1_gz),
        (old_l2_tar, old_l2_gz),
        (old_l3_tar, old_l3_gz),
        (old_l4_tar, old_l4_gz),
    ]

    # ------------------------------------------------------------------ #
    # New image                                                            #
    # ------------------------------------------------------------------ #

    # Layer 1: exactly identical to old layer 1 (same tar bytes → same compressed bytes)
    new_l1_tar = old_l1_tar
    new_l1_gz  = old_l1_gz

    # Layer 2: same tar content, level-9 compression → same diff_id, different digest
    new_l2_gz = compress_gzip(old_l2_tar, level=9)
    assert sha256_digest(old_l2_tar) == sha256_digest(old_l2_tar)   # sanity
    assert sha256_digest(old_l2_gz) != sha256_digest(new_l2_gz), \
        "recompressed layer should have different digest"

    # Layer 3: updated pkg_b+pkg_c objects combined into one layer.
    # Hardlinks preserve the same usr/ basenames as old layers 3 and 4,
    # so tar-diff can match new objects to old ones across both source layers.
    z_new_path    = ostree_path(libz_new)
    bz2_new_path  = ostree_path(libbz2_new)
    lzma_new_path = ostree_path(liblzma_new)
    pcre_new_path = ostree_path(libpcre_new)
    new_l3_tar = make_layer_tar(
        {z_new_path: libz_new, bz2_new_path: libbz2_new,
         lzma_new_path: liblzma_new, pcre_new_path: libpcre_new},
        {
            "usr/lib64/libz.so.1":    z_new_path,    # basename matches old layer 3
            "usr/lib64/libbz2.so.1":  bz2_new_path,  # basename matches old layer 3
            "usr/lib64/liblzma.so.5": lzma_new_path, # basename matches old layer 4
            "usr/lib64/libpcre.so.3": pcre_new_path, # basename matches old layer 4
        },
    )
    new_l3_gz = compress_gzip(new_l3_tar)

    new_layers = [
        (new_l1_tar, new_l1_gz),
        (old_l2_tar, new_l2_gz),
        (new_l3_tar, new_l3_gz),
    ]

    # ------------------------------------------------------------------ #
    # Run tests                                                            #
    # ------------------------------------------------------------------ #

    with tempfile.TemporaryDirectory(prefix='oci-delta-test-') as tmp:
        old_img       = os.path.join(tmp, 'old.oci-archive')
        new_img       = os.path.join(tmp, 'new.oci-archive')
        delta         = os.path.join(tmp, 'delta.tar')
        reconstructed = os.path.join(tmp, 'reconstructed.oci-archive')
        delta_source  = os.path.join(tmp, 'delta-source')

        print("Creating synthetic OCI images...")
        write_oci_archive(old_img, old_layers)
        write_oci_archive(new_img, new_layers)

        # ---- Test 1+2: delta creation skips identical and recompressed layers ----
        print("\n[Test 1+2] Delta creation: identical and recompressed layers are skipped")
        result = run([str(oci_delta), 'create', '-v', old_img, new_img, delta])
        stats = result.stdout
        check('Skipped layers:   2' in stats,
              'skipped 2 layers (identical + recompressed)')
        check('Processed layers: 1' in stats,
              'processed 1 layer (the new combined layer)')
        check('Original layer bytes:   0' in stats,
              'tar-diff produced smaller output than original for all processed layers')

        # ---- Test 3: apply reconstructs correct diff_ids ----
        print("\n[Test 3] Apply: reconstructed archive has correct diff_ids")
        os.makedirs(delta_source)
        extract_ostree_objects(old_img, delta_source)
        run([str(oci_delta), 'apply',
             f'--delta-source={delta_source}', delta, reconstructed])

        expected_diff_ids = get_diff_ids(new_img)
        actual_diff_ids   = get_diff_ids(reconstructed)
        check(len(actual_diff_ids) == len(expected_diff_ids),
              f'reconstructed image has {len(expected_diff_ids)} layers')
        for i, (exp, act) in enumerate(zip(expected_diff_ids, actual_diff_ids)):
            check(exp == act, f'layer {i+1} diff_id matches')

        # ---- Test 4: skipped layers pass through by digest ----
        print("\n[Test 4] Skipped layers present in reconstructed archive by digest")
        new_digests  = set(get_layer_digests(new_img))
        rec_digests  = set(get_layer_digests(reconstructed))
        # Layer 1 (identical) keeps same digest in output
        check(sha256_digest(new_l1_gz) in rec_digests,
              'identical layer digest preserved in output')
        # Layer 2 (recompressed) is NOT in the delta, so the reconstructed
        # archive uses the new digest from the updated manifest
        # (bootc will match it by diff_id, not digest)
        check(len(rec_digests) == len(new_digests),
              'reconstructed archive has same number of layers as new image')

        print("\nAll tests passed.")


if __name__ == '__main__':
    main()
