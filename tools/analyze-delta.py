#!/usr/bin/env python3
"""Analyze a bootc-delta file and print information about its layers."""

import argparse
import io
import json
import os
import sys

try:
    import zstandard as zstd
except ImportError:
    sys.exit("error: 'zstandard' package required — pip install zstandard")

MAGIC = b'tardf1\n\x00'
OP_DATA, OP_OPEN, OP_COPY, OP_ADDDATA, OP_SEEK = 0, 1, 2, 3, 4
TAR_BLOCK = 512


def fmt_size(n):
    n = abs(n)
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if n < 1024 or unit == 'TB':
            return f"{n} B" if unit == 'B' else f"{n:.1f} {unit}"
        n /= 1024


def pct(a, b):
    return f"{a / b * 100:.1f}%" if b else "n/a"


def pad_for(size):
    return (TAR_BLOCK - size % TAR_BLOCK) % TAR_BLOCK


# ── OCI / tar index ───────────────────────────────────────────────────────────

class TarIndex:
    def __init__(self, path):
        self.path = path
        self._entries = {}
        with open(path, 'rb') as f:
            pos = 0
            while True:
                buf = f.read(TAR_BLOCK)
                if len(buf) < TAR_BLOCK or all(b == 0 for b in buf):
                    break
                pos += TAR_BLOCK
                name = buf[:100].rstrip(b'\x00').decode('utf-8', errors='replace')
                sf = buf[124:136]
                size = int.from_bytes(sf[1:12], 'big') if sf[0] & 0x80 else \
                       int(sf.rstrip(b'\x00 ') or b'0', 8)
                self._entries[name] = (pos, size)
                pos += ((size + TAR_BLOCK - 1) // TAR_BLOCK) * TAR_BLOCK
                f.seek(pos)

    def has(self, name):
        return name in self._entries

    def size_of(self, name):
        e = self._entries.get(name)
        return e[1] if e else None

    def read(self, name):
        off, size = self._entries[name]
        with open(self.path, 'rb') as f:
            f.seek(off)
            return f.read(size)


MEDIA_DELTA_CONFIG   = "application/vnd.redhat.bootc-delta.config.v1+json"
MEDIA_TAR_DIFF       = "application/vnd.tar-diff"
MEDIA_IMAGE_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
MEDIA_IMAGE_CONFIG   = "application/vnd.oci.image.config.v1+json"
MEDIA_LAYER_GZIP     = "application/vnd.oci.image.layer.v1.tar+gzip"

ANNOTATION_DELTA_TO           = "io.github.containers.delta.to"
ANNOTATION_DELTA_FROM         = "io.github.containers.delta.from"
ANNOTATION_DELTA_FROM_DIFF_ID = "io.github.containers.delta.from-diff-id"
ANNOTATION_DELTA_REUSED       = "io.github.containers.delta.reused"
ANNOTATION_DELTA_REUSED_DIFF_ID = "io.github.containers.delta.reused-diff-id"


def blob_path(digest):
    return 'blobs/sha256/' + digest.split(':')[1]


def open_delta(path):
    idx = TarIndex(path)
    oci_index = json.loads(idx.read('index.json'))
    delta_manifest_digest = oci_index['manifests'][0]['digest']
    delta_manifest = json.loads(idx.read(blob_path(delta_manifest_digest)))

    if delta_manifest.get('config', {}).get('mediaType') != MEDIA_DELTA_CONFIG:
        sys.exit("error: not a bootc-delta file (unexpected config mediaType)")

    image_manifest_desc = None
    image_config_desc = None
    delta_layer_by_to = {}  # digest string -> layer descriptor dict

    for layer in delta_manifest.get('layers', []):
        mt = layer.get('mediaType', '')
        if mt == MEDIA_IMAGE_MANIFEST:
            image_manifest_desc = layer
        elif mt == MEDIA_IMAGE_CONFIG:
            image_config_desc = layer
        elif mt in (MEDIA_TAR_DIFF, MEDIA_LAYER_GZIP):
            to_digest = (layer.get('annotations') or {}).get(ANNOTATION_DELTA_TO)
            if to_digest:
                delta_layer_by_to[to_digest] = layer

    if image_manifest_desc is None:
        sys.exit("error: delta file has no embedded image manifest layer")
    if image_config_desc is None:
        sys.exit("error: delta file has no embedded image config layer")

    image_manifest = json.loads(idx.read(blob_path(image_manifest_desc['digest'])))
    image_config = json.loads(idx.read(blob_path(image_config_desc['digest'])))

    return idx, image_manifest, image_config, delta_manifest, delta_layer_by_to


# ── tar-diff op stream ────────────────────────────────────────────────────────

def _varint(buf):
    r, s = 0, 0
    while True:
        b = buf.read(1)
        if not b:
            raise EOFError
        x = b[0]
        r |= (x & 0x7f) << s
        if not x & 0x80:
            return r
        s += 7


def iter_ops(blob):
    dctx = zstd.ZstdDecompressor()
    with dctx.stream_reader(io.BytesIO(blob[8:])) as raw:
        buf = io.BufferedReader(raw, buffer_size=1 << 20)
        while True:
            b = buf.read(1)
            if not b:
                return
            op = b[0]
            size = _varint(buf)
            if op in (OP_DATA, OP_ADDDATA, OP_OPEN):
                yield op, size, buf.read(size)
            else:
                yield op, size, None


# ── output tar stream simulation ──────────────────────────────────────────────

class FileEntry:
    __slots__ = ('name', 'size', 'typeflag', 'copy_bytes', 'adddata_bytes',
                 'data_bytes', 'sources', 'payload_data', 'compressed_payload')

    def __init__(self, name, size, typeflag='0'):
        self.name = name
        self.size = size
        self.typeflag = typeflag
        self.copy_bytes = 0
        self.adddata_bytes = 0
        self.data_bytes = 0
        self.sources = []  # ordered unique source paths used for this file's data
        self.payload_data = bytearray()  # adddata diff bytes + raw data bytes
        self.compressed_payload = 0

    @property
    def is_regular(self):
        return self.typeflag in ('0', '\0', '') and self.size > 0

    @property
    def reuse_type(self):
        if self.copy_bytes + self.adddata_bytes + self.data_bytes == 0:
            return 'new'
        if self.adddata_bytes == 0 and self.data_bytes == 0:
            return 'reused'
        if self.copy_bytes == 0 and self.adddata_bytes == 0:
            return 'new'
        return 'delta'


def _parse_pax(data):
    out = {}
    text = data.decode('utf-8', errors='replace')
    i = 0
    while i < len(text):
        sp = text.find(' ', i)
        if sp < 0:
            break
        try:
            length = int(text[i:sp])
        except ValueError:
            break
        kv = text[sp + 1:i + length].rstrip('\n')
        eq = kv.find('=')
        if eq >= 0:
            out[kv[:eq]] = kv[eq + 1:]
        i += length
    return out


def analyze_layer(blob):
    """Simulate the output tar stream from tar-diff ops, return (files, hardlinks).

    hardlinks maps ostree object path -> list of user-visible paths that hardlink to it.
    """
    files = []
    hardlinks = {}  # ostree_object_path -> [usr/lib64/libfoo.so, ...]

    state = 'header'
    hbuf = bytearray()
    current_file = None
    data_rem = 0
    pad_rem = 0
    current_src = None
    ext_buf = bytearray()
    pending_name = None

    def _start_entry(name, size, typeflag):
        nonlocal state, current_file, data_rem, pad_rem
        entry = FileEntry(name, size, typeflag)
        files.append(entry)
        current_file = entry if entry.is_regular else None
        data_rem = size
        pad_rem = pad_for(size)
        if size > 0:
            state = 'data'
        elif pad_rem:
            state = 'padding'
        else:
            state = 'header'

    def _parse_hbuf():
        nonlocal state, hbuf, pending_name, current_file, data_rem, pad_rem, ext_buf
        buf = bytes(hbuf)
        hbuf = bytearray()
        if all(b == 0 for b in buf):
            state = 'header'
            return
        name = buf[:100].rstrip(b'\x00').decode('utf-8', errors='replace')
        magic = buf[257:263]
        if magic in (b'ustar ', b'ustar\x00'):
            prefix = buf[345:500].rstrip(b'\x00').decode('utf-8', errors='replace')
            if prefix:
                name = prefix + '/' + name
        sf = buf[124:136]
        size = int.from_bytes(sf[1:12], 'big') if sf[0] & 0x80 else \
               int(sf.rstrip(b'\x00 ') or b'0', 8)
        typeflag = chr(buf[156]) if buf[156] else '0'

        if typeflag == 'L':  # GNU long name
            data_rem = size
            pad_rem = pad_for(size)
            ext_buf = bytearray()
            state = 'gnu_name'
            return
        if typeflag in ('x', 'g'):  # PAX extended header
            data_rem = size
            pad_rem = pad_for(size)
            ext_buf = bytearray()
            state = 'pax'
            return

        if pending_name is not None:
            name = pending_name
            pending_name = None

        if typeflag == '1':  # hardlink
            linkname = buf[157:257].rstrip(b'\x00').decode('utf-8', errors='replace')
            # ostree encodes the object hash in the link name as "path\x00hash.file";
            # take only the part before the null byte
            display = name.split('\x00')[0]
            if linkname and display:
                hardlinks.setdefault(linkname, []).append(display)
            state = 'header'
            return

        _start_entry(name, size, typeflag)

    def feed_literal(data):
        nonlocal state, hbuf, current_file, data_rem, pad_rem
        nonlocal ext_buf, pending_name, current_src
        i = 0
        while i < len(data):
            if state == 'header':
                need = TAR_BLOCK - len(hbuf)
                chunk = data[i:i + need]
                hbuf.extend(chunk)
                i += len(chunk)
                if len(hbuf) == TAR_BLOCK:
                    _parse_hbuf()

            elif state in ('gnu_name', 'pax'):
                take = min(data_rem, len(data) - i)
                ext_buf.extend(data[i:i + take])
                i += take
                data_rem -= take
                if data_rem == 0:
                    nonlocal_pending = None
                    if state == 'gnu_name':
                        nonlocal_pending = ext_buf.rstrip(b'\x00').decode('utf-8', errors='replace')
                    else:
                        recs = _parse_pax(bytes(ext_buf))
                        nonlocal_pending = recs.get('path')
                    pending_name = nonlocal_pending
                    state = 'ext_pad' if pad_rem else 'header'

            elif state == 'ext_pad':
                take = min(pad_rem, len(data) - i)
                i += take
                pad_rem -= take
                if pad_rem == 0:
                    state = 'header'

            elif state == 'data':
                take = min(data_rem, len(data) - i)
                if current_file is not None:
                    current_file.data_bytes += take
                    current_file.payload_data.extend(data[i:i + take])
                i += take
                data_rem -= take
                if data_rem == 0:
                    current_file = None
                    state = 'padding' if pad_rem else 'header'

            elif state == 'padding':
                take = min(pad_rem, len(data) - i)
                i += take
                pad_rem -= take
                if pad_rem == 0:
                    state = 'header'

    def advance_reused(n, op, adddata=None):
        nonlocal state, current_file, data_rem, pad_rem, current_src
        rem = n
        src_off = 0
        while rem > 0:
            if state == 'data':
                take = min(data_rem, rem)
                if current_file is not None:
                    if op == OP_COPY:
                        current_file.copy_bytes += take
                    else:
                        current_file.adddata_bytes += take
                        if adddata is not None:
                            current_file.payload_data.extend(adddata[src_off:src_off + take])
                    if current_src and (not current_file.sources or current_file.sources[-1] != current_src):
                        current_file.sources.append(current_src)
                rem -= take
                src_off += take
                data_rem -= take
                if data_rem == 0:
                    current_file = None
                    state = 'padding' if pad_rem else 'header'
            elif state == 'padding':
                take = min(pad_rem, rem)
                rem -= take
                pad_rem -= take
                if pad_rem == 0:
                    state = 'header'
            else:
                break

    for op, size, data in iter_ops(blob):
        if op == OP_DATA:
            feed_literal(data)
        elif op == OP_OPEN:
            current_src = data.decode('utf-8')
        elif op == OP_COPY:
            advance_reused(size, OP_COPY)
        elif op == OP_ADDDATA:
            advance_reused(size, OP_ADDDATA, data)
        # OP_SEEK: no output bytes

    return files, hardlinks


# ── reporting ─────────────────────────────────────────────────────────────────

def report(delta_path, verbose):
    idx, image_manifest, image_config, delta_manifest, delta_layer_by_to = open_delta(delta_path)
    diff_ids = image_config['rootfs']['diff_ids']
    layers = image_manifest['layers']
    total_size = os.path.getsize(delta_path)
    delta_annotations = delta_manifest.get('annotations') or {}

    reused_digests = json.loads(delta_annotations.get(ANNOTATION_DELTA_REUSED, '[]'))
    reused_diff_ids = json.loads(delta_annotations.get(ANNOTATION_DELTA_REUSED_DIFF_ID, '[]'))

    # Build per-layer created_by from history, skipping empty_layer entries
    history = image_config.get('history', [])
    layer_created_by = [h.get('created_by', '') for h in history if not h.get('empty_layer')]

    print(f"Delta: {delta_path}  ({fmt_size(total_size)})")
    print(f"Layers: {len(layers)} total"
          f"  (reused: {len(reused_digests)}, changed: {len(delta_layer_by_to)})")
    print()

    total_orig = 0
    total_blob = 0

    for i, layer in enumerate(layers):
        digest = layer['digest']
        diff_id = diff_ids[i] if i < len(diff_ids) else '?'
        created_by = layer_created_by[i] if i < len(layer_created_by) else None
        orig_size = layer.get('size', 0)
        delta_layer = delta_layer_by_to.get(digest)

        print(f"Layer {i + 1}/{len(layers)}")
        if created_by:
            print(f"  Created by: {created_by}")

        if delta_layer is None:
            print(f"  Status:  skipped (reused from parent)")
            print(f"  Digest:  {digest}")
            print(f"  diff_id: {diff_id}")
        else:
            delta_blob_digest = delta_layer['digest']
            delta_blob_size = delta_layer.get('size', 0)
            is_tar_diff = delta_layer.get('mediaType') == MEDIA_TAR_DIFF

            annotations = delta_layer.get('annotations') or {}
            from_digests = json.loads(annotations.get(ANNOTATION_DELTA_FROM, '[]'))
            if isinstance(from_digests, str):
                from_digests = [from_digests]

            if not is_tar_diff:
                print(f"  Status:  original (tar-diff not smaller)")
                print(f"  Size:    {fmt_size(delta_blob_size)}")
                print(f"  Digest:  {digest}")
                print(f"  diff_id: {diff_id}")
                total_orig += orig_size
                total_blob += delta_blob_size
            else:
                saved = orig_size - delta_blob_size
                print(f"  Status:  delta")
                print(f"  Blob size:     {fmt_size(delta_blob_size)}"
                      f"  (original: {fmt_size(orig_size)}, saved: {fmt_size(saved)} / {pct(saved, orig_size)})")
                print(f"  Digest:  {digest}")
                print(f"  diff_id: {diff_id}")
                if from_digests:
                    print(f"  Sources: {', '.join(d[:19] for d in from_digests)}")
                total_orig += orig_size
                total_blob += delta_blob_size

                blob = idx.read(blob_path(delta_blob_digest))
                files, hardlinks = analyze_layer(blob)

                cctx = zstd.ZstdCompressor(level=3)
                for f in files:
                    if f.payload_data:
                        f.compressed_payload = len(cctx.compress(bytes(f.payload_data)))
                        f.payload_data = bytearray()  # free memory
                    else:
                        f.compressed_payload = 0

                def display_name(ostree_path):
                    for n in hardlinks.get(ostree_path, []):
                        if not n.startswith('sysroot/'):
                            return n
                    return os.path.basename(ostree_path)

                regular = [f for f in files if f.is_regular]

                n_full    = sum(1 for f in regular if f.reuse_type == 'reused')
                n_partial = sum(1 for f in regular if f.reuse_type == 'delta')
                n_new     = sum(1 for f in regular if f.reuse_type == 'new')

                seen = set()
                unique_sources = []
                for f in regular:
                    for s in f.sources:
                        if s not in seen:
                            unique_sources.append(s)
                            seen.add(s)

                print(f"  Regular files: {len(regular)}"
                      f"  (fully reused: {n_full},"
                      f"  partially reused: {n_partial},"
                      f"  new: {n_new})")
                print(f"  Unique source objects: {len(unique_sources)}")

                if verbose:
                    OSTREE_META_EXTS = {'.dirmeta', '.dirtree', '.commit', '.file-xattrs-link'}

                    def is_ostree_meta(f):
                        return os.path.splitext(f.name)[1] in OSTREE_META_EXTS

                    def source_display(sources):
                        if not sources:
                            return ''
                        # Try to resolve the first source to a user-visible name via hardlinks
                        name = display_name(sources[0]) if sources[0] in hardlinks else sources[0]
                        if len(sources) > 1:
                            name += f' (+{len(sources) - 1})'
                        return name

                    print()
                    w = 52
                    sw = 40
                    print(f"  {'File':<{w}} {'Size':>9}  {'Compressed':>10}  {'Type':<8}  Source")
                    print(f"  {'-'*w} {'-'*9}  {'-'*10}  {'-'*8}  {'-'*sw}")

                    meta = [f for f in files if f.is_regular and is_ostree_meta(f)]
                    for f in files:
                        if not f.is_regular or is_ostree_meta(f):
                            continue
                        rt = f.reuse_type
                        name = display_name(f.name) or f.name
                        if len(name) > w:
                            name = '…' + name[-(w - 1):]
                        src = source_display(f.sources)
                        if len(src) > sw:
                            src = '…' + src[-(sw - 1):]
                        print(f"  {name:<{w}} {fmt_size(f.size):>9}  {fmt_size(f.compressed_payload):>10}  {rt:<8}  {src}")

                    if meta:
                        ma = sum(f.adddata_bytes for f in meta)
                        md = sum(f.data_bytes for f in meta)
                        ms = sum(f.size for f in meta)
                        mcp = sum(f.compressed_payload for f in meta)
                        mrt = 'reused' if not ma and not md else \
                              'new' if not ma else 'delta'
                        label = f"[metadata] ({len(meta)} objects)"
                        meta_sources = []
                        seen_ms = set()
                        for mf in meta:
                            for s in mf.sources:
                                if s not in seen_ms:
                                    meta_sources.append(s)
                                    seen_ms.add(s)
                        msrc = source_display(meta_sources)
                        if len(msrc) > sw:
                            msrc = '…' + msrc[-(sw - 1):]
                        print(f"  {label:<{w}} {fmt_size(ms):>9}  {fmt_size(mcp):>10}  {mrt:<8}  {msrc}")

        print()

    if total_orig:
        saved = total_orig - total_blob
        print(f"Processed layers total: {fmt_size(total_blob)} stored"
              f"  (original: {fmt_size(total_orig)}, saved: {fmt_size(saved)} / {pct(saved, total_orig)})")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('delta', help="path to the bootc-delta file")
    ap.add_argument('--verbose', '-v', action='store_true',
                    help="list all regular files in each delta layer")
    args = ap.parse_args()
    if not os.path.exists(args.delta):
        sys.exit(f"error: {args.delta}: not found")
    report(args.delta, args.verbose)


if __name__ == '__main__':
    main()
