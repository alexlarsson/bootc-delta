#!/usr/bin/env python3
"""Create a bare-user ostree repo from an OCI archive.

The resulting repo has the same layout as a bootc ostree repo, with the
image manifest stored as commit metadata under an ostree/container/image ref.

Usage: create-ostree-repo.py <oci-archive> <output-repo>
"""

import argparse
import json
import os
import shutil
import subprocess
import tarfile
import tempfile


def read_tar_member(archive, name):
    with archive.extractfile(name) as f:
        return f.read()


def main():
    parser = argparse.ArgumentParser(
        description='Create a bare-user ostree repo from an OCI archive')
    parser.add_argument('oci_archive', help='Path to the OCI archive')
    parser.add_argument('output_repo', help='Path for the output ostree repo')
    args = parser.parse_args()

    oci_archive = os.path.abspath(args.oci_archive)

    with tarfile.open(oci_archive) as archive:
        index = json.loads(read_tar_member(archive, 'index.json'))
        manifest_desc = index['manifests'][0]
        manifest_digest = manifest_desc['digest']
        manifest_blob = f"blobs/sha256/{manifest_digest.split(':')[1]}"
        manifest_data = read_tar_member(archive, manifest_blob)
        manifest = json.loads(manifest_data)

        config_digest = manifest['config']['digest']
        manifest_json = manifest_data.decode()

        print(f"Image has {len(manifest['layers'])} layers")

    with tempfile.TemporaryDirectory(prefix='create-ostree-repo-') as tmpdir:
        rootfs = os.path.join(tmpdir, 'rootfs')
        os.makedirs(rootfs)

        print("Creating container from image...")
        result = subprocess.run(
            ['podman', 'create', f'oci-archive:{oci_archive}'],
            capture_output=True, text=True, check=True)
        container_id = result.stdout.strip()

        try:
            print("Exporting container filesystem...")
            export = subprocess.Popen(
                ['podman', 'export', container_id],
                stdout=subprocess.PIPE)
            subprocess.run(
                ['tar', '-xf', '-', '-C', rootfs],
                stdin=export.stdout, check=True)
            export.wait()
            if export.returncode != 0:
                raise subprocess.CalledProcessError(export.returncode, 'podman export')
        finally:
            subprocess.run(['podman', 'rm', container_id],
                           capture_output=True)

        # Remove /sysroot (bootc doesn't include it in the commit)
        sysroot = os.path.join(rootfs, 'sysroot')
        if os.path.exists(sysroot):
            shutil.rmtree(sysroot)

        # Ensure all files are readable (some ostree objects may have mode 000)
        subprocess.run(['chmod', '-R', 'u+r', rootfs], check=True)

        subprocess.run(
            ['ostree', 'init', '--mode=bare-user', f'--repo={args.output_repo}'],
            check=True)

        ref = 'ostree/container/image/oci'
        cmd = [
            'ostree', 'commit',
            f'--repo={args.output_repo}',
            f'--branch={ref}',
            f'--add-metadata-string=ostree.manifest={manifest_json}',
            f'--canonical-permissions',
            f'--tree=dir={rootfs}',
        ]

        print("Committing to ostree repo...")
        subprocess.run(cmd, check=True)

    print(f"Created ostree repo at {args.output_repo}")
    print(f"  Ref: {ref}")
    print(f"  Config digest: {config_digest}")


if __name__ == '__main__':
    main()
