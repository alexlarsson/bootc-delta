# bootc-delta

bootc-delta is a tool to take two oci archive files, called the "old and "new" image below)
containing bootc images and producing a resulting file, called a "delta" that can be used to update
a bootc host with the old image installed to the new image, without having the new oci archive
available.  The advantage of using the delta is that it is significantly smaller, as it avoids
shipping data that is already locally available from the installed old image.

## Mode of operation

An OCI image (and thus OCI archive) consists of some json metadata, and a list of compressed tar
files, one for each image layer. Each layer is references from the metadata twice, once (the digest
id) by sha256 digest of the compressed tar file, and once (the diff id) by the sha256 digest of the
uncompressed file. The later is important because various operations can cause layers to be
recompressed, but using the diff_id we can ensure we're referencing the same data.

When bootc installs a new image (both when pulling from a registry or from an OCI archive), it will
look at each layers digest id and diff_id comparing it to the set of already installed layers. If
there is a match, the layer in the image isn't even looked at.

This allows the first level of deltas in bootc-delta, we just generate a normal OCI image that
leaves out the tar files for the layers that will already be installed. Such an oci archive can be
installed with a command like `bootc switch --transport oci-archive $FILE`.

The above is helped by the layer chunking that is typically done for bootc images. At the build time
bootc tries to find related files (typically those installed from the same package), and puts them
in separate layers. If two different images install the same package version they are likely to end
up with an identical layer, even if the images are not directly related. This allows the first level
of delta support to be more efficient than you would otherwise thing.

The second level of delta is at the file level inside each layer. Even if a layer has changed, often
many files are identical, and others are similar to the previous version of the same file. On a
bootc system the layer files for an installed image are available in the ostree repository under
`/ostree/repo/objects`, even for images that are not the currently booted system. So, the idea is to
create a delta format that allows reconstructing the "delta level 1" oci archive file given the
information in the delta and the existing repo object files. Then the reconstructed oci archived
can be used with `bootc switch` to install the image.

To create these layer deltas, we use the [tar-diff](https://github.com/containers/tar-diff) tool
that creates binary deltas between tar files.

A typical bootc layer has content that looks something like this:
```
-rwxr-xr-x 0/0         1410656 1970-01-01 01:00 sysroot/ostree/repo/objects/8a/5d...d.file
hrwxr-xr-x 0/0               0 1970-01-01 01:00 usr/bin/bash link to sysroot/ostree/repo/objects/8a/5d...d.file
```

In other words, it has the ostree object file, as well as a hardlink to it with the deployed
path. This makes it easy to know which files can be used as sources for the deltas. We can just look
for a file prefix of `sysroot/ostree/repo/objects`.

To completely support what is required, tar-diff was [extended](https://github.com/containers/tar-diff/pull/66) to:
 * support the hardlinked structure of bootc images
 * support multiple "old" images (we can use all old layers as delta source material)
 * filtering the delta source files by prefix

Of course, some layers are bound to be completely new, so we only store the tar-diff for layers
where the diff is smaller than the original layer file.

There is one problem with this approach. The deltas (by their nature) work on the uncompressed layer
tar files, so we have to recompress after reconstruction. This means the reconstructed image will
have a manifest with different digest ids for the delta layers. The diff_id of the layers will be the
same though, so bootc (at least a recent enought version) will be able to know that these layers
are the same.

## Example usage

Create a bootc delta from two oci archive.
```
$ bootc-delta create old.oci-archive new.oci-archive update.bootc-delta
```

Apply a delta on a bootc system.
```
$ bootc-delta create update.bootc-delta new.oci-archive
$ bootc switch --transport=oci-archive new.oci-archive
$ rm new.oci-archive
```

## Delta sizes

Here are some example images and deltas between them. image is an automotive image, and image2 is a
similar build that adds a file and an extra package. oldimage is similar to base, but its older so
it will have older versions of some packages.

```
-rw-r--r--. 1 alex alex 318M 27 mar 11.30 oldimage.oci-archive
-rw-r--r--. 1 alex alex 306M 27 mar 14.25 image.oci-archive
-rw-r--r--. 1 alex alex 309M 27 mar 14.25 image2.oci-archive
```

Here are the delta sizes going between these.

```
-rw-r--r--. 1 alex alex  21M 27 mar 14.28 oldimage-to-image.delta
-rw-r--r--. 1 alex alex  16M 27 mar 14.30 image-to-image2.delta
```

I also computed a full delta between quay.io/fedora/fedora-bootc:41-x86_64 and quay.io/fedora/fedora-bootc:42-x86_64:

```
-rw-r--r--. 1 alex alex 972M 30 mar 11.37 fedora-bootc-41-x86_64.oci-archive
-rw-r--r--. 1 alex alex 999M 30 mar 11.37 fedora-bootc-42-x86_64.oci-archive
-rw-r--r--. 1 alex alex 555M 30 mar 11.56 fedora-41-to-42.delta
```

There is a script `tools/analyze-delta.py` in the repo that analyzes the produced deltas (with more
detail if you pass `--verbose`). This allows you to see in more detail what is being reused.


## Requirements

The target system has to run a bootc version 1.15.0 or later, that contains [the fix to use layer
diff_ids](https://github.com/bootc-dev/bootc/pull/2081).

The required support in tar-diff has been merged in [PR #66](https://github.com/containers/tar-diff/pull/66) and is
available in the current main branch. This is not yet in a release, but will be in the release after 0.3.1.
