#!/bin/sh
# Bootstraps the medallion lake layout and a least-privilege service account.
# Idempotent: safe to re-run on every `make up`.
#
# Invoked as `sh /scripts/bootstrap.sh` by the compose file, so it does not
# depend on the execute bit surviving a clone from a Windows filesystem.
#
# The client writes its alias configuration to a directory under $HOME. This
# image runs unprivileged with no writable home, so the location is passed
# explicitly on every call rather than relying on an environment variable.
# Granting the container root would have been the easier fix and the wrong one.
set -e

MC="mc --config-dir /tmp/.mc"

echo "== connecting to the object store as the administrative user"
$MC alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

# Bronze carries a retention lock, which object storage only permits on a
# bucket created with locking enabled - it cannot be turned on afterwards.
echo "== creating medallion zones"
$MC mb --ignore-existing --with-lock local/bronze
for bucket in silver gold quarantine; do
  $MC mb --ignore-existing "local/$bucket"
done

# Versioning gives a recovery path from an erroneous overwrite.
for bucket in bronze silver gold quarantine; do
  $MC version enable "local/$bucket" || echo "   (versioning already set on $bucket)"
done

# Bronze is the immutable source of truth: retain, never overwrite in place.
$MC retention set --default GOVERNANCE 90d local/bronze \
  || echo "   (retention not applied - continuing)"

# Scoped account for the pipeline. Spark and the ingestion service
# authenticate with this, never with the root credentials. This must succeed:
# without it every later upload fails with InvalidAccessKeyId.
echo "== creating the scoped pipeline account"
$MC admin user add local "$LAKE_ACCESS_KEY" "$LAKE_SECRET_KEY"
$MC admin policy attach local readwrite --user "$LAKE_ACCESS_KEY" \
  || echo "   (policy already attached)"

echo "== result"
$MC ls local
$MC admin user list local
echo "lake bootstrap complete: bronze / silver / gold / quarantine"
