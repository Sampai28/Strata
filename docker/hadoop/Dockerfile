# Official Apache Hadoop with two directories pre-created. No other changes.
#
# This exists to solve a specific interaction between the image's entrypoint and
# Docker named volumes, which cost an hour the first time and is invisible in
# the logs:
#
#   1. /opt/starter.sh formats the NameNode only when the directory named by
#      ENSURE_NAMENODE_DIR does NOT exist:
#
#          if [ ! -d "$ENSURE_NAMENODE_DIR" ]; then hdfs namenode -format -force
#
#      Mounting a named volume directly onto dfs.namenode.name.dir creates that
#      directory, so the check sees it, skips the format, and the NameNode then
#      dies with InconsistentFSStateException — a message that points at
#      corruption rather than at a volume mount.
#
#   2. Docker initialises a fresh named volume with the ownership of the image
#      path it is mounted over. /hadoop does not exist in the base image, so the
#      volume is created root-owned, and this image runs as uid 1000 (hadoop),
#      which then cannot write to its own storage directory.
#
# Creating an EMPTY /hadoop/dfs owned by hadoop fixes both at once. The volume
# mounts over /hadoop/dfs and inherits hadoop ownership, while /hadoop/dfs/name
# stays absent inside the fresh volume, so the format fires exactly once on a
# cold start and never again.
#
# Consequence worth knowing: removing the namenode-data volume is how you reset
# the filesystem, and it will re-format on the next start.

FROM apache/hadoop:3.3.6

USER root

RUN mkdir -p /hadoop/dfs /hadoop/tmp \
    && chown -R hadoop:users /hadoop \
    && chmod 0755 /hadoop /hadoop/dfs /hadoop/tmp

USER hadoop
