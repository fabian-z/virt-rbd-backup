from dataclasses import dataclass
import base64

import rados
import rbd


class CephConnectionException(Exception):
    pass


@dataclass
class CephConnection:
    """ Helper class to abstract API state for RADOS and RBD, with only a single cluster / pool / image
    opened at the same time. Improves structuring and reasoning about edge cases / closing handles"""
    rados_id: str  # will be admin if None
    key: bytes  # will be base64 encoded

    config: str = "/etc/ceph/ceph.conf"
    cluster: rados.Rados = None
    ioctx: rados.Ioctx = None
    image: rbd.Image = None

    def __init__(self, rados_id: str, key: bytes, config: str = "/etc/ceph/ceph.conf"):
        self.rados_id = rados_id
        self.key = key
        self.config = config

        enc_key = base64.b64encode(self.key).decode('utf-8')
        self.cluster = rados.Rados(conffile=self.config, rados_id=self.rados_id,
                                   conf=dict(key=enc_key))

    def close_image(self):
        if self.image != None:
            self.image.close()
            self.image = None

    def close_pool(self):
        if self.ioctx != None:
            self.ioctx.close()
            self.ioctx = None

    def close_cluster(self):
        # TODO check if cluster should be reused?
        if self.cluster != None:
            self.cluster.shutdown()
            self.cluster = None

    def close(self):
        self.close_image()
        self.close_pool()
        self.close_cluster()

    def connect(self):
        print("Connecting to Ceph monitors: {}".format(
            str(self.cluster.conf_get('mon host'))))
        self.cluster.connect()
        self.cluster.require_state("connected")
        print("Connected to Ceph Cluster ID: {}".format(self.cluster.get_fsid()))

    # Helper functions to establish state
    def require_cluster_connection(self):
        if self.cluster == None:
            raise CephConnectionException("not connected to cluster")

    def require_pool_opened(self):
        self.require_cluster_connection()
        if self.ioctx == None:
            raise CephConnectionException("no pool opened")

    def require_image_opened(self):
        self.require_pool_opened()
        if self.image == None:
            raise CephConnectionException("no image opened")

    def print_stats(self):
        self.require_cluster_connection()
        print("Ceph Statistics:")
        stats = self.cluster.get_cluster_stats()
        for key, value in stats.items():
            print(key, value)

    def print_pools(self):
        self.require_cluster_connection()
        pools = self.cluster.list_pools()
        for pool in pools:
            print(pool)

    def pool_exists(self, pool_name):
        self.require_cluster_connection()
        return self.cluster.pool_exists(pool_name)

    def open_pool(self, pool_name):
        self.require_cluster_connection()
        self.ioctx = self.cluster.open_ioctx(pool_name)

    def open_image(self, image_name, snapshot=None, read_only=False):
        self.require_pool_opened()
        self.image = rbd.Image(self.ioctx, name=image_name,
                               snapshot=snapshot, read_only=read_only)

    def create_snapshot(self, snapshot_name, protected=False):
        self.require_image_opened()
        self.image.create_snap(snapshot_name)
        if protected:
            self.image.protect_snap(snapshot_name)

    def remove_snapshot(self, snapshot_name, force_protected=False):
        self.require_image_opened()
        if self.image.is_protected_snap(snapshot_name) and force_protected:
            self.image.unprotect_snap(snapshot_name)
        self.image.remove_snap(snapshot_name)
