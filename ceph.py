"""ceph module provides RADOS and RBD connections, snapshot management and I/O functionality"""
from dataclasses import dataclass
import base64

import rados
import rbd


class CephConnectionException(Exception):
    """Exception raised if method is called on a closed or invalid Ceph property"""
    pass


@dataclass
class CephConnection:
    """Helper class to abstract API state for RADOS and RBD, with only a single cluster / pool / image
    opened at the same time. Improves structuring and reasoning about edge cases / closing handles"""
    rados_id: str  # will be admin if None
    key: bytes  # will be base64 encoded

    config: str = "/etc/ceph/ceph.conf"
    cluster: rados.Rados = None
    ioctx: rados.Ioctx = None
    image: rbd.Image = None

    def __init__(self, rados_id: str, key: bytes, config: str = "/etc/ceph/ceph.conf"):
        """Initialize the CephConnection instance, and provide the RADOS object.
        Configuration is read here"""
        self.rados_id = rados_id
        self.key = key
        self.config = config

        enc_key = base64.b64encode(self.key).decode('utf-8')
        self.cluster = rados.Rados(conffile=self.config, rados_id=self.rados_id,
                                   conf=dict(key=enc_key))

    def close_image(self):
        """Close image if opened"""
        if self.image != None:
            self.image.close()
            self.image = None

    def close_pool(self):
        """Close pool if opened"""
        if self.ioctx != None:
            self.ioctx.close()
            self.ioctx = None

    def close_cluster(self):
        """Close cluster connection if opened"""
        # TODO check if cluster should be reused?
        if self.cluster != None:
            self.cluster.shutdown()
            self.cluster = None

    def close(self):
        """Close all opened ressources"""
        self.close_image()
        self.close_pool()
        self.close_cluster()

    def connect(self):
        """Establish connections to the Ceph monitors"""
        # print("Connecting to Ceph monitors: {}".format(
        #    str(self.cluster.conf_get('mon host'))))
        self.cluster.connect()
        self.cluster.require_state("connected")
        #print("Connected to Ceph Cluster ID: {}".format(self.cluster.get_fsid()))

    # Helper functions to establish state
    def require_cluster_connection(self):
        """Functional helper to require existance of a cluster connection in other methods"""
        if self.cluster == None:
            raise CephConnectionException("Not connected to cluster")

    def require_pool_opened(self):
        """Functional helper to require existance of a pool ioctx in other methods"""
        self.require_cluster_connection()
        if self.ioctx == None:
            raise CephConnectionException("No pool opened")

    def require_image_opened(self):
        """Functional helper to require an open image in other methods"""
        self.require_pool_opened()
        if self.image == None:
            raise CephConnectionException("No image opened")

    def print_stats(self):
        """Print statistics about the connected cluster"""
        self.require_cluster_connection()
        print("Ceph Statistics:")
        stats = self.cluster.get_cluster_stats()
        for key, value in stats.items():
            print(key, value)

    def print_pools(self):
        """Print list of available storage pools"""
        self.require_cluster_connection()
        pools = self.cluster.list_pools()
        for pool in pools:
            print(pool)

    def pool_exists(self, pool_name):
        """Check if specified pool_name exists on the cluster"""
        self.require_cluster_connection()
        return self.cluster.pool_exists(pool_name)

    def open_pool(self, pool_name):
        """Open a new pool ioctx"""
        self.require_cluster_connection()
        self.ioctx = self.cluster.open_ioctx(pool_name)

    def open_image(self, image_name, snapshot=None, read_only=False):
        """Open a new image"""
        self.require_pool_opened()
        self.image = rbd.Image(self.ioctx, name=image_name,
                               snapshot=snapshot, read_only=read_only)

    def create_snapshot(self, snapshot_name, protected=False):
        """Create a new image snapshot with the specified name and protection status"""
        self.require_image_opened()
        self.image.create_snap(snapshot_name)
        if protected:
            self.image.protect_snap(snapshot_name)

    def remove_snapshot(self, snapshot_name, force_protected=False):
        """Remove a specified snapshot, forcing removal of protected snapshots if requested"""
        self.require_image_opened()
        if self.image.is_protected_snap(snapshot_name) and force_protected:
            self.image.unprotect_snap(snapshot_name)
        self.image.remove_snap(snapshot_name)
