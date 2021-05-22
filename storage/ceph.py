import rados
import rbd

#TODO change to try / finally or context managers, refactor to classes

cluster = rados.Rados(conffile='/etc/ceph/ceph.conf',
                      rados_id="libvirt", conf=dict(key="base64key"))
print("\nlibrados version: {}".format(str(cluster.version())))
print("Will attempt to connect to: {}".format(
    str(cluster.conf_get('mon host'))))

cluster.connect()
print("\nCluster ID: {}".format(cluster.get_fsid()))

print("\n\nCluster Statistics")
print("==================")
cluster_stats = cluster.get_cluster_stats()

for key, value in cluster_stats.items():
    print(key, value)

print("\n\nPool Operations")
print("===============")

print("\nPool named 'libvirt-pool' exists: {}".format(
    str(cluster.pool_exists('libvirt-pool'))))
print("\nVerify 'libvirt-pool' Pool Exists")
print("-------------------------")
pools = cluster.list_pools()

for pool in pools:
    print(pool)

ioctx = cluster.open_ioctx('libvirt-pool')
image = rbd.Image(ioctx, 'myimage')
image.create_snap("myimage-snapshotname")
image.protect_snap("myimage-snapshotname")

#TODO copy snapshot to backup output module, remove snapshot afterwards

# Does not actually shutdown the cluster, only disconnects RADOS client
image.close()
ioctx.close()
cluster.shutdown()
