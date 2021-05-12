# virt-rbd-backup

Automatic backup of RBD images accessed by virtual machines run by QEMU and managed by libvirt

Uses libvirt Python API and Ceph librbd RBD Python API to list relevant virtual machines, create a RBD snapshot and save the snapshot to defined output.

# Project scope

- Dynamic list of relevant libvirt virtual machines
- Create / Process / Delete RBD snapshots
- Functionality for multiple RBD images per virtual machine
- Modular output in file or external processes (e.g. borg backup)
- If supported by guest operating system: Pause FS activity during snapshot (via QEMU guest agent)

# Out of scope

- Implementation of backup management
- Scheduling / Automation (external via systemd timer or cron)
- Cluster configuration or management

# References

[libvirt Python development guide](https://libvirt.org/docs/libvirt-appdev-guide-python/en-US/html/)

[RBD Python API reference](https://docs.ceph.com/en/latest/rbd/api/librbdpy/)

# Contributions

Contributions welcome - feel free to fork, experiment and open an issue and / or pull request.

# License

Apache License 2.0, see LICENSE
