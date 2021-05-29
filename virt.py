#!/usr/bin/env python3
"""virt module providing a connection to libvirt hypervisor management"""

from xml.etree import ElementTree
from dataclasses import dataclass

import libvirt


class LibvirtConnectionException(Exception):
    """Exception raised when methods are called on a closed connection."""
    pass


@dataclass
class VirtRBDImage:
    """VirtRBDImage represents an RBD image attached to the libvirt domain specified by domain."""
    domain: str  # Given with UUID string
    name: str
    pool: str
    username: str
    secret: bytes
    snapshot_name: str = ""


@dataclass
class VirtConnection:
    """VirtConnection represents an connection to specific hypervisor"""
    connection_string: str
    conn: libvirt.virConnect = None

    def open(self):
        """Open the hypervisor management connection"""
        self.conn = libvirt.open(self.connection_string)

    def close(self):
        """Close the hypervisor management connection, marking the connection
        as closed using None"""
        if self.conn != None:
            self.conn.close()
            self.conn = None

    def __getattr__(self, attr):
        """___getattr__ is called when the method or attribute does not exist in this class,
        redirecting to the embbeded conn. It is used to present VirtConnection as a superset
        of libvirt.virConnect"""
        if self.conn == None:
            raise LibvirtConnectionException("invalid connection")
        return getattr(self.conn, attr)

    def list_virtrbd_images(self):
        """Main function listing defined virtual machines and their relevant RBD images
        with location (pool) and authentication information.
        Data is recorded and returned in an array of VirtRBDImage instances."""
        images_list = []
        domains = self.listAllDomains(0)
        for dom in domains:
            # locking not supported by libvirt API..
            raw_xml = dom.XMLDesc(0)
            tree = ElementTree.fromstring(raw_xml)
            disks = tree.findall('devices/disk')

            for disk in disks:
                disk_type = disk.get("type")

                if disk_type != "network":
                    print("Ignoring non-network disk for domain: "+dom.name())
                    continue

                # ElementTree.dump(disk)

                # Get disk details
                source = disk.find("source")
                protocol = source.get("protocol")

                if protocol != "rbd":
                    print("Ignoring non-RBD network disk for domain: "+dom.name())
                    continue

                name = source.get("name")
                [pool, image] = name.split("/")

                # Get auth information
                auth = disk.find("auth")
                rbd_username = auth.get("username")

                secret = auth.find("secret")
                secret_uuid = secret.get("uuid")

                libvirt_secret = self.secretLookupByUUIDString(secret_uuid)
                rbd_secret = libvirt_secret.value()

                images_list.append(
                    VirtRBDImage(dom.UUIDString(), image, pool, rbd_username, rbd_secret))

        return images_list
