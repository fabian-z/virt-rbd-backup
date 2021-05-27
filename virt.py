#!/usr/bin/env python3
import sys
from xml.etree import ElementTree
from dataclasses import dataclass

# Pydantic dataclasses implementation
# provides type validation at runtime
#from pydantic.dataclasses import dataclass

import libvirt


class LibvirtConnectionException(Exception):
    pass


@dataclass
class VirtRBDImage:
    """VirtRBDImage represents an RBD image attached to the libvirt domain specified by domain."""
    domain: str
    name: str
    pool: str
    username: str
    secret: bytes


@dataclass
class VirtConnection:
    """VirtConnection represents an connection to specific hypervisor"""
    connection_string: str
    conn: libvirt.virConnect = None

    def open(self):
        try:
            self.conn = libvirt.open(self.connection_string)
        except libvirt.libvirtError as e:
            print(repr(e), file=sys.stderr)
            sys.exit(1)

    def close(self):
        self.conn.close()
        self.conn = None

    def __getattr__(self, attr):
        """___getattr__ is called when the method or attribute does not exist in this class,
        redirecting to the embbeded conn. It is used to present VirtConnection as a superset
        of libvirt.virConnect"""
        print(attr)
        if self.conn == None:
            raise LibvirtConnectionException("invalid connection")
        return getattr(self.conn, attr)


def list_virtrbd_images(connection):

    images_list = []
    domains = connection.listAllDomains(0)
    for dom in domains:
        print("Processing domain: "+dom.name())
        # TODO add locking?
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

            libvirt_secret = connection.secretLookupByUUIDString(secret_uuid)
            rbd_secret = libvirt_secret.value()

            images_list.append(
                VirtRBDImage(dom.UUIDString(), image, pool, rbd_username, rbd_secret))

    return images_list
