#!/usr/bin/env python3
import virtual.virt as virt

conn = virt.VirtConnection("qemu:///system")
conn.open()
images = virt.list_virtrbd_images(conn)
for image in images:
    if image.domain.isActive():
        image.domain.fsFreeze()
        # create snapshot
        image.domain.fsThaw()


conn.close()