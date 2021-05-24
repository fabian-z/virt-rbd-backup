#!/usr/bin/env python3
import sys
from datetime import datetime

import virt
import ceph
import output.borg as borg

virt_conn = virt.VirtConnection("qemu:///system")
virt_conn.open()

try:
    images = virt.list_virtrbd_images(virt_conn)
    for image in images:
        # freeze = image.domain.isActive()
        freeze = False
        frozen = False
        if freeze:
            try:
                image.domain.fsFreeze()
                frozen = True
            except Exception as e:
                print("Error freezing guest FS - continue with hot snapshot:", e)
                frozen = False

        storage_conn = ceph.CephConnection(image.username, image.secret)
        try:
            storage_conn.connect()
            storage_conn.pool_exists(image.pool)
            storage_conn.open_pool(image.pool)
            storage_conn.open_image(image.name)
            timestamp = datetime.utcnow().strftime('%Y_%m_%d_%s')
            snapshot_name = image.name+"-backup-"+timestamp
            storage_conn.create_snapshot(snapshot_name, protected=True)
            storage_conn.close_image()
            storage_conn.open_image(
                image.name, snapshot=snapshot_name, read_only=True)

            borg.backup("testrepo", storage_conn.image,
                        filename="stdin", progress=True)

            storage_conn.close_image()
            storage_conn.open_image(image.name)
            storage_conn.remove_snapshot(
                snapshot_name, force_protected=True)
        except Exception as e:
            print("Error creating snapshot or backup for image: ", image.name)
            print("Exception occured: ", e)
        finally:
            storage_conn.close()
            if frozen:
                try:
                    image.domain.fsThaw()
                except Exception as e:
                    print("Error thawing guest FS - guest may be unresponsive:", e)
finally:
    print("Closing libvirt connection")
    virt_conn.close()
