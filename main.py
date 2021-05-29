#!/usr/bin/env python3
"""Automatic backup of RBD images accessed by virtual machines run by QEMU and managed by libvirt
Uses libvirt Python API and Ceph librbd RBD Python API to list relevant virtual machines,
create a RBD snapshot and save the snapshot to defined output modules.
Configuration is done via config.py (copy from config.py.example)."""

from datetime import datetime

# Multiprocessing from Python documentationen for module multiprocessing
# https://docs.python.org/3/library/multiprocessing.html
import multiprocessing

import virt
import ceph
import output.restic as restic
from config import NUMBER_OF_PROCESSES, LIBVIRT_CONNECTION, TARGET_REPO, TARGET_KEYFILE


def worker(input, output):
    """Run by worker processes, executes backup processing for tasks from the queue"""
    for domainImage in iter(input.get, None):
        print(
            f"Processing {len(domainImage)} images for domain {domainImage[0].domain}")
        result = process_backup(domainImage)
        output.put(result)

# List images and start parallel backup operations


def run_parallel():
    """List images and start parallel backup operations with worker pool"""
    virt_conn = virt.VirtConnection(LIBVIRT_CONNECTION)
    images = []
    try:
        virt_conn.open()
        images = virt_conn.list_virtrbd_images()
    finally:
        virt_conn.close()

    domainImages = {}
    for image in images:
        cluster = domainImages.get(image.domain, [])
        cluster.append(image)
        domainImages[image.domain] = cluster

    # Create queues
    task_queue = multiprocessing.Queue()
    done_queue = multiprocessing.Queue()

    # Submit tasks
    for domain in domainImages:
        # TODO map tasks for same VM to sequentially the same worker
        # and improve multi-image snapshot coherency
        task_queue.put(domainImages[domain])

    # Start worker processes
    for _ in range(NUMBER_OF_PROCESSES):
        multiprocessing.Process(target=worker, args=(
            task_queue, done_queue)).start()

    # Get and print results
    for _ in range(len(images)):
        (result, text) = done_queue.get()
        if result:
            print(f"Backup successful: {text}")
        else:
            print(f"Backup failed: {text}")

    # Tell child processes to stop
    for _ in range(NUMBER_OF_PROCESSES):
        task_queue.put(None)


def process_backup(domainImages):
    """Process the backup of a set of domain images.
    Handles orchestration of other modules.
    Assumptions: Images belong to a single domain and have identical authentication to the Ceph cluster
    It is not required for images to be in the same pool"""
    exceptions = []
    virt_conn = virt.VirtConnection(LIBVIRT_CONNECTION)
    try:
        virt_conn.open()
        domain = virt_conn.lookupByUUIDString(domainImages[0].domain)
        # freeze = image.domain.isActive()
        freeze = False
        frozen = False
        if freeze:
            try:
                domain.fsFreeze()
                frozen = True
            except Exception as e:
                print("Error freezing guest FS - continue with hot snapshot: ", repr(e))
                frozen = False

        try:
            storage_conn = ceph.CephConnection(
                domainImages[0].username, domainImages[0].secret)
            storage_conn.connect()

            # First pass: Create backup snapshosts
            for image in domainImages:
                storage_conn.pool_exists(image.pool)
                storage_conn.open_pool(image.pool)
                storage_conn.open_image(image.name)
                timestamp = datetime.utcnow().strftime('%Y_%m_%d_%s')
                image.snapshot_name = image.name+"-backup-"+timestamp
                storage_conn.create_snapshot(
                    image.snapshot_name, protected=True)
                storage_conn.close_image()
                storage_conn.close_pool()

            # Second pass: Copy snapshot content to backup module
            for image in domainImages:
                storage_conn.open_pool(image.pool)
                storage_conn.open_image(
                    image.name, snapshot=image.snapshot_name, read_only=True)

                restic.backup(TARGET_REPO, TARGET_KEYFILE, storage_conn.image,
                              filename=image.name+".img", progress=True)

                storage_conn.close_image()
                storage_conn.open_image(image.name)
                storage_conn.remove_snapshot(
                    image.snapshot_name, force_protected=True)
                storage_conn.close_image()
                storage_conn.close_pool()

        except Exception as e:
            exceptions.append(
                (False, "Error creating snapshot or backup for domain: " + domainImages[0].domain + ". Exception: " + repr(e)))
            raise
        finally:
            storage_conn.close()
            if frozen:
                try:
                    domain.fsThaw()
                except Exception as e:
                    exceptions.append(
                        (False, "Error thawing guest FS - guest may be unresponsive: " + repr(e)))
                    raise
    except Exception as e:
        exceptions.append(
            (False, "Error during libvirt connection: " + repr(e)))

    finally:
        virt_conn.close()

    if len(exceptions) == 0:
        return (True, f"No error occured for domain {domainImages[0].domain}")
    else:
        # Only give first exception for now
        return exceptions[0]


# Entrypoint definition
if __name__ == '__main__':
    multiprocessing.freeze_support()
    run_parallel()
