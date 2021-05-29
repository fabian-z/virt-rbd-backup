#!/usr/bin/env python3
import sys
from datetime import datetime

# Multiprocessing from Python documentationen for module multiprocessing
# https://docs.python.org/3/library/multiprocessing.html
import multiprocessing

import virt
import ceph
import output.restic as restic

NUMBER_OF_PROCESSES = 4

# TODO configuration with multiple backends?
TARGET_REPO = "/path/to/repository"
TARGET_KEYFILE = "/path/to/keyfile"

# Function run by worker processes


def worker(input, output):
    for image in iter(input.get, None):
        print(f"Processing {image.name} for domain {image.domain}")
        result = process_backup(image)
        output.put(result)

# List images and start parallel backup operations


def run_parallel():
    virt_conn = virt.VirtConnection("qemu:///system")
    virt_conn.open()
    images = []
    try:
        images = virt_conn.list_virtrbd_images()
    finally:
        virt_conn.close()

    # Create queues
    task_queue = multiprocessing.Queue()
    done_queue = multiprocessing.Queue()

    # Submit tasks
    for image in images:
        # TODO map tasks for same VM to sequentially the same worker
        # and improve multi-image snapshot coherency
        task_queue.put(image)

    # Start worker processes
    for _ in range(NUMBER_OF_PROCESSES):
        multiprocessing.Process(target=worker, args=(
            task_queue, done_queue)).start()

    # Get and print results
    print('Unordered results:')
    for _ in range(len(images)):
        (result, text) = done_queue.get()
        if result:
            print(f"Backup successful: {text}")
        else:
            print(f"Backup failed: {text}")

    # Tell child processes to stop
    for _ in range(NUMBER_OF_PROCESSES):
        task_queue.put(None)


def process_backup(image):
    exceptions = []
    virt_conn = virt.VirtConnection("qemu:///system")
    try:
        virt_conn.open()
        domain = virt_conn.lookupByUUIDString(image.domain)
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
            storage_conn = ceph.CephConnection(image.username, image.secret)
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

            restic.backup(TARGET_REPO, TARGET_KEYFILE, storage_conn.image,
                          filename=image.name+".img", progress=True)

            storage_conn.close_image()
            storage_conn.open_image(image.name)
            storage_conn.remove_snapshot(
                snapshot_name, force_protected=True)
        except Exception as e:
            exceptions.append(
                (False, "Error creating snapshot or backup for image: " + image.name+". Exception: " + repr(e)))
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
        return (True, f"No error occured for image {image.name}")
    else:
        # Only give first exception for now
        return exceptions[0]


# Entrypoint definition
if __name__ == '__main__':
    multiprocessing.freeze_support()
    run_parallel()
