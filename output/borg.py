import subprocess
from datetime import datetime
import shutil

BACKUP_BASE = "rbd_backup"
CHUNK_SIZE = 16384 # 16kb chunks

class BackupException(Exception):
    pass

def backup(target_repo, src, filename=None, progress=False):
    timestamp = datetime.utcnow().strftime('%Y_%m_%d_%s')
    target = target_repo+"::"+BACKUP_BASE+"-"+timestamp
    process = subprocess.Popen(("/usr/bin/borg", "create", "-p", target, "-"),
                               stdin=subprocess.PIPE, stdout=subprocess.DEVNULL)

    size = src.size()

    offset = 0
    write_last = False
    while True:
        if offset + CHUNK_SIZE > size:
            last_chunk = size - offset
            data = src.read(offset, last_chunk)
            write_last = True
        else:
            data = src.read(offset, CHUNK_SIZE)

        offset += len(data)
        written = process.stdin.write(data)

        if written != len(data):
            raise BackupException("Short write")

        if write_last:
            break
    
    process.stdin.close()

    return_code = process.wait()
    if return_code != 0:
        print("Backup error, see log")
    else:
        print("Backup successful")