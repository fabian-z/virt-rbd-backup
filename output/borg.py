import subprocess
from datetime import datetime
import shutil

BACKUP_BASE = "rbd_backup"


def backup(target_repo, src, filename=None, progress=False):
    timestamp = datetime.utcnow().strftime('%Y_%m_%d_%s')
    target = target_repo+"::"+BACKUP_BASE+"-"+timestamp
    process = subprocess.Popen(("/usr/bin/borg", "create", "-p", target, "-"),
                               stdin=subprocess.PIPE, stdout=subprocess.DEVNULL)

    shutil.copyfileobj(src, process.stdin)
    process.stdin.close()

    return_code = process.wait()
    if return_code != 0:
        print("Backup error, see log")
    else:
        print("Backup successful")
