import subprocess

BACKUP_BASE = "rbd_backup"
CHUNK_SIZE = 4194304  # 4MB chunks
# target_repo can be local dir, or 
# e.g. sftp:user@host:/path/to/repo/
PW_LOCATION = "/path/to/pwfile"

class BackupException(Exception):
    pass


def backup(target_repo, src, filename="stdin", progress=False):
    # TODO add tagging?
    # Omit timestamp since restic backend does timestamp with snapshots
    # Consistent filenames should improve deduplication
    process = subprocess.Popen(("/usr/bin/restic", "-p", PW_LOCATION, "-r", target_repo, "backup", "--stdin", "--stdin-filename", filename),
                               stdin=subprocess.PIPE, stdout=subprocess.DEVNULL)

    # TODO could be refactored to I/O wrapper class
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

        # Calculating offset with read length handles short reads
        offset += len(data)
        written = process.stdin.write(data)

        if written != len(data):
            raise BackupException("Short write")

        if write_last:
            break

    process.stdin.close()

    return_code = process.wait()
    if return_code != 0:
        raise BackupException("Backup error, see log")
    else:
        print("Backup successful")
