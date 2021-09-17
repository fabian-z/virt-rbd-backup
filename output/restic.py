"""restic output module interfacing with https://github.com/restic/restic"""
import subprocess

CHUNK_SIZE = 4194304  # 4MB chunks
# target_repo can be local dir, or
# e.g. sftp:user@host:/path/to/repo/


class BackupException(Exception):
    """Exception raised when the backup process experiences a I/O or execution error"""
    pass


def backup(target_repo, keyfile, src, filename="stdin", progress=False):
    """Provide the backup module functionality.
    A subprocess is spawned and passed the data from src (Ceph connection with open image).
    Data is passed through a stdin pipe, the return code is evaluated to check for errors"""

    # TODO add tagging?
    process = subprocess.Popen(("restic", "--no-cache", "-p", keyfile, "-r", target_repo, "backup", "--stdin", "--stdin-filename", filename),
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
        raise BackupException(
            f"Backup error for repository {target_repo}, image {filename} - see log")
