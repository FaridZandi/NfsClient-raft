import time
import signal
from contextlib import contextmanager
from pyNfsClient import (Mount, NFSv3, MNT3_OK, NFS3_OK, DATA_SYNC)


class OperationTimeout(Exception):
    pass


@contextmanager
def timeout(seconds):
    """Context manager to timeout blocking socket operations."""
    def _handle(signum, frame):
        raise OperationTimeout()

    previous = signal.signal(signal.SIGALRM, _handle)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)

host = "10.70.10.110"
mount_path = "/srv/nfs/sharedfarid"

auth = {"flavor": 1,
        "machine_name": "sim-08",
        "uid": 6120,
        "gid": 30142,
        "aux_gid": list(),
        }


dir_name = "dir8"
reps = 10
CREATE_UNCHECKED = 0  # From NFSv3 spec
TIMEOUT=1

# portmap = Portmap(host, timeout=3600)
# portmap.connect()
# mnt_port = portmap.getport(Mount.program, Mount.program_version)

mnt_port = 2049


class NFSWrapper:
    def __init__(self, host, mount_path, auth, dir_name, mnt_port=2049, nfs_port=2049, timeout=1):
        self.host = host
        self.mount_path = mount_path
        self.auth = auth
        self.dir_name = dir_name
        self.mnt_port = mnt_port
        self.nfs_port = nfs_port
        self.timeout = timeout
        self.mount = None
        self.nfs3 = None
        self.root_fh = None
        self.dir_fh = None
        self.remount()

    def _disconnect(self):
        if self.nfs3:
            try:
                self.nfs3.disconnect()
            except Exception:
                pass
        if self.mount:
            try:
                self.mount.umnt()
                self.mount.disconnect()
            except Exception:
                pass

    def remount(self):
        self._disconnect()
        self.mount = Mount(host=self.host, auth=self.auth, port=self.mnt_port, timeout=self.timeout)
        self.mount.connect()
        mnt_res = self.mount.mnt(self.mount_path, self.auth)
        if mnt_res["status"] != MNT3_OK:
            raise Exception(f"Mount failed: {mnt_res['status']}")
        self.root_fh = mnt_res["mountinfo"]["fhandle"]
        self.nfs3 = NFSv3(self.host, self.nfs_port, timeout=self.timeout, auth=self.auth)
        self.nfs3.connect()
        # make sure directory handle is valid
        self.execute(lambda: self.nfs3.mkdir(self.root_fh, self.dir_name, mode=0o777, auth=self.auth))
        lookup = self.execute(lambda: self.nfs3.lookup(self.root_fh, self.dir_name, self.auth))
        if lookup["status"] != NFS3_OK:
            raise Exception("Cannot find or create target directory")
        self.dir_fh = lookup["resok"]["object"]["data"]

    def execute(self, op):
        while True:
            try:
                with timeout(self.timeout):
                    return op()
            except OperationTimeout:
                print("Operation timed out. Remounting and retrying ...")
            except Exception as exc:
                print(f"Operation failed ({exc}). Remounting and retrying ...")
            self.remount()

    def close(self):
        self._disconnect()


def test_logic(client):
    for x in range(1, reps + 1):
        filename = f"file{x}.txt"
        file_content = f"this is file number {x}"
        new_filename = f"renamed_file{x}.txt"

        create_res = yield lambda: client.nfs3.create(client.dir_fh, filename, CREATE_UNCHECKED, auth=auth)
        if create_res["status"] != NFS3_OK:
            print(f"Create failed for {filename}: {create_res['status']}")
            continue

        rename_res = yield lambda: client.nfs3.rename(client.dir_fh, filename, client.dir_fh, new_filename, auth=auth)
        if rename_res["status"] != NFS3_OK:
            print(f"Rename failed for {filename}: {rename_res['status']}")
            continue

        renamed_lookup = yield lambda: client.nfs3.lookup(client.dir_fh, new_filename, auth)
        if renamed_lookup["status"] != NFS3_OK:
            print(f"Lookup failed for {new_filename}: {renamed_lookup['status']}")
            continue
        file_fh = renamed_lookup["resok"]["object"]["data"]

        write_res = yield lambda: client.nfs3.write(file_fh, offset=0, count=len(file_content),
                                                   content=file_content, stable_how=DATA_SYNC, auth=auth)
        if write_res["status"] != NFS3_OK:
            print(f"Write failed for {new_filename}: {write_res['status']}")

        print("waiting ...")
        time.sleep(1)


def run(client, logic):
    gen = logic(client)
    result = None
    while True:
        try:
            op = gen.send(result)
        except StopIteration:
            break
        result = client.execute(op)


if __name__ == "__main__":
    client = NFSWrapper(host, mount_path, auth, dir_name, mnt_port=mnt_port, nfs_port=2049, timeout=TIMEOUT)
    try:
        run(client, test_logic)
    finally:
        client.close()
