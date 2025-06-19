import time
import signal
from contextlib import contextmanager

from pyNfsClient import Mount, NFSv3, MNT3_OK, NFS3_OK, DATA_SYNC

host = "10.70.10.110"
mount_path = "/srv/nfs/sharedfarid"

auth = {
    "flavor": 1,
    "machine_name": "sim-08",
    "uid": 6120,
    "gid": 30142,
    "aux_gid": list(),
}

dir_name = "dir8"
reps = 10
CREATE_UNCHECKED = 0
TIMEOUT = 1


class TimeoutError(Exception):
    pass


@contextmanager
def timeout(seconds):
    def handler(signum, frame):
        raise TimeoutError()

    previous = signal.signal(signal.SIGALRM, handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def run_with_timeout(func, seconds, *args, **kwargs):
    with timeout(seconds):
        return func(*args, **kwargs)


class NFSTester:
    def __init__(self, host, mount_path, auth, timeout=1):
        self.host = host
        self.mount_path = mount_path
        self.auth = auth
        self.timeout = timeout
        self.mount_port = 2049
        self.nfs_port = 2049
        self.mount = None
        self.nfs3 = None
        self.root_fh = None
        self.dir_fh = None

    def connect(self):
        def mount_connect():
            self.mount = Mount(host=self.host, auth=self.auth, port=self.mount_port, timeout=self.timeout)
            self.mount.connect()
            res = self.mount.mnt(self.mount_path, self.auth)
            if res["status"] != MNT3_OK:
                raise RuntimeError(f"Mount failed: {res['status']}")
            self.root_fh = res["mountinfo"]["fhandle"]

        run_with_timeout(mount_connect, self.timeout)

        def nfs_connect():
            self.nfs3 = NFSv3(self.host, self.nfs_port, timeout=self.timeout, auth=self.auth)
            self.nfs3.connect()

        run_with_timeout(nfs_connect, self.timeout)
        self._prepare_dir()

    def disconnect(self):
        if self.nfs3:
            try:
                run_with_timeout(self.nfs3.disconnect, self.timeout)
            except Exception:
                pass
            self.nfs3 = None
        if self.mount:
            try:
                run_with_timeout(self.mount.umnt, self.timeout)
            except Exception:
                pass
            try:
                run_with_timeout(self.mount.disconnect, self.timeout)
            except Exception:
                pass
            self.mount = None

    def remount(self):
        print("[INFO] Remounting filesystem ...")
        self.disconnect()
        time.sleep(1)
        self.connect()

    def _prepare_dir(self):
        run_with_timeout(lambda: self.nfs3.mkdir(self.root_fh, dir_name, mode=0o777, auth=self.auth), self.timeout)
        lookup = run_with_timeout(lambda: self.nfs3.lookup(self.root_fh, dir_name, self.auth), self.timeout)
        if lookup["status"] != NFS3_OK:
            raise RuntimeError("Cannot find or create target directory")
        self.dir_fh = lookup["resok"]["object"]["data"]

    def op(self, func, *args, **kwargs):
        try:
            return run_with_timeout(lambda: func(*args, **kwargs), self.timeout)
        except TimeoutError:
            print(f"[WARN] Operation {func.__name__} timed out")
            self.remount()
            return None


def main():
    tester = NFSTester(host, mount_path, auth, TIMEOUT)
    tester.connect()
    try:
        for x in range(1, reps + 1):
            filename = f"file{x}.txt"
            content = f"this is file number {x}"
            new_name = f"renamed_file{x}.txt"

            print("create ...")
            create_res = tester.op(tester.nfs3.create, tester.dir_fh, filename, CREATE_UNCHECKED, auth=auth)
            if not create_res or create_res["status"] != NFS3_OK:
                print(f"Create failed for {filename}")
                continue

            print("rename ...")
            rename_res = tester.op(tester.nfs3.rename, tester.dir_fh, filename, tester.dir_fh, new_name, auth=auth)
            if not rename_res or rename_res["status"] != NFS3_OK:
                print(f"Rename failed for {filename}")
                continue

            print("renamed lookup ...")
            lookup_res = tester.op(tester.nfs3.lookup, tester.dir_fh, new_name, auth)
            if not lookup_res or lookup_res["status"] != NFS3_OK:
                print(f"Lookup failed for {new_name}")
                continue
            fh = lookup_res["resok"]["object"]["data"]

            print("write ...")
            write_res = tester.op(tester.nfs3.write, fh, offset=0, count=len(content), content=content,
                                  stable_how=DATA_SYNC, auth=auth)
            if not write_res or write_res["status"] != NFS3_OK:
                print(f"Write failed for {new_name}")

            print("waiting ...")
            time.sleep(1)
    finally:
        tester.disconnect()


if __name__ == "__main__":
    main()
