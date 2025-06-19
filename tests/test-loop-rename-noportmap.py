import time
import threading
from queue import Queue
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
CREATE_UNCHECKED = 0  # From NFSv3 spec
TIMEOUT = 1

mnt_port = 2049
nfs_port = 2049


class SafeNFSClient:
    """Wrapper around NFSv3 that reconnects on timeouts."""

    def __init__(self, host, mount_path, dir_name, auth, mnt_port, nfs_port, timeout):
        self.host = host
        self.mount_path = mount_path
        self.dir_name = dir_name
        self.auth = auth
        self.mnt_port = mnt_port
        self.nfs_port = nfs_port
        self.timeout = timeout
        self.mount = None
        self.nfs = None
        self.root_fh = None
        self.dir_fh = None
        self.reconnect()

    def _call_with_timeout(self, func, *args, **kwargs):
        result = Queue()

        def target():
            try:
                result.put((True, func(*args, **kwargs)))
            except Exception as exc:  # pragma: no cover - best effort logging
                result.put((False, exc))

        t = threading.Thread(target=target, daemon=True)
        t.start()
        t.join(self.timeout)
        if t.is_alive():
            print("[WARN] NFS operation timed out; remounting")
            self.reconnect()
            return None
        success, res = result.get()
        if not success:
            print(f"[WARN] NFS operation raised {res}; remounting")
            self.reconnect()
            return None
        return res

    def reconnect(self):
        if self.nfs:
            try:
                self.nfs.disconnect()
            except Exception:
                pass
        if self.mount:
            try:
                self.mount.umnt()
            except Exception:
                pass
            try:
                self.mount.disconnect()
            except Exception:
                pass

        self.mount = Mount(host=self.host, auth=self.auth, port=self.mnt_port, timeout=self.timeout)
        self.mount.connect()
        mnt_res = self.mount.mnt(self.mount_path, self.auth)
        if mnt_res["status"] != MNT3_OK:
            raise RuntimeError(f"Mount failed: {mnt_res['status']}")
        self.root_fh = mnt_res["mountinfo"]["fhandle"]

        self.nfs = NFSv3(self.host, self.nfs_port, timeout=self.timeout, auth=self.auth)
        self.nfs.connect()

        # Ensure the working directory exists and fetch its handle
        self._call_with_timeout(self.nfs.mkdir, self.root_fh, self.dir_name, mode=0o777, auth=self.auth)
        lookup = self._call_with_timeout(self.nfs.lookup, self.root_fh, self.dir_name, self.auth)
        if lookup and lookup["status"] == NFS3_OK:
            self.dir_fh = lookup["resok"]["object"]["data"]
        else:
            raise RuntimeError("Could not obtain directory handle")

    def call(self, func, *args, **kwargs):
        return self._call_with_timeout(func, *args, **kwargs)

    def cleanup(self):
        if self.nfs:
            try:
                self.nfs.disconnect()
            except Exception:
                pass
        if self.mount:
            try:
                self.mount.umnt()
            except Exception:
                pass
            try:
                self.mount.disconnect()
            except Exception:
                pass


def main():
    client = SafeNFSClient(host, mount_path, dir_name, auth, mnt_port, nfs_port, TIMEOUT)
    try:
        for x in range(1, reps + 1):
            filename = f"file{x}.txt"
            new_filename = f"renamed_file{x}.txt"
            file_content = f"this is file number {x}"

            print("create ...")
            create_res = client.call(client.nfs.create, client.dir_fh, filename, CREATE_UNCHECKED, auth=auth)
            if not create_res or create_res["status"] != NFS3_OK:
                print(f"Create failed for {filename}")
                continue

            print("rename ...")
            rename_res = client.call(
                client.nfs.rename,
                client.dir_fh,
                filename,
                client.dir_fh,
                new_filename,
                auth=auth,
            )
            if not rename_res or rename_res["status"] != NFS3_OK:
                print(f"Rename failed for {filename}")
                continue

            print("renamed lookup ...")
            renamed_lookup = client.call(client.nfs.lookup, client.dir_fh, new_filename, auth)
            if not renamed_lookup or renamed_lookup["status"] != NFS3_OK:
                print(f"Lookup failed for {new_filename}")
                continue
            file_fh = renamed_lookup["resok"]["object"]["data"]

            print("write ...")
            write_res = client.call(
                client.nfs.write,
                file_fh,
                offset=0,
                count=len(file_content),
                content=file_content,
                stable_how=DATA_SYNC,
                auth=auth,
            )
            if not write_res or write_res["status"] != NFS3_OK:
                print(f"Write failed for {new_filename}")

            print("waiting ...")
            time.sleep(1)
    finally:
        client.cleanup()


if __name__ == "__main__":
    main()
