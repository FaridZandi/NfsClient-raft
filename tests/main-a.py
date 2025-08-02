import os
import time
from pyNfsClient import (Mount, NFSv3, MNT3_OK, NFS_PROGRAM, NFS_V3, NFS3_OK, DATA_SYNC)
import concurrent.futures
import functools

TIMEOUT = 5 # Default timeout for NFS operations 
RETRIES = 20 # Number of retries for NFS operations

def timeout(seconds):
    """Decorator to run a function with a timeout using ThreadPoolExecutor."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(func, *args, **kwargs)
                try:
                    return future.result(timeout=seconds)
                except concurrent.futures.TimeoutError:
                    print(f"Function '{func.__name__}' timed out after {seconds} seconds")
                    return None
        return wrapper
    return decorator


from functools import wraps

def nfs_retry(RETRIES=3):
    """Decorator to retry NFS operations, reconnecting on failure or exception."""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            for attempt in range(RETRIES):
                try:
                    return func(self, *args, **kwargs)
                except Exception as e:
                    print(f"[ERROR] Exception in {func.__name__} (attempt {attempt+1}): {e}")
                # Reconnect and retry
                print(f"Retrying NFS connection for {func.__name__} (attempt {attempt+2}/{RETRIES})...")
                try:
                    if self.nfs3:
                        self.nfs3.disconnect()
                except Exception:
                    pass
                
                self.connect_nfs()
            print(f"Failed to execute {func.__name__} after {RETRIES} retries.")
            return None
        return wrapper
    return decorator


class NFSClient:
    def __init__(self, host, mnt_port, nfs_port, mount_path,
                 user_id=None, group_id=None, file_count=10, loop_delay=0.1,
                 rep_count=10):
        self.host = host
        self.mnt_port = mnt_port
        self.nfs_port = nfs_port
        self.mount_path = mount_path
        self.file_count = file_count
        self.loop_delay = loop_delay
        self.rep_count = rep_count

        self.user_id = user_id if user_id is not None else os.getuid()
        self.group_id = group_id if group_id is not None else os.getgid()
        self.auth = {
            "flavor": 1,
            "machine_name": host,
            "uid": self.user_id,
            "gid": self.group_id,
            "aux_gid": list(),
        }
        self.mount = None
        self.nfs3 = None
        self.root_fh = None
        self.dir_fh = None



    def connect_nfs(self):
        for i in range(RETRIES):
            try:
                self.nfs3 = NFSv3(self.host, self.nfs_port, TIMEOUT, auth=self.auth)
                self.nfs3.connect()
                print(f"Connected to NFS server at {self.host}:{self.nfs_port}")
                return
            except Exception as e:
                print(f"[ERROR] NFS connection attempt {i+1} failed: {e}")
                if i < RETRIES - 1:
                    print("Retrying in 2 seconds...")
                    time.sleep(2)
        raise Exception("Failed to connect to NFS server after multiple attempts")
    
    # def mount_fs(self):
    #     self.mount = Mount(host=self.host, auth=self.auth, port=self.mnt_port, timeout=TIMEOUT)
    #     self.mount.connect()
    #     mnt_res = self.mount.mnt(self.mount_path, self.auth)
    #     if mnt_res["status"] != MNT3_OK:
    #         raise Exception(f"Mount failed: {mnt_res['status']}")
    #     self.root_fh = mnt_res["mountinfo"]["fhandle"]

    def mount_fs(self):
        for attempt in range(RETRIES):
            try:
                self.mount = Mount(host=self.host, auth=self.auth, port=self.mnt_port, timeout=TIMEOUT)
                self.mount.connect()
                mnt_res = self.mount.mnt(self.mount_path, self.auth)
                if mnt_res["status"] != MNT3_OK:
                    raise Exception(f"Mount failed: {mnt_res['status']}")
                self.root_fh = mnt_res["mountinfo"]["fhandle"]
                print(f"Mounted NFS at {self.mount_path} with root file handle: {self.root_fh}")
                return
            except Exception as e:
                print(f"[ERROR] Mount attempt {attempt+1} failed: {e}")
                if attempt < RETRIES - 1:
                    print("Retrying in 2 seconds...")
                    time.sleep(2)
        raise Exception("Failed to mount NFS after multiple attempts")
    
    
    def ensure_directory(self, dir_name, mode=0o777):
        self.nfs3.mkdir(self.root_fh, dir_name, mode=mode, auth=self.auth)
        dir_lookup = self.nfs3.lookup(self.root_fh, dir_name, self.auth)
        if dir_lookup["status"] != NFS3_OK:
            raise Exception("Cannot find or create target directory")
        self.dir_fh = dir_lookup["resok"]["object"]["data"]

    # def create_file(self, number):
    #     filename = f"file{number}.txt"
    #     create_res = self.nfs3.create(self.dir_fh, filename, 0, auth=self.auth)
    #     if create_res["status"] != NFS3_OK:
    #         print(f"Create failed for {filename}: {create_res['status']}")
    #         return None
    #     file_fh = create_res["resok"]["obj"]["handle"]["data"]
    #     print(f"Created {filename}, file handle: {file_fh}")
    #     return file_fh

    @nfs_retry(RETRIES)
    def create_file(self, number):
        filename = f"file{number}.txt"
        create_res = self.nfs3.create(self.dir_fh, filename, 0, auth=self.auth)
        if create_res["status"] != NFS3_OK:
            raise Exception(f"Create failed for {filename}: {create_res['status']}")
        file_fh = create_res["resok"]["obj"]["handle"]["data"]
        print(f"Created {filename}, file handle: {file_fh}")
        return file_fh

    @nfs_retry(RETRIES)
    def write_to_file(self, file_fh, number):
        if file_fh is None:
            return  # Don't attempt to write to a nonexistent file

        file_content = ""
        for rep in range(1, self.rep_count + 1):
            file_content += f"this is file number {number}, This the repetition number {rep}\n"

        write_res = self.nfs3.write(
            file_fh, offset=0, count=len(file_content),
            content=file_content, stable_how=DATA_SYNC, auth=self.auth)

        if write_res["status"] != NFS3_OK:
            print(f"Write failed for file{number}.txt: {write_res['status']}")

    def cleanup(self):
        if self.nfs3:
            self.nfs3.disconnect()
        if self.mount:
            self.mount.umnt()
            self.mount.disconnect()

    def run(self, dir_name):
        try:
            print(f"Using user ID: {self.user_id}, group ID: {self.group_id}")
            print(f"Using mount path: {self.mount_path}, mnt_port: {self.mnt_port}, nfs_port: {self.nfs_port}")
            self.mount_fs()
            print(f"Root file handle: {self.root_fh}")
            self.connect_nfs()
            self.ensure_directory(dir_name)
            
            for number in range(1, self.file_count + 1):
                print(f"Creating file {number} in directory {dir_name}")
                file_fh = self.create_file(number)

                if file_fh:
                    print(f"Writing to file {number}")
                    self.write_to_file(file_fh, number)
                    time.sleep(self.loop_delay)
                else:
                    print(f"Skipping write for file {number} due to creation failure")
        finally:
            self.cleanup()

if __name__ == "__main__":
    home_dir = os.path.expanduser("~")
    mount_path = f"{home_dir}/srv/nfs/shared"
    client = NFSClient(
        host="localhost",
        mnt_port=2049,
        nfs_port=2049,
        mount_path=mount_path,
        file_count=10,
        loop_delay=0.1,
    )
    client.run(dir_name="dir4")
