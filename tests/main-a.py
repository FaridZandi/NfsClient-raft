import os
import time
from pyNfsClient import (Mount, NFSv3, MNT3_OK, NFS_PROGRAM, NFS_V3, NFS3_OK, DATA_SYNC, NFS3ERR_EXIST, NFS3ERR_NOENT)
import concurrent.futures
import functools
import argparse

TIMEOUT = 1 # Default timeout for NFS operations 
RETRIES = 20 # Number of retries for NFS operations
FILE_REPS = 3 # Number of repetitions for file content
FILE_COUNT = 2 # Number of files to create
DIR_NAME = "dir2" # Directory name to create and use
RETRY_DELAY = 1 # Delay between retries in seconds
MODE="exec+verify"

RETRY_FAILED = "RETRY_FAILED"
SETUP_FAILED = "SETUP_FAILED"

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

def nfs_retry(retries=3):
    """Decorator to retry NFS operations, reconnecting on failure or exception."""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):

            starting_time = time.time() 
            
            for attempt in range(retries):
                try:
                    ret = func(self, *args, **kwargs)
                    finish_time = time.time()
                    print(f"[INFO] {func.__name__} completed in {finish_time - starting_time:.2f} seconds")
                    return ret
                except Exception as e:
                    print(f"[ERROR] Exception in {func.__name__} (attempt {attempt+1}): {e}")
                    
                    
                # Reconnect and retry
                self.cleanup()
                
                time.sleep(RETRY_DELAY)

                try:
                    self.setup()
                except Exception as e:
                    print(f"[ERROR] Setup failed during retry (attempt {attempt+1}): {e}")
                    return SETUP_FAILED

            print(f"Failed to execute {func.__name__} after {retries} retries.")
            return RETRY_FAILED
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
        for attempt in range(RETRIES):
            try:
                self.nfs3 = NFSv3(self.host, self.nfs_port, 
                                  TIMEOUT, auth=self.auth)
                self.nfs3.connect()
                print(f"Connected to NFS server at {self.host}:{self.nfs_port}")
                return
            except Exception as e:
                print(f"[ERROR] NFS connection attempt {attempt+1} failed: {e}")
                if attempt < RETRIES - 1:
                    print(f"Retrying in {TIMEOUT} seconds...")
                    time.sleep(TIMEOUT)
        raise Exception("Failed to connect to NFS server after multiple attempts")
    
    def connect_mount(self):
        for attempt in range(RETRIES):
            try:
                self.mount = Mount(host=self.host, auth=self.auth, 
                                   port=self.mnt_port, timeout=TIMEOUT)
                self.mount.connect()
                print(f"Connected to mount at {self.host}:{self.mnt_port}")
                return
            except Exception as e:
                print(f"[ERROR] Mount Connected attempt {attempt+1} failed: {e}")
                if attempt < RETRIES - 1:
                    print(f"Retrying in {TIMEOUT} seconds...")
                    time.sleep(TIMEOUT)
        raise Exception("Failed to mount NFS after multiple attempts")

    
    def setup(self):
        print(f"Using user ID: {self.user_id}, group ID: {self.group_id}")
        try:
            self.connect_mount()
            self.connect_nfs() 
        except Exception as e:
            print(f"Setup failed: {e}")
        
    def cleanup(self):
        try: 
            if self.nfs3:
                self.nfs3.disconnect()
                self.nfs3 = None
        except Exception as e:
            print(f"Error during NFS cleanup: {e}")
            self.nfs3 = None
            
        try:
            if self.mount:
                self.mount.disconnect()
                self.mount = None
        except Exception as e:
            print(f"Error during mount cleanup: {e}")
            self.mount = None

                
    @nfs_retry(RETRIES)
    def mount_fs(self): 
        mnt_res = self.mount.mnt(self.mount_path, self.auth)
        if mnt_res["status"] != MNT3_OK:
            raise Exception(f"Mount failed: {mnt_res['status']}")
        self.root_fh = mnt_res["mountinfo"]["fhandle"]
        print(f"Mounted NFS at {self.mount_path} with root file handle: {self.root_fh}")
    
    
    # intentionally left out the decorator. see comment below 
    def unmount(self):
        if self.mount:
            # sometimes, we get reconnected to a different replica, which doesn't know about the mount operation
            # sent to the initial replica, which causes the unmount operation to fail, which is not a big issue.
            # Doesn't seem necessary to retry unmounting in this case, but still a problem. 
            try:
                self.mount.umnt()
            except Exception as e:
                print("Unmount Failed. Skipping retries for now.")
    
    @nfs_retry(RETRIES)
    def nfs_mkdir(self, dir_name, mode=0o777, exists_okay=False):
        mkdir_res = self.nfs3.mkdir(self.root_fh, dir_name, mode=mode, auth=self.auth)
        if mkdir_res["status"] == NFS3ERR_EXIST and exists_okay:
            return mkdir_res
        if mkdir_res["status"] != NFS3_OK:
            raise Exception(f"mkdir failed for {dir_name}: {mkdir_res['status']}")
        return mkdir_res


    @nfs_retry(RETRIES)
    def nfs_lookup_fh(self, parent, dir_name):
        dir_lookup = self.nfs3.lookup(parent, dir_name, self.auth)
        print(f"Lookup result for {dir_name} with results {dir_lookup["status"]}")
        if dir_lookup["status"] == NFS3ERR_NOENT: 
            return NFS3ERR_NOENT
        if dir_lookup["status"] != NFS3_OK:
            raise Exception(f"lookup failed for {dir_name} in {parent}: {dir_lookup['status']}")
        return dir_lookup["resok"]["object"]["data"]

    @nfs_retry(RETRIES)
    def create_file(self, number):
        filename = f"file{number}.txt"
        create_res = self.nfs3.create(self.dir_fh, filename, 0, auth=self.auth)
        if create_res["status"] != NFS3_OK:
            raise Exception(f"Create failed for {filename}: {create_res['status']}")
        file_fh = create_res["resok"]["obj"]["handle"]["data"]
        # Print file handle in hex for better readability
        print(f"Created {filename}, file handle: {file_fh.hex() if isinstance(file_fh, bytes) else str(file_fh)}")
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
            raise Exception(f"Write failed for file{number}.txt: {write_res['status']}")

        return 0 

    

    def run(self, dir_name):
        try:
            print(f"Creating directory: {dir_name}")
            self.nfs_mkdir(dir_name, exists_okay=True)
            
            print(f"Directory {dir_name} created or already exists")
            self.dir_fh = self.nfs_lookup_fh(self.root_fh, dir_name)
            
            for number in range(1, self.file_count + 1):
                print(f"Creating file {number} in directory {dir_name}")
                file_fh = self.create_file(number)
                
                if file_fh:
                    if file_fh == RETRY_FAILED:
                        print(f"Retry failed for file {number}")
                        return 1
                    
                    print(f"Writing to file {number}")
                    
                    res = self.write_to_file(file_fh, number)
                    if res == RETRY_FAILED: 
                        print(f"Retry failed for file {number}")
                        return 1
                    if res == SETUP_FAILED:
                        print(f"Setup failed for file {number}")
                        return 2

                    time.sleep(self.loop_delay)
                else:
                    print(f"Skipping write for file {number} due to creation failure")
                    
            return 0 
        finally:
            print("running done.")
            # self.cleanup()

    def verify_files(self, dir_name):
        """Verify that files were created and written to correctly."""

        self.dir_fh = self.nfs_lookup_fh(self.root_fh, dir_name)
        # check if the directory exists 
        
        verified = [0] * self.file_count
        
        
        for number in range(1, self.file_count + 1):
            filename = f"file{number}.txt"
            print(f"Verifying file {filename} in directory {dir_name}")
            file_fh = self.nfs_lookup_fh(self.dir_fh, filename)

            if file_fh:
                print(f"File {filename} found, verifying content")
                read_res = self.nfs3.read(file_fh, offset=0, auth=self.auth)
                if read_res["status"] == NFS3_OK:
                    content = read_res["resok"]["data"]
                    expected_content = ""
                    for rep in range(1, self.rep_count + 1):
                        expected_content += f"this is file number {number}, This the repetition number {rep}\n"
                    if content.decode() == expected_content:
                        print(f"File {filename} verified successfully")
                        verified[number - 1] = 1
                    else:
                        print(f"Content mismatch in file {filename}")
                        verified[number - 1] = 0
                else:
                    print(f"Read failed for file {filename}: {read_res['status']}")
            else:
                print(f"File {filename} not found in directory {dir_name}")
        
        # print in yellow color
        print("\033[93m" + f"Verification results for directory {dir_name}:")
        
        all_passed = all(status == 1 for status in verified)
        
        for status in verified:
            if status == 1:
                print("\033[92m" + "O", end="")
            else:
                print("\033[91m" + "X", end="")

        # print("\nVerification complete.")
        print("\033[93m" + "\nVerification complete.")
        # Reset color
        print("\033[0m")
        
        if all_passed:
            print("CLIENT: All files verified successfully.")
        
if __name__ == "__main__":
    home_dir = os.path.expanduser("~")
    default_mount_path = os.path.join(home_dir, "srv/nfs/shared")
    
    parser = argparse.ArgumentParser(
        description="NFS client workload generator/validator"
    )
    # Flags for the constants at the top
    parser.add_argument("--timeout", type=int, default=TIMEOUT,
                        help=f"RPC timeout (s). Default: {TIMEOUT}")
    parser.add_argument("--retries", type=int, default=RETRIES,
                        help=f"Number of retries. Default: {RETRIES}")
    parser.add_argument("--file-reps", type=int, default=FILE_REPS,
                        help=f"Repetitions per file. Default: {FILE_REPS}")
    parser.add_argument("--file-count", type=int, default=FILE_COUNT,
                        help=f"Number of files to create. Default: {FILE_COUNT}")
    parser.add_argument("--dir-name", default=DIR_NAME,
                        help=f"Target directory name. Default: {DIR_NAME}")
    parser.add_argument("--retry-delay", type=int, default=RETRY_DELAY,
                        help=f"Delay between retries (s). Default: {RETRY_DELAY}")
    parser.add_argument("--mode", default=MODE,
                        help=f"Operation mode: exec, verify, exec+verify. Default: {MODE}")

    # (Optional but handy) operational flags you might want anyway
    parser.add_argument("--host", default="localhost", help="NFS server host")
    parser.add_argument("--mnt-port", type=int, default=2049, help="mountd port")
    parser.add_argument("--nfs-port", type=int, default=2049, help="nfsd port")
    parser.add_argument("--mount-path", default=default_mount_path, help="Export path to mount")
    parser.add_argument("--loop-delay", type=float, default=0.0, help="Sleep between file writes (s)")
    parser.add_argument("--uid", type=int, default=None, help="Override UID for auth")
    parser.add_argument("--gid", type=int, default=None, help="Override GID for auth")

    args = parser.parse_args()

    # Update module-level constants (keeps simple places reading globals happy)
    TIMEOUT = args.timeout
    RETRIES = args.retries
    FILE_REPS = args.file_reps
    FILE_COUNT = args.file_count
    DIR_NAME = args.dir_name
    RETRY_DELAY = args.retry_delay
    MODE = args.mode.lower()
    
    client = NFSClient(
        host=args.host,
        mnt_port=args.mnt_port,
        nfs_port=args.nfs_port,
        mount_path=args.mount_path,
        file_count=FILE_COUNT,
        loop_delay=args.loop_delay,
        rep_count=FILE_REPS,
        user_id=args.uid,
        group_id=args.gid,
    )
    # Make sure methods decorated with @nfs_retry() see the new values
    client.retries = RETRIES
    client.timeout = TIMEOUT

    ################################

    client.setup()
    client.mount_fs()
    run_res = 0
    if MODE in ("exec", "exec+verify"):
         run_res = client.run(dir_name=DIR_NAME)
         if run_res != 0:
             print(f"Error occurred during file operations: {run_res}")

    if MODE in ("verify", "exec+verify"):
        if run_res != 0: 
            print("run failed")
        else: 
            client.verify_files(dir_name=DIR_NAME)
    client.unmount()
    client.cleanup()