import os 
import time
from pprint import pprint 
from pyNfsClient import (Mount, NFSv3, MNT3_OK, NFS_PROGRAM,
                         NFS_V3, NFS3_OK, DATA_SYNC)


# get the home directory of the user running the script
home_dir = os.path.expanduser("~")
mount_path = "{}/srv/nfs/shared".format(home_dir) 
dir_name = "dir3"
mnt_port = 2049
nfs_port = 2049
user_id = os.getuid()
group_id = os.getgid()

host = "localhost"  # Use localhost for testing
file_count = 10  # Number of files to create
loop_delay = 0.1  # Delay between file creations in seconds

print(f"Using user ID: {user_id}, group ID: {group_id}")    
print(f"Using mount path: {mount_path}, mnt_port: {mnt_port}, nfs_port: {nfs_port}")        


auth = {
    "flavor": 1,
    "machine_name": host,
    "uid": user_id,  # Use the user ID from the environment
    "gid": group_id,  # Use the group ID from the environment 
    "aux_gid": list(),
}

CREATE_UNCHECKED = 0  # From NFSv3 spec

mount = Mount(host=host, auth=auth, port=mnt_port, timeout=3600)
mount.connect()
mnt_res = mount.mnt(mount_path, auth)


if mnt_res["status"] == MNT3_OK:
    root_fh = mnt_res["mountinfo"]["fhandle"]
    print(f"Root file handle: {root_fh}")

    nfs3 = None
    try:
        # nfs_port = portmap.getport(NFS_PROGRAM, NFS_V3)
        nfs3 = NFSv3(host, nfs_port, 3600, auth=auth)
        nfs3.connect()

        # Create the directory (ignore error if already exists)
        mkdir_res = nfs3.mkdir(root_fh, dir_name, mode=0o777, auth=auth)

        # raise Exception("Arbitrary exception to make the test shorter") 
        
        # Even if it fails, attempt lookup
        dir_lookup = nfs3.lookup(root_fh, dir_name, auth)
        if dir_lookup["status"] != NFS3_OK:
            print(f"Directory lookup failed: {dir_lookup['status']}")
            raise Exception("Cannot find or create target directory")
        dir_fh = dir_lookup["resok"]["object"]["data"]
        print("directory file handle:", dir_fh) 

        # Create some files with specific content
        for x in range(1, file_count + 1):
            filename = f"file{x}.txt"
            
            # file_content = f"this is file number {x}\n"
            file_content = ""
            for i in range(1, 10):
                text = f"this is file number {x}, which is repeated many times. This the repetition number {i}\n"
                file_content += text
                
            create_res = nfs3.create(dir_fh, filename, CREATE_UNCHECKED, auth=auth)
            if create_res["status"] != NFS3_OK:
                print(f"Create failed for {filename}: {create_res['status']}")
                continue
            
            file_fh = create_res["resok"]["obj"]["handle"]["data"]
            print(file_fh)

            write_res = nfs3.write(file_fh, offset=0, count=len(file_content),
                                   content=file_content, stable_how=DATA_SYNC, auth=auth)
            if write_res["status"] != NFS3_OK:
                print(f"Write failed for {filename}: {write_res['status']}")

            time.sleep(loop_delay)

    finally:
        if nfs3:
            nfs3.disconnect()
        mount.umnt()
        mount.disconnect()
else:
    print(f"Mount failed: {mnt_res['status']}")
    mount.disconnect()
