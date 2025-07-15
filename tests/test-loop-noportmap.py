import time
from pprint import pprint 
from pyNfsClient import (Mount, NFSv3, MNT3_OK, NFS_PROGRAM,
                         NFS_V3, NFS3_OK, DATA_SYNC)

host = "10.70.10.110"

# get server number from command line argument
import sys
if len(sys.argv) > 1:
    server_number = sys.argv[1]
else:
    server_number = 0
    
if server_number == "0":
    mount_path, mnt_port, nfs_port = "/srv/nfs/shared", 2049, 2049
elif server_number == "1":  
    mount_path, mnt_port, nfs_port = "/srv/nfs/shared1", 2050, 2050
elif server_number == "2":
    mount_path, mnt_port, nfs_port = "/srv/nfs/shared2", 2051, 2051
elif server_number == "3":
    mount_path, mnt_port, nfs_port = "/srv/nfs/shared3", 2052, 2052
elif server_number == "4":
    mount_path, mnt_port, nfs_port = "/srv/nfs/shared4", 2053, 2053
elif server_number == "5":  
    mount_path, mnt_port, nfs_port = "/srv/nfs/shared5", 2054, 2054
else: 
    exit(f"Invalid server number: {server_number}. Use 0-5.")

print(f"Using mount path: {mount_path}, mnt_port: {mnt_port}, nfs_port: {nfs_port}")        

dir_name = "dir7"

auth = {"flavor": 1,
        "machine_name": "sim-08",
        "uid": 6120,
        "gid": 30142,
        "aux_gid": list(),
        }


CREATE_UNCHECKED = 0  # From NFSv3 spec

# portmap = Portmap(host, timeout=3600)
# portmap.connect()
# mnt_port = portmap.getport(Mount.program, Mount.program_version)


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

        raise Exception("Arbitrary exception to make the test shorter") 
        
        # Even if it fails, attempt lookup
        dir_lookup = nfs3.lookup(root_fh, dir_name, auth)
        if dir_lookup["status"] != NFS3_OK:
            print(f"Directory lookup failed: {dir_lookup['status']}")
            raise Exception("Cannot find or create target directory")
        dir_fh = dir_lookup["resok"]["object"]["data"]
        print("directory file handle:", dir_fh) 

        # Create 100 files with specific content
        for x in range(1, 5):
            filename = f"file{x}.txt"
            file_content = f"this is file number {x}\n"

            create_res = nfs3.create(dir_fh, filename, CREATE_UNCHECKED, auth=auth)
            if create_res["status"] != NFS3_OK:
                print(f"Create failed for {filename}: {create_res['status']}")
                continue
            
            # print("create res:")
            # pprint(create_res)
            
            file_fh = create_res["resok"]["obj"]["handle"]["data"]
            print(file_fh)

            write_res = nfs3.write(file_fh, offset=0, count=len(file_content),
                                   content=file_content, stable_how=DATA_SYNC, auth=auth)
            if write_res["status"] != NFS3_OK:
                print(f"Write failed for {filename}: {write_res['status']}")

            time.sleep(1)

    finally:
        if nfs3:
            nfs3.disconnect()
        mount.umnt()
        mount.disconnect()
        # portmap.disconnect()
else:
    print(f"Mount failed: {mnt_res['status']}")
    mount.disconnect()
    # portmap.disconnect()
