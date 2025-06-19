import time
from pprint import pprint 
from pyNfsClient import (Mount, NFSv3, MNT3_OK, NFS_PROGRAM,
                         NFS_V3, NFS3_OK, DATA_SYNC)

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

mount = Mount(host=host, auth=auth, port=mnt_port, timeout=TIMEOUT)
mount.connect()

print("mounting ...")
mnt_res = mount.mnt(mount_path, auth)

if mnt_res["status"] == MNT3_OK:
    root_fh = mnt_res["mountinfo"]["fhandle"]
    nfs3 = None
    try:
        # nfs_port = portmap.getport(NFS_PROGRAM, NFS_V3)
        nfs_port = 2049 
        
        print("connecting ...")
        nfs3 = NFSv3(host, nfs_port, timeout=TIMEOUT, auth=auth)
        nfs3.connect()

        print("mkdir ...")
        # Create the directory (ignore error if already exists)
        mkdir_res = nfs3.mkdir(root_fh, dir_name, mode=0o777, auth=auth)

        # Even if it fails, attempt lookup
        print("file lookup ...")
        dir_lookup = nfs3.lookup(root_fh, dir_name, auth)
        if dir_lookup["status"] != NFS3_OK:
            print(f"Directory lookup failed: {dir_lookup['status']}")
            raise Exception("Cannot find or create target directory")
        dir_fh = dir_lookup["resok"]["object"]["data"]

        # Create 100 files with specific content
        for x in range(1, reps + 1):
            filename = f"file{x}.txt"
            file_content = f"this is file number {x}"
            new_filename = f"renamed_file{x}.txt"

            # 1. Create the file
            print("create ...")
            create_res = nfs3.create(dir_fh, filename, CREATE_UNCHECKED, auth=auth)
            if create_res["status"] != NFS3_OK:
                print(f"Create failed for {filename}: {create_res['status']}")
                continue

            # 2. Rename the file
            print("rename ...")
            rename_res = nfs3.rename(
                dir_fh, filename,
                dir_fh, new_filename,
                auth=auth
            )
            if rename_res["status"] != NFS3_OK:
                print(f"Rename failed for {filename}: {rename_res['status']}")
                continue

            print("renamed lookup ...")
            # 3. Lookup the file handle for the new name
            renamed_lookup = nfs3.lookup(dir_fh, new_filename, auth)
            if renamed_lookup["status"] != NFS3_OK:
                print(f"Lookup failed for {new_filename}: {renamed_lookup['status']}")
                continue
            file_fh = renamed_lookup["resok"]["object"]["data"]

            print("write ...")
            # 4. Write to the renamed file
            write_res = nfs3.write(file_fh, offset=0, count=len(file_content),
                                content=file_content, stable_how=DATA_SYNC, auth=auth)
            if write_res["status"] != NFS3_OK:
                print(f"Write failed for {new_filename}: {write_res['status']}")

            print("waiting ...")
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
