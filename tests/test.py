from pyNfsClient import (Portmap, Mount, NFSv3, MNT3_OK, NFS_PROGRAM,
                       NFS_V3, NFS3_OK, DATA_SYNC)

# variable preparation
host = "10.70.10.110"
mount_path = "/srv/nfs/sharedfarid"

auth = {"flavor": 1,
        "machine_name": "sim-08",
        "uid": 6120,
        "gid": 30142,
        "aux_gid": list(),
        }

text = "SAMPLE TEXT"

print(f"[INFO] Starting NFS client script.")
print(f"[INFO] Target host: {host}")
print(f"[INFO] Target mount path: {mount_path}")

# portmap initialization
print("[STEP] Initializing Portmap connection...")
portmap = Portmap(host, timeout=3600)
portmap.connect()
print("[SUCCESS] Connected to Portmap.")

# mount initialization
print("[STEP] Getting mountd port from Portmap...")
mnt_port = portmap.getport(Mount.program, Mount.program_version)
print(f"[SUCCESS] Mountd port retrieved: {mnt_port}")

print("[STEP] Initializing Mount client...")
mount = Mount(host=host, auth=auth, port=mnt_port, timeout=3600)
mount.connect()
print("[SUCCESS] Connected to Mountd.")

# do mount
print(f"[STEP] Requesting mount of '{mount_path}' ...")
mnt_res = mount.mnt(mount_path, auth)
print(f"[DEBUG] Mount result: {mnt_res}")

if mnt_res["status"] == MNT3_OK:
    print(f"[SUCCESS] Successfully mounted '{mount_path}'.")
    root_fh = mnt_res["mountinfo"]["fhandle"]
    try:
        print("[STEP] Getting NFSv3 port from Portmap...")
        nfs_port = portmap.getport(NFS_PROGRAM, NFS_V3)
        print(f"[SUCCESS] NFSv3 port retrieved: {nfs_port}")

        # nfs actions
        print("[STEP] Initializing NFSv3 client...")
        nfs3 = NFSv3(host, nfs_port, 3600, auth=auth)
        nfs3.connect()
        print("[SUCCESS] Connected to NFSv3.")

        print(f"[STEP] Looking up 'file.txt' in '{mount_path}'...")
        lookup_res = nfs3.lookup(root_fh, "file.txt", auth)
        print(f"[DEBUG] Lookup result: {lookup_res}")

        if lookup_res["status"] == NFS3_OK:
            print("[SUCCESS] Found 'file.txt' in mount directory.")
            fh = lookup_res["resok"]["object"]["data"]

            print("[STEP] Writing 'Sample text' to 'file.txt' (offset=0)...")
            write_res = nfs3.write(fh, offset=0, count=len(text), content=text,
                                   stable_how=DATA_SYNC, auth=auth)
            print(f"[DEBUG] Write result: {write_res}")

            if write_res["status"] == NFS3_OK:
                print("[SUCCESS] Write to 'file.txt' successful.")

                print("[STEP] Reading data back from 'file.txt' (offset=0)...")
                read_res = nfs3.read(fh, offset=0, auth=auth)
                print(f"[DEBUG] Read result: {read_res}")

                if read_res["status"] == NFS3_OK:
                    read_content = str(read_res["resok"]["data"], encoding="utf-8")
                    print(f"[SUCCESS] Read content: '{read_content}'")
                    assert read_content.startswith(text)
                    print("[SUCCESS] Content check passed.")
                else:
                    print(f"[ERROR] Failed to read from file. Status: {read_res['status']}")
            else:
                print(f"[ERROR] Write failed. Status: {write_res['status']}")
        else:
            print(f"[ERROR] Lookup failed. Status: {lookup_res['status']}")
    finally:
        print("[STEP] Cleaning up: disconnecting NFS, unmounting, and closing connections...")
        try:
            if nfs3:
                nfs3.disconnect()
                print("[SUCCESS] Disconnected from NFSv3.")
        except Exception as e:
            print(f"[WARN] Exception while disconnecting NFSv3: {e}")

        try:
            mount.umnt()
            print(f"[SUCCESS] Unmounted '{mount_path}'.")
        except Exception as e:
            print(f"[WARN] Exception while unmounting: {e}")

        try:
            mount.disconnect()
            print("[SUCCESS] Disconnected from Mountd.")
        except Exception as e:
            print(f"[WARN] Exception while disconnecting Mountd: {e}")

        try:
            portmap.disconnect()
            print("[SUCCESS] Disconnected from Portmap.")
        except Exception as e:
            print(f"[WARN] Exception while disconnecting Portmap: {e}")

else:
    print(f"[ERROR] Mount failed. Status: {mnt_res['status']}")
    try:
        mount.disconnect()
        print("[SUCCESS] Disconnected from Mountd.")
    except Exception as e:
        print(f"[WARN] Exception while disconnecting Mountd: {e}")
    try:
        portmap.disconnect()
        print("[SUCCESS] Disconnected from Portmap.")
    except Exception as e:
        print(f"[WARN] Exception while disconnecting Portmap: {e}")
