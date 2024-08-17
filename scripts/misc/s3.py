import subprocess

def cp_if_exists(config, bucket, dst, name=None):
    bucket_full = f"{config['general']['name']}/{bucket}"
    if subprocess.run(["./bin/mc", "find", bucket_full]).returncode == 0:
        subprocess.run(["./bin/mc", "cp", "-r", bucket_full, dst])
        if name is not None:
            print(f"Success: {name} saved to {dst}!")
    else:
        print(f"Warning: Bucket {bucket} does not exist in the S3 storage ({config['general']['name']}). Something must have gone wrong with the collection in the applications.")