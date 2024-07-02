import subprocess
import sys
import argparse
from multiprocessing import Pool

def main():
    parser = argparse.ArgumentParser('run_script_on_remotes')
    parser.add_argument('cmd', help='Command to be run on the remote hosts')
    parser.add_argument('--hosts', default='hosts.txt', help='Hosts file')
    parser.add_argument('--key', default='/home/lennart/.ssh/id_rsa_continuum', help='SSH keyfile')
    parser.add_argument('--files', nargs='+', default=[], help='List of files to be added')
    parser.add_argument('-v', '--verbose', action='store_true', help='Always print output.')

    args = parser.parse_args()

    run_on_remotes(args.cmd, args.hosts, args.key, args.files, args.verbose)


def run_on_remotes(cmd, path_hosts, path_key=None, files=[], verbose=False):

    with open(path_hosts, 'r') as file_hosts:

        hosts = list(map(lambda host: host.strip(), file_hosts.readlines()))
        hosts = list(filter(lambda host: not host.startswith('#') and not host == '', hosts))

        args_per_host = [(host, cmd, path_key, files, verbose) for host in hosts]
        
        with Pool() as pool:
            results = pool.starmap(process_host, args_per_host)

        for (host, proc) in zip(hosts, results):
            print('✅' if proc.returncode == 0 else '❌', end=' ')
            print(f"{host} retured with code {proc.returncode}")
            if proc.returncode != 0 or verbose:
                for line in proc.stdout.decode().split('\n'):
                    print(f'[stdout({host}] {line}')
                for line in proc.stderr.decode().split('\n'):
                    print(f'[stderr({host}] {line}')


def run_commands_on_remote(host, commands, keyfile=None):
    for command in commands:
        # commands = [shlex.quote(command) for command in commands]
        flag_keyfile = f"-i {keyfile} " if keyfile else ""
        arg = f"ssh -o StrictHostKeyChecking=no {flag_keyfile}{host} \"{command}\""
        print(command)
        proc = subprocess.run(
            arg,
            shell=True,
            stdout=sys.stdout,
        )


def process_host(host, cmd, path_key=None, files=[], verbose=False):

    proc = None

    flag_key = ""
    if path_key is not None:
        flag_key = f"-i {path_key}"

    for file in files:
        if verbose:
            print(f'scp -o -i {path_key} {file} {host.strip()}:~/')

        proc = subprocess.run(f'scp -o StrictHostKeyChecking=no {flag_key} {file} {host.strip()}:~/', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if proc.returncode != 0:
            return proc

    if verbose:
        print(f"ssh -o StrictHostKeyChecking=no {flag_key} {host.strip()} {cmd}")
    proc = subprocess.run(f"ssh -o StrictHostKeyChecking=no -i {path_key} {host.strip()} {cmd}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    return proc


if __name__ == "__main__":
    main()