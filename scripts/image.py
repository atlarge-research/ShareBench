import os
import subprocess
import argparse
import yaml
import jinja2 as jin

CONFIG = 'config.yaml'
KEY = 'docker'

def compile_project():
    sdkman_init_script = os.path.expanduser("~/.sdkman/bin/sdkman-init.sh")
    command = f'source {sdkman_init_script} && sbt package'
    subprocess.run(command, shell=True, executable='/bin/bash', check=True)

def docker_build(uri_image):
    subprocess.run(['docker', 'build', '--platform', 'linux/amd64', '-t', uri_image, '-f', 'docker/Dockerfile', '.'], check=True)

def docker_push(uri_image):
    subprocess.run(['docker', 'login'], check=True)
    subprocess.run(['docker', 'push', uri_image], check=True)

def push_image(config = True):
    username = config[KEY]['username']
    image = config['general']['name']
    version = config[KEY]['version']
    uri = f"{username}/{image}:{version}"

    docker_build(uri)
    docker_push(uri)

if __name__ == "__main__":
    with open(CONFIG, 'r') as file:
        config = yaml.safe_load(file)

    parser = argparse.ArgumentParser(description='Build and push the docker image.')
    parser.add_argument('-b', '--build', action='store_true', help='Build the program from source.')
    args = parser.parse_args()

    if args.build:
        compile_project()
    
    push_image(config)