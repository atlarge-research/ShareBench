import argparse
import subprocess
import grp
import os
import urllib.request
import yaml
import shutil
from misc.filenames import get_spark_full_name, get_spark_path
from telegraf.run_on_remotes import run_on_remotes
from apply_configurations import apply_configurations

PATH_CONFIG = "./config.yaml"

PKG_MNG = "apt"
# TODO: possibly add this to config

DIR_JARS = "./docker/jars"
URLS_JARS = [
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar",
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar",
    "https://repo1.maven.org/maven2/org/yaml/snakeyaml/1.26/snakeyaml-1.26.jar",
    "https://repo1.maven.org/maven2/org/apache/spark/spark-hadoop-cloud_2.12/3.5.1/spark-hadoop-cloud_2.12-3.5.1.jar"
    "https://repo1.maven.org/maven2/com/lihaoyi/ujson_2.12/3.3.1/ujson_2.12-3.3.1.jar",
    "https://repo1.maven.org/maven2/com/lihaoyi/upickle_2.12/3.3.1/upickle_2.12-3.3.1.jar",
    "https://repo1.maven.org/maven2/com/lihaoyi/upickle-core_2.12/3.3.1/upickle-core_2.12-3.3.1.jar",
    "https://repo1.maven.org/maven2/com/lihaoyi/upickle-implicits_2.12/3.3.1/upickle-implicits_2.12-3.3.1.jar",
]
# TODO: insert scala version automatically

DIR_BIN = "./bin"
URL_MINIO = "https://dl.min.io/client/mc/release/linux-amd64/mc"

URL_INFLUX = "https://download.influxdata.com/influxdb/releases/influxdb2-client-2.7.5-linux-amd64.tar.gz"
DIR_TMP = "./.tmp"

DIR_SPARK = "./spark"
URL_SPARK_BASE = "https://dlcdn.apache.org/spark"
EXT_SPARK_ARCHIVE = ".tgz"

PATH_HOSTS = './hosts.txt'
PATH_INSTALL_TELEGRAF = './scripts/telegraf/install-telegraf.sh'
PATH_TELEGRAF_CONF = "./scripts/telegraf/telegraf.conf"

def main():
    with open(PATH_CONFIG, "r") as file:
        config = yaml.safe_load(file)

    parser = argparse.ArgumentParser(description="Apply configurations to template files.")
    parser.add_argument('-t', '--targets', nargs='+', default=list(TARGETS), help='List of installations to perform. Defaults to all available installations if omitted.')

    args = parser.parse_args()
    targets = args.targets

    for target in targets:
        TARGETS[target](config)

def install_docker(config):
    print("Installing docker...")
    subprocess.run(f"sudo {PKG_MNG} update", check=True, shell=True)
    subprocess.run(f"sudo {PKG_MNG} install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin docker-compose", check=True, shell=True)

    # Check if the docker group exists, if not, create it
    if "docker" not in [group.gr_name for group in grp.getgrall()]:
        subprocess.run("sudo groupadd docker", check=True, shell=True)
    else:
        print("Docker group already exists. Skipping groupadd.")

    # Check if the current user is already in the docker group
    if os.getenv("USER") not in grp.getgrnam("docker").gr_mem:
        subprocess.run(f"sudo usermod -aG docker {os.getenv('USER')}", check=True, shell=True)
    else:
        print(f"User {os.getenv('USER')} is already in the docker group. Skipping usermod.")

    subprocess.run("sudo systemctl enable docker.service", check=True, shell=True)
    subprocess.run("sudo systemctl enable containerd.service", check=True, shell=True)

def install_zip(config):
    print("Installing zip and unzip...")
    subprocess.run(f"sudo {PKG_MNG} update", check=True, shell=True)
    subprocess.run(f"sudo {PKG_MNG} install -y zip unzip", check=True, shell=True)

def install_sdk_man(config):
    print("Installing sdk_man...")
    if not os.path.isdir(os.path.join(os.environ['HOME'], '.sdkman')):
        print("SDKMAN! is not installed. Installing...")
        subprocess.run("curl -s \"https://get.sdkman.io/\" | bash", shell=True, check=True)
    else:
        print("SDKMAN! is already installed.")

    cmds = [
        f"source \"{os.environ['HOME']}/.sdkman/bin/sdkman-init.sh\"",
        "sdk install java 11.0.22-tem",
        "sdk install sbt",
    ]
    subprocess.run(' && '.join(cmds), shell=True, check=True, executable='/bin/bash')

def download_dependency_jars(config):
    print("Downloading dependency jar files...")
    os.makedirs(DIR_JARS, exist_ok=True)

    # Download each jar file
    for url in URLS_JARS:
        file_name = url.split('/')[-1]  # Extract file name from URL
        dst_path = os.path.join(DIR_JARS, file_name)
        
        download_if_not_exists(url, dst_path) 

def start_services(config):
    print("Creating folders for service mounts...")

    for service in config['services']:
        config_service = config['services'][service]
        try:
            mnt = config_service['mnt']
            os.makedirs(mnt, exist_ok=False)
            print(f"Folder '{mnt}' created.")
        except OSError:
            print(f"Folder '{mnt}' already exists.")
        except KeyError:
            continue
    
    print("Starting services...")
    path_docker_compose = config['templates']['targets']['services']['dst']
    subprocess.run(f"docker-compose -f '{path_docker_compose}' up -d", check=True, shell=True)

def setup_minio(config):
    print("Setting up minio...")
    os.makedirs(DIR_BIN, exist_ok=True)
    dst_path = os.path.join(DIR_BIN, 'mc')

    download_if_not_exists(URL_MINIO, dst_path)

    subprocess.run(f"chmod +x {dst_path}", check=True, shell=True)

    # requires docker compose to be up and running
    config_minio = config['services']['minio']
    name = config['general']['name']
    url = f"http://{config['services']['general']['ip']}:{config_minio['ports']['core']}"
    user = config_minio['access_key']
    secret = config_minio['secret_key']
    cmd = f"{dst_path} alias set {name} {url} {user} {secret}"
    subprocess.run(cmd, check=True, shell=True)

def setup_influx(config):
    print("Setting up influx...")
    os.makedirs(DIR_BIN, exist_ok=True)
    path_influx = os.path.join(DIR_BIN, 'influx')

    if not os.path.exists(path_influx):
        path_archive = os.path.join(DIR_TMP, URL_INFLUX.split('/')[-1])
        download_if_not_exists(URL_INFLUX, path_archive)
        path_extracted = os.path.join(DIR_TMP, 'influx')
        os.makedirs(path_extracted, exist_ok=True)
        cmd = f"tar xzf {path_archive} -C {path_extracted}"
        subprocess.run(cmd, check=True, shell=True)
        shutil.move(os.path.join(path_extracted, 'influx'), path_influx)

        os.remove(path_archive)
        shutil.rmtree(path_extracted)

    config_influx = config['services']['influx']
    name = config['general']['name']
    url = f"http://{config['services']['general']['ip']}:{config_influx['port']}"
    org = config['general']['name']
    token = config_influx['token']

    # delete any existing configs with the same name
    subprocess.run(f"{path_influx} config rm {name}", shell=True, check=False, stdout=subprocess.DEVNULL)

    cmd = f"{path_influx} config create --config-name {name} --host-url {url} --org {org} --token {token} --active"
    subprocess.run(cmd, check=True, shell=True)

def download_spark(config):
    print("Downloading spark...")
    spark_ver = config['spark']['version']
    spark_full_name = get_spark_full_name(config)
    spark_path = get_spark_path(config)

    if not os.path.exists(spark_path):
        os.makedirs(DIR_SPARK, exist_ok=True)
        name_archive = spark_full_name + EXT_SPARK_ARCHIVE
        url_spark = f"{URL_SPARK_BASE}/spark-{spark_ver}/{name_archive}"
        path_archive = os.path.join(DIR_TMP, name_archive)
        download_if_not_exists(url_spark, path_archive)
        subprocess.run(f"tar xf {path_archive} -C {DIR_TMP}/", shell=True, check=True)
        shutil.move(os.path.join(DIR_TMP, spark_full_name), DIR_SPARK)
        os.remove(path_archive)

def download_if_not_exists(url, dst_path):
    print(f"Downloading {dst_path}...")
    if not os.path.exists(dst_path):
        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        saved_path, _ = urllib.request.urlretrieve(url, dst_path)
        print(f"Saved to {saved_path}")
        return saved_path
    else:
        print(f"{dst_path} already exists. Skipping download.")
        return dst_path

def install_telegraf_on_remotes(config):
    print("Installing telegraf on remote machines...")
    ssh_keyfile = config['kubernetes']['ssh_keyfile']
    run_on_remotes(f"./{os.path.basename(PATH_INSTALL_TELEGRAF)}", path_hosts=PATH_HOSTS, path_key=ssh_keyfile, files=[PATH_INSTALL_TELEGRAF, PATH_TELEGRAF_CONF], verbose=True)
        
TARGETS = {
    'configurations': apply_configurations,
    'docker': install_docker,
    'zip': install_zip,
    'sdk_man': install_sdk_man,
    'jars': download_dependency_jars,
    'services': start_services,
    'minio': setup_minio,
    'influx': setup_influx,
    'spark': download_spark,
    'telegraf': install_telegraf_on_remotes
}

if __name__ == "__main__":
    main()
    