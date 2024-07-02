import yaml
import jinja2 as jin
import argparse

PATH_CONFIG = 'config.yaml'
KEY = 'templates'
KEY_SRC = 'src'
KEY_DST = 'dst'

with open(PATH_CONFIG, 'r') as file:
    config = yaml.safe_load(file)

parser = argparse.ArgumentParser('apply_configurations')
parser.add_argument('-t', '--targets', nargs='+', default=config[KEY]['targets'], help='List of configurations to apply. Defaults to all available targets if omitted.')

args = parser.parse_args()
targets = args.targets

for target in targets:
    target_config = config[KEY]['targets'][target]
    src = f'{config[KEY]["path"]}/{target_config[KEY_SRC]}'
    dst = target_config[KEY_DST]

    print(f'Applying configuration to \'{config[KEY]["path"]}/{src}\' and saving result to \'{dst}\'...')
    try:
        with open(src, 'r') as file_src:
            template = jin.Template(file_src.read())

        with open(dst, 'w') as file_dst:
            file_dst.write(template.render(config))
    except FileNotFoundError:
        print(f'File \'{src}\' not found. Skipping...')

