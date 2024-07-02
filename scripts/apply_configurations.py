import yaml
import jinja2 as jin
import argparse
import shutil
import os

PATH_CONFIG = 'config.yaml'
KEY = 'templates'
KEY_SRC = 'src'
KEY_DST = 'dst'

def main():

    with open(PATH_CONFIG, 'r') as file:
        config = yaml.safe_load(file)

    parser = argparse.ArgumentParser(description="Apply configurations to template files.")
    parser.add_argument('-t', '--targets', nargs='+', default=None, help='List of configurations to apply. Defaults to all available targets if omitted.')

    args = parser.parse_args()
    targets = args.targets

    apply_configurations(config, targets)
    

def apply_configurations(config, targets=None):

    targets_all = list(config[KEY]['targets'])

    if targets is None:
        targets = list(config[KEY]['targets'])

    for target in targets:

        try:
            target_config = config[KEY]['targets'][target]
        except KeyError:
            print(f'No configuration found for \'{target}\'. Available options are: {", ".join(targets_all)}')
            continue

        try:
            dst = target_config[KEY_DST]
            if target == 'config':
                print(f'Copying {PATH_CONFIG} to \'{dst}\'...')
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy(PATH_CONFIG, dst)

            else:
                    src = f'{config[KEY]["path"]}/{target_config[KEY_SRC]}'

                    print(f'Applying configuration to \'{src}\' and saving result to \'{dst}\'...')
                    with open(src, 'r') as file_src:
                        template = jin.Template(file_src.read())

                    with open(dst, 'w') as file_dst:
                        file_dst.write(template.render(config))
        except FileNotFoundError:
            print(f'File \'{src}\' not found. Skipping...')
        except KeyError as e:
            print(f'No {e} attribute found for \'{target}\'. Skipping...')


if __name__ == "__main__":
    main()
