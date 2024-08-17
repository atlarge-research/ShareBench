import yaml
from run_workload import run_workload
import argparse
import os
import time
import contextlib
import sys
import traceback

PATH_CONFIG = "config.yaml"
PROPERTIES = ["workloads", "num_apps", "mechanisms"]


def main():

    with open(PATH_CONFIG, "r") as file:
        config = yaml.safe_load(file)

    parser = argparse.ArgumentParser(description="Run spark experiment.")
    parser.add_argument("recipe", type=str, help="Path of recipe file to use")
    parser.add_argument("--start_delay", type=int, default=45, help="Start delay for each run.")
    parser.add_argument("--redirect_stdout", action="store_true", help="Redirect stdout to a file.")
    parser.add_argument("--redirect_stderr", action="store_true", help="Redirect stderr to a file.")

    args = parser.parse_args()
    run_experiment(
        config,
        args.recipe,
        args.start_delay,
        args.redirect_stdout,
        args.redirect_stderr,
    )


def run_experiment(config, path_book, start_delay, redirect_stdout=False, redirect_stderr=False):

    experiment_time = str(int(time.time()))
    book_name = os.path.splitext(os.path.basename(path_book))[0]

    stdout_file, stderr_file = get_output_files(book_name, experiment_time, redirect_stdout, redirect_stderr)

    try:
        with contextlib.redirect_stdout(stdout_file), contextlib.redirect_stderr(stderr_file):

            with open(path_book, "r") as file:
                book = yaml.safe_load(file)

            try:
                recipes = book["recipes"]
            except KeyError:
                print("No 'recipes' found in book.")
                return

            recipe_runs = []

            for i, recipe in enumerate(recipes):

                # print(recipe)

                print(f"Recipe {i + 1}/{len(recipes)}")

                try:
                    recipe_runs.append(run_recipe(config, book, recipe, start_delay))
                    # save session id + properties to list
                except ValueError as e:
                    print("Error with recipe definition: " + str(e))
                    print("Skipping recipe...")
                    continue

            dir_results = os.path.join("experiments", "results", book_name)
            os.makedirs(dir_results, exist_ok=True)

            results = {"start_delay": start_delay, "runs": recipe_runs}

            with open(os.path.join(dir_results, f"{experiment_time}.yaml"), "w") as file:
                yaml.dump(results, file)
    except Exception as e:
        print(traceback.format_exc(), file=stderr_file)
        raise e
    finally:
        if redirect_stdout:
            stdout_file.close()
        if redirect_stderr:
            stderr_file.close()


def run_recipe(config, book, recipe, start_delay):

    properties = {}

    for property in PROPERTIES:
        properties[property] = list(get_for_run_or_default(book, recipe, property))

    append_app_count = get_for_run_or_default(book, recipe, "append_app_count", default=True)

    print(properties)
    runs = {}

    for workload in properties["workloads"]:
        for mechanism in properties["mechanisms"]:
            for num_apps in properties["num_apps"]:
                try:

                    print(append_app_count)

                    workload_ext = workload
                    if append_app_count:
                        workload_ext += f"_{num_apps}"

                    print(workload_ext)

                    print(f"Running: '{workload_ext}' | {mechanism} | {num_apps}")
                    session_id = run_workload(
                        config,
                        workload=workload_ext,
                        num_apps=num_apps,
                        start_delay=start_delay,
                        mechanism=mechanism,
                    )
                    runs[session_id] = {
                        "success": True,
                        "workload": workload_ext,
                        "num_apps": num_apps,
                        "mechanism": mechanism,
                    }
                except ValueError as e:
                    print("Error with workload: " + str(e))
                    runs[session_id] = {"success": False}
    # print(runs)
    return runs


def get_output_files(book_name, experiment_time, redirect_stdout, redirect_stderr, buffering=1):
    
    if redirect_stdout or redirect_stderr:
        dir_output = os.path.join("experiments", "output", book_name, experiment_time)
        os.makedirs(dir_output, exist_ok=True)

    if redirect_stdout:
        path_stdout_file = os.path.join(dir_output, "stdout.txt")
        stdout_file = open(path_stdout_file, "w", buffering=buffering)
    else:
        stdout_file = sys.stdout

    if redirect_stderr:
        path_stderr_file = os.path.join(dir_output, "stderr.txt")
        stderr_file = open(path_stderr_file, "w", buffering=buffering)
    else:
        stderr_file = sys.stderr

    return (stdout_file, stderr_file)


def get_for_run_or_default(book, run, property, default=None, verbose=False):

    if run is not None:
        try:
            return run[property]
        except KeyError:
            pass

    if verbose:
        print(f"No custom {property} property defined for recipe. Using default.")

    try:
        return book["default"][property]
    except KeyError:
        if default is None:
            raise ValueError(f"Recipe is ill defined - '{property}' neither defined for the recipe, nor a default given.")
        else: 
            return default

if __name__ == "__main__":
    main()
