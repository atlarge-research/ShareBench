import os

SPARK_SUBMIT = 'bin/spark-submit'

def get_submit_command(config, mode, args=[], conf=[], add_conf = ""):
    spark_submit = f"{get_source_path(config)}/{SPARK_SUBMIT}"

    if not os.path.isfile(spark_submit):
        raise FileNotFoundError(f"Error: \'{spark_submit}\' not found. Are you in the root directory of the project?")

    spark_flags = [
        f"--class {config['scala']['class']}"
        f"--properties-file {config['templates']['targets']['spark']['dst']}"
        f"--conf spark.kubernetes.driver.podTemplateFile={config['templates']['targets']['driver_pod']['dst']}"
        f"--conf spark.kubernetes.executor.podTemplateFile={config['templates']['targets']['executor_pod_simple']['dst']}"
        f"--deploy-mode cluster"
    ]

    print(args)

    command = ' '.join([
        spark_submit,
        ' '.join(spark_flags),
        ' '.join(list(map(lambda s: "--conf " + s, add_conf))),
        add_conf,
        get_jar_path(config),
        mode,
        ' '.join(args),
    ])

    command = ' '.join(command.split())

    return command

def get_jar_path(config):
    return f"local:///opt/{config['general']['name']}/{config['general']['name']}_{config['scala']['version_short']}-1.0.jar"

def get_full_name(config):
    config_spark = config['spark']
    return f"spark-{config_spark['version']}-{config_spark['type']}"

def get_source_path(config):
    return os.path.join(config['dirs']['spark'], get_full_name(config))