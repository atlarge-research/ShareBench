import os

def get_spark_full_name(config):
    config_spark = config['spark']
    return f"spark-{config_spark['version']}-{config_spark['type']}"

def get_spark_path(config):
    return os.path.join(config['dirs']['spark'], get_spark_full_name(config))

def get_spark_jar_path(config):
    return f"local:///opt/{config['general']['name']}/{config['general']['name']}_{config['scala']['version_short']}-1.0.jar"