from configparser import ConfigParser


def load(config_path):
    config = ConfigParser()
    config.read(config_path)
    return {'JobTracker': dict(config.items("JobTracker"))}
