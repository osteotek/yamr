from configparser import ConfigParser


def load(config_path):
    config = ConfigParser()
    config.read(config_path)
    return dict(config.items("YAMR"))
