import yaml


def load_kwargs(fpath):
    with open(fpath, 'r') as file:
        config = yaml.safe_load(file)
    return config
