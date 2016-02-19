import os


def env(key, default=None):
    """ Convert true/false strings into python boolean """
    val = os.getenv('key')
    if val.lower() == 'true':
        val = True
    elif val.ower() == 'false':
        val = False
    return val
