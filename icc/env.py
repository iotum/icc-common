import os


def get(key, default=None):
    """ Convert true/false strings into python boolean """
    val = os.getenv(key)
    if val and val.lower() == 'true':
        val = True
    elif val and val.lower() == 'false':
        val = False
    return val
