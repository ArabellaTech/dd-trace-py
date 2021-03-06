"""Patch librairies to be automatically instrumented.

It can monkey patch supported standard libraries and third party modules.
A patched module will automatically report spans with its default configuration.

A library instrumentation can be configured (for instance, to report as another service)
using Pin. For that, check its documentation.
"""
import logging
import importlib
import threading


# Default set of modules to automatically patch or not
PATCH_MODULES = {
    'cassandra': True,
    'celery': True,
    'elasticsearch': True,
    'mongoengine': True,
    'mysql': True,
    'psycopg': True,
    'pylibmc': True,
    'pymongo': True,
    'redis': True,
    'requests': False,  # Not ready yet
    'sqlalchemy': False,  # Prefer DB client instrumentation
    'sqlite3': True,
}

_LOCK = threading.Lock()
_PATCHED_MODULES = set()


def patch_all(**patch_modules):
    """ Automatically patches all available modules.

    :param dict **patch_modules: Override whether particular modules
            are patched or not.

    >>> patch_all({'redis': False, 'cassandra': False})
    """
    modules = PATCH_MODULES.copy()
    modules.update(patch_modules)

    patch(raise_errors=False, **modules)

def patch(raise_errors=True, **patch_modules):
    """ Patch a set of given modules

    :param bool raise_errors: Raise error if one patch fail.
    :param dict **patch_modules: List of modules to patch.
        Example: {'psycopg': True, 'elasticsearch': True}
    """
    modules = [m for (m, should_patch) in patch_modules.items() if should_patch]
    count = 0
    for module in modules:
        patched = patch_module(module, raise_errors=raise_errors)
        if patched:
            count += 1

    logging.info("patched %s/%s modules (%s)",
        count,
        len(modules),
        ",".join(get_patched_modules()))


def patch_module(module, raise_errors=True):
    """Patch a single module

    Returns if the module got properly patched.
    """
    try:
        return _patch_module(module)
    except Exception as exc:
        if raise_errors:
            raise
        logging.debug("failed to patch %s: %s", module, exc)
        return False

def get_patched_modules():
    """Get the list of patched modules"""
    with _LOCK:
        return sorted(_PATCHED_MODULES)

def _patch_module(module):
    """_patch_module will attempt to monkey patch the module.

    Returns if the module got patched.
    Can also raise errors if it fails.
    """
    path = 'ddtrace.contrib.%s' % module
    with _LOCK:
        if module in _PATCHED_MODULES:
            logging.debug("already patched: %s", path)
            return False

        imported_module = importlib.import_module(path)
        imported_module.patch()

        _PATCHED_MODULES.add(module)
        return True
