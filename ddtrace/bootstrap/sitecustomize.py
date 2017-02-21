from __future__ import print_function

"""
Bootstrapping code that is run when using the `ddtrace-run` Python entrypoint
Add all monkey-patching that needs to run by default here
"""
import os

try:
    from ddtrace import tracer
    enabled = os.environ.get("DATADOG_TRACE_ENABLED")
    if enabled and enabled.lower() == "false":
        tracer.configure(enabled=False)
    else:
        from ddtrace import patch_all; patch_all(django=True, flask=True, pylons=True) # noqa

    if 'DATADOG_ENV' in os.environ:
        tracer.set_tags({"env": os.environ["DATADOG_ENV"]})
except Exception as e:
    print("error configuring Datadog tracing")