from ...ext import AppTypes
from ...compat import stringify


class TraceMiddleware(object):
    """
    aiohttp Middleware class that will append a middleware coroutine to trace
    incoming traffic.

    TODO: this class must be moved in a contrib.aiohttp.middleware module
    """
    def __init__(self, app, tracer, service='aiohttp'):
        self.app = app
        self._tracer = tracer
        self._service = service

        # configure the current service
        self._tracer.set_service_info(
            service=service,
            app='aiohttp',
            app_type=AppTypes.web,
        )

        # add the async tracer middleware
        self.app.middlewares.append(self.middleware_factory())
        self.app.on_response_prepare.append(self.signal_factory())

    def middleware_factory(self):
        """
        The middleware factory returns an aiohttp middleware that traces the handler execution.
        Because handlers are run in different tasks for each request, we attach the Context
        instance both to the Task and to the Request objects. In this way:
            * the Task may be used by the internal tracing
            * the Request remains the main Context carrier if it should be passed as argument
              to the tracer.trace() method
        """
        # make the tracer available in the nested functions
        tracer = self._tracer

        async def middleware(app, handler, tracer=tracer):
            async def attach_context(request):
                # attach the context to the request
                ctx = tracer.get_call_context(loop=request.app.loop)
                request['__datadog_context'] = ctx
                # trace the handler
                request_span = tracer.trace('handler_request', ctx=ctx, service='aiohttp-web')
                request['__datadog_request_span'] = request_span
                return await handler(request)
            return attach_context
        return middleware

    def signal_factory(self):
        """
        The signal factory returns the on_prepare signal that is sent while the Response is
        being prepared. The signal is used to close the request span that is created during
        the trace middleware execution.
        """
        async def on_prepare(request, response):
            # TODO: it may raise an exception if it's missing
            request_span = request['__datadog_request_span']

            # default resource name
            resource = stringify(response.status)

            if request.match_info.route.resource:
                # collect the resource name based on http resource type
                res_info = request.match_info.route.resource.get_info()

                if res_info.get('path'):
                    resource = res_info.get('path')
                elif res_info.get('formatter'):
                    resource = res_info.get('formatter')
                elif res_info.get('prefix'):
                    resource = res_info.get('prefix')

            request_span.resource = resource
            request_span.set_tag('http.method', request.method)
            request_span.set_tag('http.status_code', response.status)
            request_span.set_tag('http.url', request.path)
            request_span.finish()
        return on_prepare