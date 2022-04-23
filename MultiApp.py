import asyncio
from aiohttp import web


class AppWrapper:

    def __init__(self, aioapp, host, port, loop):
        self.host = host
        self.port = port
        self.aioapp = aioapp
        self.loop = loop
        self.uris = []
        self.runners = []
        self.sites = []

    def initialize(self):
        runner = web.AppRunner(self.aioapp)
        self.runners.append(runner)
        self.loop.run_until_complete(runner.setup())
        site = web.TCPSite(runner=runner, host=self.host, port=self.port)
        self.sites.append(site)
        self.uris.append(site.name)
        self.loop.run_until_complete(site.start())

    def shutdown(self):
        server_closures = []
        for site in self.sites:
            server_closures.append(site.stop())
        self.loop.run_until_complete(
            asyncio.gather(*server_closures))

        self.loop.run_until_complete(self.aioapp.shutdown())

    def cleanup(self):
        self.loop.run_until_complete(self.aioapp.cleanup())

    def show_info(self):
        print("======== Running on {} ======== ".format(' '.join(self.uris)))


class MultiApp:

    def __init__(self, loop=None):
        self._apps = []
        self.user_supplied_loop = loop is not None
        if loop is None:
            self.loop = asyncio.get_event_loop()
        else:
            self.loop = loop

    def configure_app(self, app, host, port):
        app._set_loop(self.loop)
        self._apps.append(
            AppWrapper(app, host, port, self.loop)
        )

    def run_all(self):
        try:
            for app in self._apps:
                app.initialize()
            try:
                for app in self._apps:
                    app.show_info()
                print("(Press CTRL+C to quit)")
                self.loop.run_forever()
            except KeyboardInterrupt:  # pragma: no cover
                pass
            finally:
                for app in self._apps:
                    app.shutdown()
        finally:
            for app in self._apps:
                app.cleanup()

        if not self.user_supplied_loop:
            self.loop.close()
