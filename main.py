from Server import Server
from MultiApp import MultiApp
from aiohttp import web


# https://www.pythonfixing.com/2022/01/fixed-how-to-run-aiohttp-server-in.html
# https://localcoder.org/multiple-aiohttp-applications-running-in-the-same-process#solution_2

def main():
    srv = Server('0.0.0.0', port=80)
    app = web.Application()
    app.add_routes(srv.routes)
    # web.run_app(app=app, port=80)

    '''
    srv2 = Server('0.0.0.0', port=81)
    app2 = web.Application()
    app2.add_routes(srv2.routes)
    # web.run_app(app=app2, port=81)

    srv3 = Server('0.0.0.0', port=82)
    app3 = web.Application()
    app3.add_routes(srv3.routes)
    # web.run_app(app=app2, port=81)

    srv4 = Server('0.0.0.0', port=83)
    app4 = web.Application()
    app4.add_routes(srv4.routes)
    # web.run_app(app=app2, port=81)
    '''
    ma = MultiApp()
    ma.configure_app(app, host=srv.host, port=srv.port)
    '''
    ma.configure_app(app2, host=srv2.host, port=srv2.port)
    ma.configure_app(app3, host=srv3.host, port=srv3.port)
    ma.configure_app(app4, host=srv4.host, port=srv4.port)
    '''
    ma.run_all()


if __name__ == "__main__":
    main()
