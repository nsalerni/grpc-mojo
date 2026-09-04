# Probe: grpc.health.v1 Check on Server or PollingServer.

from std.sys import argv

from grpc import Health, PollingServer, Server, ServingStatus


def main() raises:
    var registry = Health()
    registry.set_status("echo.Echo", ServingStatus.SERVING)
    registry.set_status("down.Down", ServingStatus.NOT_SERVING)
    var args = argv()
    if len(args) > 1 and args[1] == "polling":
        var polling = PollingServer("127.0.0.1", 0)
        polling.add_health_service(registry^)
        polling.serve()
        return
    var server = Server("127.0.0.1", 0)
    server.add_health_service(registry^)
    server.serve()
