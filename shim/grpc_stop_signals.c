/* Async-signal-safe SIGTERM/SIGINT writer for PollingServer's self-pipe. */

#include <signal.h>
#include <unistd.h>

static int grpc_stop_write_fd = -1;

static void grpc_stop_handler(int sig) {
    unsigned char byte = 1;
    (void)sig;
    if (grpc_stop_write_fd >= 0) {
        (void)write(grpc_stop_write_fd, &byte, 1);
    }
}

int grpc_install_stop_signals(int write_fd) {
    struct sigaction action;
    grpc_stop_write_fd = write_fd;
    action.sa_handler = grpc_stop_handler;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    if (sigaction(SIGTERM, &action, 0) < 0) {
        return -1;
    }
    if (sigaction(SIGINT, &action, 0) < 0) {
        return -1;
    }
    return 0;
}
