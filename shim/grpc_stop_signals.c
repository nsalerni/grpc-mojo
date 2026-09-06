/* Async-signal-safe SIGTERM/SIGINT writer for PollingServer's self-pipe. */

#include <signal.h>
#include <unistd.h>

static volatile sig_atomic_t grpc_stop_write_fd = -1;

static void grpc_stop_handler(int sig) {
    unsigned char byte = 1;
    (void)sig;
    if (grpc_stop_write_fd >= 0) {
        /* GCC -Werror=unused-result ignores a void cast on write(2). */
        ssize_t n = write((int)grpc_stop_write_fd, &byte, 1);
        (void)n;
    }
}

int grpc_install_stop_signals(int write_fd) {
    struct sigaction action;
    grpc_stop_write_fd = write_fd;
    action.sa_handler = grpc_stop_handler;
    sigemptyset(&action.sa_mask);
    action.sa_flags = SA_RESTART;
    if (sigaction(SIGTERM, &action, 0) < 0) {
        return -1;
    }
    if (sigaction(SIGINT, &action, 0) < 0) {
        return -1;
    }
    return 0;
}
