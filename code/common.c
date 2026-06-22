/* ============================================================================
 * common.c -- Implementation of the shared helpers declared in common.h.
 * ============================================================================ */

#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <sys/stat.h>
#include "common.h"

/* Install a handler via sigaction, sa_flags=0 (no SA_RESTART). */
void setup_handler(int sig, void (*handler)(int))
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(sig, &sa, NULL);
}

/* "Full" write with EINTR handling. */
ssize_t write_all(int fd, const void *buf, size_t len)
{
    size_t done = 0;
    while (done < len) {
        ssize_t n = write(fd, (const char *)buf + done, len - done);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        done += (size_t)n;
    }
    return (ssize_t)done;
}

/* FIFO read-end + dummy write-end, read-end made blocking. Prints nothing; on
 * error returns -1 with errno preserved across the cleanup close()s. */
int open_fifo_r_dw(const char *path, mode_t mode, int *read_fd, int *dummy_write_fd)
{
    if (mkfifo(path, mode) != 0 && errno != EEXIST)    /* idempotent */
        return -1;

    int rfd = open(path, O_RDONLY | O_NONBLOCK);       /* does not block without a writer */
    if (rfd < 0)
        return -1;

    int wfd = open(path, O_WRONLY);                    /* dummy: read-end already open */
    if (wfd < 0) {
        int e = errno; close(rfd); errno = e;
        return -1;
    }

    int fl = fcntl(rfd, F_GETFL, 0);                   /* clear O_NONBLOCK on the read-end */
    if (fl < 0 || fcntl(rfd, F_SETFL, fl & ~O_NONBLOCK) < 0) {
        int e = errno; close(rfd); close(wfd); errno = e;
        return -1;
    }

    *read_fd = rfd;
    *dummy_write_fd = wfd;
    return 0;
}

/* Read one line from fd, byte by byte. Returns bytes read, 0 = EOF, -1 = error. */
ssize_t read_line_from_fd(int fd, char *buf, size_t size)
{
    size_t i = 0;
    while (i < size - 1) {
        char c;
        ssize_t n = read(fd, &c, 1);
        if (n < 0) { if (errno == EINTR) continue; return -1; }
        if (n == 0) break;
        buf[i++] = c;
        if (c == '\n') break;
    }
    buf[i] = '\0';
    return (ssize_t)i;
}