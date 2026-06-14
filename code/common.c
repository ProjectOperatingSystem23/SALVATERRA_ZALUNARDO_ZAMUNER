/* ============================================================================
 * common.c  --  Implementazione degli helper condivisi dichiarati in common.h
 *
 * Queste funzioni erano duplicate (identiche) in warehouse.c, supplier.c e
 * order_client.c. Centralizzarle qui -- un solo .c linkato in tutti gli
 * eseguibili -- evita la divergenza: una correzione vale per tutti. (DRY)
 *
 * Riferimenti: Lab03 (sigaction), Lab05 (fd/IO, EINTR), Lab06 (FIFO).
 * ============================================================================ */

#define _POSIX_C_SOURCE 200809L

#include <string.h>     /* memset             */
#include <unistd.h>     /* read, write, close */
#include <fcntl.h>      /* open, fcntl, O_*   */
#include <signal.h>     /* sigaction          */
#include <errno.h>      /* errno              */
#include <sys/stat.h>   /* mkfifo             */
#include "common.h"

/* Installa un handler con sigaction, sa_flags=0 (niente SA_RESTART). */
void setup_handler(int sig, void (*fn)(int))
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = fn;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(sig, &sa, NULL);
}

/* write "completa" con gestione EINTR. */
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
/* ── lettura di una riga da fd, byte per byte (Lab05) ──────────────────────
 * Con i soli fd (niente stdio) non c'e' una "readline" pronta: leggere 1 byte
 * alla volta e' la soluzione piu' semplice e corretta. Usata per CSV/.conf,
 * caricati una sola volta all'avvio: l'inefficienza e' irrilevante.
 * Ritorna i byte letti, 0 = EOF, -1 = errore. */
ssize_t fd_read_line(int fd, char *buf, size_t size)
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

/* FIFO in lettura + write-end "dummy" + read-end reso bloccante.
 * NON stampa: su errore ritorna -1 preservando errno (le close finali
 * potrebbero sovrascriverlo, quindi lo salviamo e ripristiniamo). */
int open_fifo_rw(const char *path, mode_t mode, int *read_fd, int *dummy_write_fd)
{
    if (mkfifo(path, mode) != 0 && errno != EEXIST)    /* idempotente */
        return -1;

    int rfd = open(path, O_RDONLY | O_NONBLOCK);       /* non blocca senza writer */
    if (rfd < 0)
        return -1;

    int wfd = open(path, O_WRONLY);                    /* dummy: read-end gia' aperto */
    if (wfd < 0) {
        int e = errno; close(rfd); errno = e;
        return -1;
    }

    /*TODO: QUESTI COMANDI NON SONO NEI LAB, VA BENE USARLI MA ESSERE PRONTI ALLE FRANZILLATE*/
    int fl = fcntl(rfd, F_GETFL, 0);                   /* togli O_NONBLOCK dal read-end */
    if (fl < 0 || fcntl(rfd, F_SETFL, fl & ~O_NONBLOCK) < 0) {
        int e = errno; close(rfd); close(wfd); errno = e;
        return -1;
    }

    *read_fd = rfd;
    *dummy_write_fd = wfd;
    return 0;
}
