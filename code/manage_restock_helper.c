/* ============================================================================
 * manage_restock_helper.c  --  Helper C dello script manage.sh (Project 2026-3)
 *
 * Uso (invocato da manage.sh restock, NON direttamente dall'utente finale):
 *   ./manual_restock <item_id> <quantity>
 *
 * RUOLO (spec 2.2.8 "Restock an item by sending a message ... via IPC"):
 *   E' il lato "manager" del canale di restock. manage.sh e' Bash e NON sa
 *   scrivere struct binarie sulle FIFO (echo scriverebbe testo, che il
 *   warehouse interpreterebbe come byte sbagliati); quindi l'IPC vero lo fa
 *   questo helper in C, riusando ESATTAMENTE la struct RestockMsg di common.h
 *   (la stessa che usano i supplier reali). Flusso:
 *       1. valida item_id e quantity (entrambi >= 1);
 *       2. apre la RESTOCK_FIFO del warehouse in scrittura;
 *       3. scrive UN RestockMsg con supplier_id = MANUAL_RESTOCK_SUPPLIER_ID (0);
 *       4. ritorna uno degli ERR_* di common.h come exit code (spec 2.2.9).
 *
 *   Il restock e' ASINCRONO (fire-and-forget): il thread "restock" del
 *   warehouse legge il messaggio e incrementa lo stock quando lo elabora. Non
 *   c'e' FIFO di risposta (a differenza di order_helper): per vedere il nuovo
 *   stock si usa ./manage.sh status.
 *
 * GERARCHIA DI VALIDAZIONE (come order_helper / order.sh):
 *   - manage.sh     : input lato utente (item_id/qty interi >= 1);
 *   - manage_restock_helper: ricontrolla item_id/qty >= 1 (difesa in profondita' per
 *                     chi lo chiamasse direttamente) e la sicurezza del
 *                     wire-format;
 *   - warehouse     : autorita' semantica (item esistente? -> altrimenti
 *                     [RESTOCK] item non trovato; scarta item/qty <= 0).
 *
 * SCELTE DI PROGETTO (per la relazione):
 *   [APERTURA FIFO NON BLOCCANTE - Lab06]  Apriamo la RESTOCK_FIFO con
 *       O_WRONLY | O_NONBLOCK. Una open(O_WRONLY) BLOCCANTE su una FIFO senza
 *       lettori resterebbe appesa per sempre: se il warehouse e' morto ma il
 *       file FIFO e' rimasto (stale), un comando "manage.sh restock" si
 *       bloccherebbe. Con O_NONBLOCK la open fallisce subito con ENXIO
 *       ("no reader") e riportiamo ERR_WAREHOUSE_DOWN. Il messaggio e' di soli
 *       sizeof(RestockMsg)=12 byte << PIPE_BUF (4096): la write e' comunque
 *       ATOMICA e non blocca (man 7 pipe), quindi O_NONBLOCK sul lato scrittura
 *       non cambia la semantica della write.
 *   [SIGPIPE CON HANDLER - Lab03]  Come supplier.c/order_helper.c: il default
 *       di SIGPIPE terminerebbe il processo. Con l'handler installato, se il
 *       warehouse muore proprio mentre scriviamo, la write ritorna -1/EPIPE
 *       (gestito -> ERR_WAREHOUSE_DOWN) invece di ucciderci.
 *   [I/O CON FILE DESCRIPTOR - Lab05]  open/write/close, niente <stdio.h> per
 *       l'IPC; write_all() e setup_handler() sono gli helper condivisi di
 *       common.c (DRY).
 *
 * Riferimenti: Lab03 (segnali, sigaction), Lab05 (fd/IO, EINTR), Lab06 (FIFO).
 * ============================================================================ */

#include <stdio.h>      /* fprintf, perror            */
#include <stdlib.h>     /* atoi                       */
#include <unistd.h>     /* close                      */
#include <fcntl.h>      /* open, O_WRONLY, O_NONBLOCK */
#include <signal.h>     /* SIGPIPE                    */
#include <errno.h>      /* errno, EPIPE, ENXIO        */
#include <string.h>     /* strerror                   */
#include "common.h"     /* RestockMsg, RESTOCK_FIFO, ERR_*, helper condivisi */


/* Validazione "intero >= 1": rifiuta vuoto, segni, cifre + spazzatura.
 * atoi() non distingue "0" da "abc" (entrambi 0), quindi qui ci limitiamo a
 * controllare il valore gia' convertito; manage.sh fa il filtro sui caratteri
 * (case "$x" in *[!0-9]* ...) prima di chiamarci (spec 2.2.9). */
int main(int argc, char *argv[])
{
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <item_id> <quantity>\n", argv[0]);
        return ERR_USAGE;
    }

    int item_id  = atoi(argv[1]);
    int quantity = atoi(argv[2]);

    if (item_id <= 0) {
        fprintf(stderr, "[RESTOCK] item_id must be >= 1 (received '%s')\n", argv[1]);
        return ERR_USAGE;
    }
    if (quantity <= 0) {
        fprintf(stderr, "[RESTOCK] quantity must be >= 1 (received '%s')\n", argv[2]);
        return ERR_USAGE;
    }

    /* SIGPIPE gestito con handler (Lab03), come supplier.c/order_helper.c. */
    setup_handler(SIGPIPE, SIG_IGN);

    /* Apertura NON bloccante: se il warehouse non c'e' (nessun lettore della
     * FIFO) la open fallisce subito invece di bloccare per sempre. */
    int fd = open(RESTOCK_FIFO, O_WRONLY | O_NONBLOCK); /*open in non bloccante non può dare EINTR*/
    if (fd < 0) {
        /* ENXIO = FIFO esiste ma nessun lettore; ENOENT = FIFO assente:
         * in entrambi i casi il warehouse non e' pronto/attivo. */
        if (errno == ENXIO || errno == ENOENT) {
            fprintf(stderr, "[RESTOCK] warehouse down"
                            "(FIFO '%s': %s)\n", RESTOCK_FIFO, strerror(errno));
            return ERR_WAREHOUSE_DOWN;
        }
        fprintf(stderr, "[RESTOCK] failed to open '%s': %s\n", RESTOCK_FIFO, strerror(errno));
        return ERR_IO;
    }

    /* Componi e invia il messaggio (supplier_id = 0 -> restock MANUALE).
     * write_all() (common.c) gestisce le write parziali e EINTR (Lab05). */

    RestockMsg msg = { MANUAL_RESTOCK_SUPPLIER_ID, item_id, quantity };
    if (write_all(fd, &msg, sizeof(msg)) < 0) {
        fprintf(stderr, "[RESTOCK] failed to write on '%s': %s\n", RESTOCK_FIFO, strerror(errno));
        close(fd);
        return ERR_IO;
    }

    close(fd);
    return ERR_OK;
}