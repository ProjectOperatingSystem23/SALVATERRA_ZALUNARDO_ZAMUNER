
/* ═══════════════════════════════════════════════════════════════════════════
 * Struct wire-format: messaggi scambiati tra processi via FIFO
 *
 * ATTENZIONE: queste struct vengono scritte e lette come blocchi binari
 * (write/read di sizeof(struct)). Tutti i processi devono usare esattamente
 * la stessa definizione — per questo stanno in common.h.
 * ═══════════════════════════════════════════════════════════════════════════ */

/* order.sh → warehouse (su ORDERS_FIFO) */
//USATA DAL C HELPER DI order.sh
typedef struct {
    char client_id[MAX_CLIENT_ID];
    char resp_fifo[MAX_RESP_FIFO];  /* path della FIFO privata del client */
    int  item_id;
    int  quantity;
} OrderRequest;

/* warehouse → order.sh (su resp_fifo privata del client) */
//IL C HELPER di order.sh USA QUESTA STRUCT? si
typedef struct {
    int err_code;       /* ERR_* code */
    int qty_shipped;
    int qty_rejected;
} OrderResponse;

/* supplier → warehouse (su RESTOCK_FIFO) */
typedef struct {
    int supplier_id;
    int item_id;
    int quantity;
} RestockMsg;

#endif /* COMMON_H */
