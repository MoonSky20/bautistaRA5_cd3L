#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <netinet/tcp.h>
#include <sys/uio.h>
#include <sched.h>

#define MAX_NODES 17
#define SOCK_BUF (1 << 22) /* 4 MB socket buffer */

/* ------------------------------------------------------------------ */
/*  Wire headers                                                        */
/* ------------------------------------------------------------------ */
typedef struct
{
    int num_rows;   /* rows in this chunk                          */
    int tree_start; /* lower bound of this sub-tree                */
    int tree_end;   /* upper bound of this sub-tree                */
    int reply_port; /* ephemeral port the PARENT is listening on   */
    int global_min;
    int global_max;
    char parent_ip[64]; /* routable IP of the node that sent this      */
} header_t;

typedef struct
{
    int num_rows;
} result_header_t;

/* ------------------------------------------------------------------ */
/*  Scatter job (passed to send thread)                                 */
/* ------------------------------------------------------------------ */
typedef struct
{
    int target;
    const int *data;
    int rows;
    int tree_start;
    int tree_end;
    int cols;
    char (*ip)[64];
    int *ports;
    int reply_port;
    int global_min;
    int global_max;
    char sender_ip[64]; /* our own routable IP, embedded in header */
} send_job_t;

/* ------------------------------------------------------------------ */
/*  Core-affinity                                                       */
/* ------------------------------------------------------------------ */
static void pin_to_core(int rank)
{
    int ncores = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (ncores <= 0)
        return;
    cpu_set_t cs;
    CPU_ZERO(&cs);
    CPU_SET(rank % ncores, &cs);
    if (sched_setaffinity(0, sizeof(cs), &cs) != 0)
        perror("sched_setaffinity (non-fatal)");
}

/* ------------------------------------------------------------------ */
/*  Print helpers                                                       */
/* ------------------------------------------------------------------ */
static void print_matrix(const int *m, int rows, int cols, const char *lbl)
{
    printf("\n%s (%d x %d):\n", lbl, rows, cols);
    for (int i = 0; i < rows; i++)
    {
        for (int j = 0; j < cols; j++)
            printf("%4d ", m[i * cols + j]);
        printf("\n");
    }
    printf("\n");
    fflush(stdout);
}

static void print_matrix_d(const double *m, int rows, int cols, const char *lbl)
{
    printf("\n%s (%d x %d):\n", lbl, rows, cols);
    for (int i = 0; i < rows; i++)
    {
        for (int j = 0; j < cols; j++)
            printf("%7.4f ", m[i * cols + j]);
        printf("\n");
    }
    printf("\n");
    fflush(stdout);
}

/* ------------------------------------------------------------------ */
/*  Socket helpers                                                      */
/* ------------------------------------------------------------------ */
static int __attribute__((unused)) send_all(int fd, const void *buf, size_t len)
{
    const char *p = buf;
    while (len)
    {
        ssize_t n = send(fd, p, len, 0);
        if (n < 0)
        {
            if (errno == EINTR)
                continue;
            return -1;
        }
        p += n;
        len -= n;
    }
    return 0;
}

static int sendv_all(int fd,
                     const void *hdr, size_t hlen,
                     const void *data, size_t dlen)
{
    struct iovec iov[2] = {
        {.iov_base = (void *)hdr, .iov_len = hlen},
        {.iov_base = (void *)data, .iov_len = dlen}};
    size_t total = hlen + dlen;
    while (total)
    {
        ssize_t n = writev(fd, iov, 2);
        if (n < 0)
        {
            if (errno == EINTR)
                continue;
            return -1;
        }
        total -= (size_t)n;
        size_t c = (size_t)n;
        for (int i = 0; i < 2 && c; i++)
        {
            if (c >= iov[i].iov_len)
            {
                c -= iov[i].iov_len;
                iov[i].iov_len = 0;
            }
            else
            {
                iov[i].iov_base = (char *)iov[i].iov_base + c;
                iov[i].iov_len -= c;
                c = 0;
            }
        }
    }
    return 0;
}

static int recv_all(int fd, void *buf, size_t len)
{
    char *p = buf;
    while (len)
    {
        ssize_t n = recv(fd, p, len, 0);
        if (n <= 0)
        {
            if (n < 0 && errno == EINTR)
                continue;
            return -1;
        }
        p += n;
        len -= n;
    }
    return 0;
}

static void tune_socket(int fd)
{
    int v = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &v, sizeof(v));
    setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &v, sizeof(v));
    v = SOCK_BUF;
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &v, sizeof(v));
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &v, sizeof(v));
}

static int connect_retry(const char *host, int port, int max_tries)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        perror("socket");
        exit(1);
    }
    tune_socket(fd);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons(port)};
    inet_pton(AF_INET, host, &a.sin_addr);
    for (int t = 0; t < max_tries; t++)
    {
        if (connect(fd, (struct sockaddr *)&a, sizeof(a)) == 0)
            return fd;
        usleep(100000);
    }
    fprintf(stderr, "connect_retry: cannot reach %s:%d after %d tries\n",
            host, port, max_tries);
    exit(1);
}

/*
 * Bind to an OS-chosen ephemeral port, start listening.
 * Returns the server fd; writes the bound port into *port_out.
 */
static int make_listener(int *port_out)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        perror("socket make_listener");
        exit(1);
    }
    int v = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &v, sizeof(v));
    tune_socket(fd);
    struct sockaddr_in sa = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = INADDR_ANY,
        .sin_port = 0};
    if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0)
    {
        perror("bind make_listener");
        exit(1);
    }
    listen(fd, 8);
    socklen_t sl = sizeof(sa);
    getsockname(fd, (struct sockaddr *)&sa, &sl);
    *port_out = ntohs(sa.sin_port);
    return fd;
}

/*
 * Discover which local IP the kernel used when the peer connected.
 * getsockname() on the accepted fd returns the real interface address —
 * the exact IP the peer (our parent) can reach us on.
 * We use this instead of ip[rank] from config.txt, which may be
 * 0.0.0.0, wrong NIC, or an unresolvable hostname.
 */
static void local_ip_from_socket(int accepted_fd, char out[64])
{
    struct sockaddr_in sa;
    socklen_t sl = sizeof(sa);
    if (getsockname(accepted_fd, (struct sockaddr *)&sa, &sl) == 0)
        inet_ntop(AF_INET, &sa.sin_addr, out, 64);
    else
    {
        perror("getsockname (non-fatal, falling back to 127.0.0.1)");
        strncpy(out, "127.0.0.1", 64);
    }
}

/* ------------------------------------------------------------------ */
/*  Scatter                                                             */
/* ------------------------------------------------------------------ */
static void send_chunk(int target,
                       const int *data,
                       int rows, int tree_start, int tree_end,
                       char ip[][64], int *ports, int cols,
                       int reply_port,
                       int global_min, int global_max,
                       const char *my_reachable_ip)
{
    if (rows < 15)
    {
        char lbl[160];
        snprintf(lbl, sizeof(lbl),
                 "Sending to rank %d (%d rows, subtree [%d..%d])",
                 target, rows, tree_start, tree_end);
        print_matrix(data, rows, cols, lbl);
    }

    int fd = connect_retry(ip[target], ports[target], 100);

    header_t h = {
        .num_rows = rows,
        .tree_start = tree_start,
        .tree_end = tree_end,
        .reply_port = reply_port,
        .global_min = global_min,
        .global_max = global_max,
    };
    strncpy(h.parent_ip, my_reachable_ip, sizeof(h.parent_ip) - 1);

    if (sendv_all(fd, &h, sizeof(h),
                  data, (size_t)rows * cols * sizeof(int)) < 0)
    {
        fprintf(stderr, "send_chunk to rank %d failed\n", target);
        exit(1);
    }
    close(fd);
}

static void *send_chunk_worker(void *arg)
{
    send_job_t *j = arg;
    send_chunk(j->target, j->data, j->rows,
               j->tree_start, j->tree_end,
               j->ip, j->ports, j->cols,
               j->reply_port,
               j->global_min, j->global_max,
               j->sender_ip);
    return NULL;
}

/* ------------------------------------------------------------------ */
/*  Min-max normalisation                                               */
/* ------------------------------------------------------------------ */
static double *minmax_transform(const int *data, int rows, int cols,
                                int global_min, int global_max)
{
    int total = rows * cols;
    double *out = malloc((size_t)total * sizeof(double));
    if (!out)
    {
        perror("malloc minmax");
        exit(1);
    }
    double range = (double)(global_max - global_min);
    for (int i = 0; i < total; i++)
        out[i] = range > 0.0 ? (double)(data[i] - global_min) / range : 0.0;
    return out;
}

/* ------------------------------------------------------------------ */
/*  Collect one child's assembled result                                */
/* ------------------------------------------------------------------ */
static void collect_one_result(int srv_fd, double *dst, int rows, int cols)
{
    int cli = accept(srv_fd, NULL, NULL);
    if (cli < 0)
    {
        perror("accept result");
        exit(1);
    }
    tune_socket(cli);

    result_header_t rh;
    if (recv_all(cli, &rh, sizeof(rh)) < 0)
    {
        fprintf(stderr, "collect_one_result: header recv failed\n");
        exit(1);
    }
    if (rh.num_rows != rows)
    {
        fprintf(stderr, "collect_one_result: expected %d rows, got %d\n",
                rows, rh.num_rows);
        exit(1);
    }
    if (recv_all(cli, dst, (size_t)rows * cols * sizeof(double)) < 0)
    {
        fprintf(stderr, "collect_one_result: data recv failed\n");
        exit(1);
    }
    close(cli);
}

/* ------------------------------------------------------------------ */
/*  divide_and_scatter                                                  */
/*                                                                      */
/*  Recursively fans out rows to the right-half of our subtree,        */
/*  transforms the left half locally, collects the right result, and   */
/*  returns with assembled[] fully populated for [tree_start..tree_end]*/
/*                                                                      */
/*  my_reachable_ip: detected from the accepted incoming socket so     */
/*  children know exactly which address to route results back to.      */
/* ------------------------------------------------------------------ */
static int divide_and_scatter(int *data,
                              int tree_start, int tree_end,
                              int total_rows,
                              char ip[][64], int *ports, int cols,
                              double *assembled,
                              int global_min, int global_max,
                              const char *my_reachable_ip)
{
    int span = tree_end - tree_start + 1;

    if (span <= 1)
    {
        /* Leaf: transform locally */
        double *t = minmax_transform(data, total_rows, cols,
                                     global_min, global_max);
        memcpy(assembled, t, (size_t)total_rows * cols * sizeof(double));
        free(t);
        return total_rows;
    }

    int mid = tree_start + span / 2;
    int right_nodes = tree_end - mid + 1;
    int right_rows = (int)((long long)total_rows * right_nodes / span);
    int left_rows = total_rows - right_rows;

    /* Open our reply listener before spawning the send thread */
    int right_reply_port;
    int right_srv = make_listener(&right_reply_port);

    send_job_t rj = {
        .target = mid,
        .data = data + (size_t)left_rows * cols,
        .rows = right_rows,
        .tree_start = mid,
        .tree_end = tree_end,
        .cols = cols,
        .ip = ip,
        .ports = ports,
        .reply_port = right_reply_port,
        .global_min = global_min,
        .global_max = global_max,
    };
    strncpy(rj.sender_ip, my_reachable_ip, sizeof(rj.sender_ip) - 1);

    pthread_t rt;
    pthread_create(&rt, NULL, send_chunk_worker, &rj);

    /* Process left half recursively */
    int local_rows = divide_and_scatter(data,
                                        tree_start, mid - 1,
                                        left_rows,
                                        ip, ports, cols,
                                        assembled,
                                        global_min, global_max,
                                        my_reachable_ip);

    pthread_join(rt, NULL);

    /* Collect right child's result */
    collect_one_result(right_srv,
                       assembled + (size_t)left_rows * cols,
                       right_rows, cols);
    close(right_srv);

    return local_rows;
}

/* ------------------------------------------------------------------ */
/*  Send result back to the node that sent us work                      */
/* ------------------------------------------------------------------ */
static void send_result(const char *parent_ip, int reply_port,
                        const double *data, int rows, int cols)
{
    int fd = connect_retry(parent_ip, reply_port, 200);
    result_header_t rh = {.num_rows = rows};
    if (sendv_all(fd, &rh, sizeof(rh),
                  data, (size_t)rows * cols * sizeof(double)) < 0)
    {
        fprintf(stderr, "send_result failed\n");
        exit(1);
    }
    close(fd);
}

/* ================================================================== */
/*  MAIN                                                                */
/* ================================================================== */
int main(int argc, char *argv[])
{
    if (argc != 4)
    {
        fprintf(stderr, "Usage: %s <n> <p> <s>\n", argv[0]);
        return 1;
    }
    int n = atoi(argv[1]);
    int p = atoi(argv[2]);
    int s = atoi(argv[3]);
    printf("Running with: n=%d, p=%d, s=%d\n", n, p, s);

    struct timespec t0, t1;

    FILE *fp = fopen("config.txt", "r");
    if (!fp)
    {
        perror("config.txt");
        return 1;
    }
    char ip[MAX_NODES][64];
    int ports[MAX_NODES], N = 0;
    while (N < MAX_NODES && fscanf(fp, "%63s %d", ip[N], &ports[N]) == 2)
        N++;
    fclose(fp);

    /* Determine rank from port (s==1 → slave, s==0 → master) */
    int rank = 0;
    if (s == 1)
    {
        rank = -1;
        for (int i = 1; i < N; i++)
            if (ports[i] == p)
            {
                rank = i;
                break;
            }
        if (rank < 0)
        {
            fprintf(stderr, "Error: port %d not found in config.txt\n", p);
            return 1;
        }
    }

    /* Pin to a dedicated core */
    pin_to_core(rank);

    /* ==================== MASTER ================================== */
    if (rank == 0)
    {
        int *M = malloc((size_t)n * n * sizeof(int));
        if (!M)
        {
            perror("malloc M");
            return 1;
        }

        srand((unsigned)time(NULL));
        for (int i = 0; i < n * n; i++)
            M[i] = rand() % 100 + 1;
        if (n < 15)
            print_matrix(M, n, n, "MASTER: Initial Matrix");

        int global_min = M[0], global_max = M[0];
        for (int i = 1; i < n * n; i++)
        {
            if (M[i] < global_min)
                global_min = M[i];
            if (M[i] > global_max)
                global_max = M[i];
        }
        printf("MASTER: global_min=%d, global_max=%d\n", global_min, global_max);

        double *result = calloc((size_t)n * n, sizeof(double));
        if (!result)
        {
            perror("calloc result");
            return 1;
        }

        clock_gettime(CLOCK_MONOTONIC, &t0);

        if (N == 1)
        {
            double *t = minmax_transform(M, n, n, global_min, global_max);
            memcpy(result, t, (size_t)n * n * sizeof(double));
            free(t);
        }
        else if (N == 2)
        {
            int reply_port;
            int reply_srv = make_listener(&reply_port);
            send_chunk(1, M, n, 1, 1, ip, ports, n,
                       reply_port, global_min, global_max,
                       ip[0]);
            collect_one_result(reply_srv, result, n, n);
            close(reply_srv);
        }
        else
        {
            int slaves = N - 1;
            int mid = 1 + slaves / 2;
            int left_rows = (int)((long long)n * (mid - 1) / slaves);
            int right_rows = n - left_rows;

            int left_reply_port, right_reply_port;
            int left_srv = make_listener(&left_reply_port);
            int right_srv = make_listener(&right_reply_port);

            send_job_t lj = {
                .target = 1,
                .data = M,
                .rows = left_rows,
                .tree_start = 1,
                .tree_end = mid - 1,
                .cols = n,
                .ip = ip,
                .ports = ports,
                .reply_port = left_reply_port,
                .global_min = global_min,
                .global_max = global_max,
            };
            memcpy(lj.sender_ip, ip[0], sizeof(lj.sender_ip));
            lj.sender_ip[sizeof(lj.sender_ip) - 1] = 0;

            send_job_t rj = {
                .target = mid,
                .data = M + (size_t)left_rows * n,
                .rows = right_rows,
                .tree_start = mid,
                .tree_end = N - 1,
                .cols = n,
                .ip = ip,
                .ports = ports,
                .reply_port = right_reply_port,
                .global_min = global_min,
                .global_max = global_max,
            };
            memcpy(rj.sender_ip, ip[0], sizeof(rj.sender_ip));
            rj.sender_ip[sizeof(rj.sender_ip) - 1] = 0;

            pthread_t lt, rt;
            pthread_create(&lt, NULL, send_chunk_worker, &lj);
            pthread_create(&rt, NULL, send_chunk_worker, &rj);
            pthread_join(lt, NULL);
            pthread_join(rt, NULL);

            collect_one_result(left_srv,
                               result, left_rows, n);
            collect_one_result(right_srv,
                               result + (size_t)left_rows * n, right_rows, n);
            close(left_srv);
            close(right_srv);
        }

        clock_gettime(CLOCK_MONOTONIC, &t1);
        if (n < 15)
            print_matrix_d(result, n, n, "MASTER: Min-Max Transformed Matrix");
        printf("MASTER time elapsed %f\n",
               (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9);

        free(result);
        free(M);
    }
    /* ==================== SLAVE =================================== */
    else
    {
        int srv = socket(AF_INET, SOCK_STREAM, 0);
        {
            int v = 1;
            setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &v, sizeof(v));
        }
        tune_socket(srv);
        {
            struct sockaddr_in sa = {
                .sin_family = AF_INET,
                .sin_addr.s_addr = INADDR_ANY,
                .sin_port = htons(p)};
            bind(srv, (struct sockaddr *)&sa, sizeof(sa));
        }
        listen(srv, 5);

        int cli = accept(srv, NULL, NULL);
        tune_socket(cli);
        clock_gettime(CLOCK_MONOTONIC, &t0);

        /*
         * KEY FIX: discover our own routable IP from the accepted socket.
         * getsockname() on the accepted fd gives the real interface the
         * kernel used — exactly what our parent (and our own children)
         * can reach. We never trust ip[rank] from config.txt for this
         * because it can be 0.0.0.0, wrong NIC, or an unresolvable name.
         */
        char my_reachable_ip[64] = "127.0.0.1";
        local_ip_from_socket(cli, my_reachable_ip);
        printf("SLAVE %d: my reachable IP = %s\n", rank, my_reachable_ip);

        header_t hdr;
        if (recv_all(cli, &hdr, sizeof(hdr)) < 0)
        {
            fprintf(stderr, "Slave %d: header recv failed\n", rank);
            exit(1);
        }

        int *data = malloc((size_t)hdr.num_rows * n * sizeof(int));
        if (!data)
        {
            perror("malloc data");
            exit(1);
        }
        if (recv_all(cli, data, (size_t)hdr.num_rows * n * sizeof(int)) < 0)
        {
            fprintf(stderr, "Slave %d: data recv failed\n", rank);
            exit(1);
        }
        close(cli);
        close(srv);

        if (n < 15)
        {
            char lbl[128];
            snprintf(lbl, sizeof(lbl),
                     "SLAVE %d: Received chunk (%d rows, subtree [%d..%d])",
                     rank, hdr.num_rows, hdr.tree_start, hdr.tree_end);
            print_matrix(data, hdr.num_rows, n, lbl);
        }

        double *assembled = malloc((size_t)hdr.num_rows * n * sizeof(double));
        if (!assembled)
        {
            perror("malloc assembled");
            exit(1);
        }

        int local_rows = divide_and_scatter(
            data,
            hdr.tree_start, hdr.tree_end,
            hdr.num_rows,
            ip, ports, n,
            assembled,
            hdr.global_min, hdr.global_max,
            my_reachable_ip); /* ← the fix */

        if (n < 15)
        {
            char lbl[128];
            snprintf(lbl, sizeof(lbl),
                     "SLAVE %d: Assembled result (%d rows, subtree [%d..%d])",
                     rank, hdr.num_rows, hdr.tree_start, hdr.tree_end);
            print_matrix_d(assembled, hdr.num_rows, n, lbl);
        }

        /*
         * Reply to whoever sent us work: hdr.parent_ip + hdr.reply_port.
         * This is the actual routable IP of our parent node, not ip[0].
         */
        printf("SLAVE %d: returning %d rows → %s:%d\n",
               rank, hdr.num_rows, hdr.parent_ip, hdr.reply_port);
        send_result(hdr.parent_ip, hdr.reply_port,
                    assembled, hdr.num_rows, n);

        clock_gettime(CLOCK_MONOTONIC, &t1);
        printf("SLAVE %d: done. local_rows=%d subtree [%d..%d] time=%f\n",
               rank, local_rows, hdr.tree_start, hdr.tree_end,
               (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9);

        free(assembled);
        free(data);
    }

    return 0;
}