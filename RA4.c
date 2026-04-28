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
#include <sys/epoll.h>
#include <sched.h>

#define MAX_NODES 17
#define SOCK_BUF (1 << 22) // 4MB Socket buffer size

/**
 Data of the slave to know what to send
 */
typedef struct
{
    int num_rows;   // number of matrix rows in this chunk
    int tree_start; // lower bound of node ranks in this subtree
    int tree_end;   // upper bound of node ranks in this subtree
} header_t;

/**
 Struct to be used to be able to target which slave to pass down in log t
 */
typedef struct
{
    int target;      // rank of the destination node
    const int *data; // pointer to the matrix data buffer
    int rows;        // number of rows to send
    int tree_start;  // start of the subtree range for the target
    int tree_end;    // end of the subtree range for the target
    int cols;        // total columns in the matrix (N)
    char (*ip)[64];  // pointer to the IP address table
    int *ports;      // pointer to the port table
} send_job_t;

/* Print Matrix Helper */
static void print_matrix(const int *matrix, int rows, int cols, const char *label)
{
    printf("\n%s (%d x %d):\n", label, rows, cols);
    for (int i = 0; i < rows; i++)
    {
        for (int j = 0; j < cols; j++)
            printf("%4d ", matrix[i * cols + j]);
        printf("\n");
    }
    printf("\n");
    fflush(stdout);
}

/**
 * Robustly sends 'len' bytes over a socket, handling partial sends
 * and signal interruptions (EINTR).
 */
static int send_all(int fd, const void *buf, size_t len)
{
    const char *p = (const char *)buf;
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
        len -= (size_t)n;
    }
    return 0;
}

/**
 * Uses vectored I/O (writev) to send a header and data buffer
 * atomically/efficiently without needing an intermediate copy.
 */
static int sendv_all(int fd, const void *hdr, size_t hlen,
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
        size_t consumed = (size_t)n;
        // Update iovec structures to reflect bytes already sent
        for (int i = 0; i < 2 && consumed; i++)
        {
            if (consumed >= iov[i].iov_len)
            {
                consumed -= iov[i].iov_len;
                iov[i].iov_len = 0;
            }
            else
            {
                iov[i].iov_base = (char *)iov[i].iov_base + consumed;
                iov[i].iov_len -= consumed;
                consumed = 0;
            }
        }
    }
    return 0;
}

/**
 * Robustly receives 'len' bytes, handling partial reads and EINTR.
 */
static int recv_all(int fd, void *buf, size_t len)
{
    char *p = (char *)buf;
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
        len -= (size_t)n;
    }
    return 0;
}

/*
 * for high performance
 */
static void tune_socket(int fd)
{
    int val = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
    setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &val, sizeof(val));
    val = SOCK_BUF;
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &val, sizeof(val));
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &val, sizeof(val));
}

/**
 * Attempts to connect to a server with a retry mechanism.
 * Useful when slaves and master start nearly simultaneously.
 */
static int connect_retry(const char *host, int port, int max_tries)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        perror("socket");
        exit(1);
    }

    // for better performance lang
    tune_socket(fd);

    // socket to connect to
    struct sockaddr_in a = {
        .sin_family = AF_INET,
        .sin_port = htons(port)};
    inet_pton(AF_INET, host, &a.sin_addr);

    for (int t = 0; t < max_tries; t++)
    {
        // connect socket
        if (connect(fd, (struct sockaddr *)&a, sizeof(a)) == 0)
            return fd;
        usleep(100000); // Wait 100ms between attempts
    }
    fprintf(stderr, "connect_retry: cannot reach %s:%d\n", host, port);
    exit(1);
}

/**
 * Establishes a connection and sends a matrix chunk preceded by a header.
 */
static void send_chunk(int target, const int *data,
                       int rows, int tree_start, int tree_end,
                       char ip[][64], int *ports, int cols)
{
    if (rows < 15)
    {
        char label[128];
        snprintf(label, sizeof(label),
                 "Sending to rank %d (%d rows, subtree [%d..%d])",
                 target, rows, tree_start, tree_end);
        print_matrix(data, rows, cols, label);
    }

    // connect to target slave of rank
    int fd = connect_retry(ip[target], ports[target], 100);

    // create a header that contains what data to send
    header_t h = {
        .num_rows = rows,
        .tree_start = tree_start,
        .tree_end = tree_end};

    // sendv_all to send
    if (sendv_all(fd, &h, sizeof(h),
                  data, (size_t)rows * cols * sizeof(int)) < 0)
    {
        fprintf(stderr, "send_chunk → rank %d failed\n", target);
        exit(1);
    }
    close(fd);
}

/* pthread wrapper for send_chunk */
static void *send_chunk_worker(void *arg)
{
    send_job_t *j = (send_job_t *)arg;
    send_chunk(j->target, j->data, j->rows,
               j->tree_start, j->tree_end,
               j->ip, j->ports, j->cols);
    return NULL;
}

/**
 * Recursively distributes the matrix using a tree-based scatter approach.
 * Each node keeps a portion and forwards the rest to its children in the tree.
 */
static int divide_conquer(int *data, int tree_start, int tree_end,
                          int total_rows,
                          char ip[][64], int *ports, int cols)
{
    int span = tree_end - tree_start + 1;
    if (span <= 1)
        return total_rows; // Leaf node: keep all remaining rows

    // Calculate split point for binary tree
    int mid = tree_start + span / 2;
    int right_nodes = tree_end - mid + 1;
    int right_rows = (int)((long long)total_rows * right_nodes / span);
    int left_rows = total_rows - right_rows;

    // Asynchronously send the right-hand subtree's data
    send_job_t rj = {
        .target = mid,
        .data = data + (size_t)left_rows * cols,
        .rows = right_rows,
        .tree_start = mid,
        .tree_end = tree_end,
        .cols = cols,
        .ip = ip,
        .ports = ports};
    pthread_t rt;
    pthread_create(&rt, NULL, send_chunk_worker, &rj);
    // Recursively process the left-hand subtree locally
    int local_rows = divide_conquer(data, tree_start, mid - 1,
                                    left_rows, ip, ports, cols);

    pthread_join(rt, NULL);
    return local_rows;
}

// MAIN CODE
int main(int argc, char *argv[])
{
    int n, p, s;

    // check if the user provided the correct number of arguments
    if (argc != 4)
    {
        fprintf(stderr, "Usage: %s <n> <p> <s>\n", argv[0]);
        return 1;
    }

    // convert string arguments to integers
    n = atoi(argv[1]); // matrix size
    p = atoi(argv[2]); // port
    s = atoi(argv[3]); // 0=master, 1=slave

    printf("Running with: n=%d, p=%d, s=%d\n", n, p, s);

    struct timespec t0, t1;

    // open network configuration
    FILE *fp = fopen("config.txt", "r");
    if (!fp)
    {
        perror("config.txt");
        return 1;
    }

    // create an array of character for each t
    char ip[MAX_NODES][64];
    int ports[MAX_NODES], N = 0;
    // read each ip and port
    while (N < MAX_NODES && fscanf(fp, "%63s %d", ip[N], &ports[N]) == 2)
        N++;
    fclose(fp);

    // Identify current node's rank based on the port provided in stdin
    // rank is used to determine for one-to-many personalized broadcast
    int rank = 0;
    if (s == 1)
    {
        rank = -1;
        for (int i = 1; i < N; i++)
        {
            if (ports[i] == p)
            {
                rank = i;
                break;
            }
        }
        if (rank < 0)
        {
            fprintf(stderr, "Error: port %d not found in config.txt\n", p);
            return 1;
        }
    }

    /* ==================== MASTER LOGIC ============================ */
    if (rank == 0)
    {
        int *M = malloc((size_t)n * n * sizeof(int));
        if (!M)
        {
            perror("malloc");
            return 1;
        }

        // Initialize matrix with random values
        srand((unsigned)time(NULL));
        for (int i = 0; i < n * n; i++)
            M[i] = rand() % 100 + 1;
        if (n < 15)
        {
            print_matrix(M, n, n, "MASTER: Initial Matrix");
        }

        // setup server socket to listen for ACKs from slaves
        int srv = socket(AF_INET, SOCK_STREAM, 0);
        int opt = 1;
        setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        tune_socket(srv);
        struct sockaddr_in sa = {
            .sin_family = AF_INET,
            .sin_addr.s_addr = INADDR_ANY,
            .sin_port = htons(ports[0] + 1000)};
        bind(srv, (struct sockaddr *)&sa, sizeof(sa));
        listen(srv, MAX_NODES);

        // record time
        clock_gettime(CLOCK_MONOTONIC, &t0);

        // begin one to many-personalized
        if (N == 2) // slave + master
        {

            send_chunk(1, M, n, 1, 1, ip, ports, n);
        }
        else if (N > 2) // more than one slave
        {
            // Split distribution into two main branches to parallelize initial send
            int slaves = N - 1;
            int mid = 1 + slaves / 2; // get index of middle
            // number of right and left rows
            int left_rows = (int)((long long)n * (mid - 1) / slaves);
            int right_rows = n - left_rows;

            // create for left and right jobs
            send_job_t lj = {
                .target = 1, .data = M, .rows = left_rows, .tree_start = 1, .tree_end = mid - 1, .cols = n, .ip = ip, .ports = ports};
            send_job_t rj = {
                .target = mid, .data = M + (size_t)left_rows * n, .rows = right_rows, .tree_start = mid, .tree_end = N - 1, .cols = n, .ip = ip, .ports = ports};
            pthread_t lt, rt;
            pthread_create(&lt, NULL, send_chunk_worker, &lj);
            pthread_create(&rt, NULL, send_chunk_worker, &rj);
            pthread_join(lt, NULL);
            pthread_join(rt, NULL);
        }

        // uses epoll to wit for all ACKS
        // used gemini AI for this to efficiently wait for ACKs
        int epfd = epoll_create1(0); // epoll instance
        struct epoll_event ev = {.events = EPOLLIN, .data.fd = srv};
        epoll_ctl(epfd, EPOLL_CTL_ADD, srv, &ev);

        struct epoll_event events[MAX_NODES];
        int acks = 0;
        while (acks < N - 1) // waits until all slaves sends an ack
        {
            int ready = epoll_wait(epfd, events, MAX_NODES, -1); // allows to receive multiple ready
            for (int i = 0; i < ready; i++)
            {
                int c = accept(srv, NULL, NULL); // Connection itself is the ACK
                close(c);
                acks++;
            }
        }
        close(epfd);
        close(srv);

        clock_gettime(CLOCK_MONOTONIC, &t1);
        printf("MASTER: All ACKs received.\n");
        printf("MASTER time elapsed %f\n",
               (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9);
        free(M);
    }
    /* ==================== SLAVE LOGIC ============================= */
    else
    {
        // Setup server to receive the chunk from parent node
        int srv = socket(AF_INET, SOCK_STREAM, 0);
        int opt = 1;
        setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        tune_socket(srv);
        struct sockaddr_in sa = {
            .sin_family = AF_INET,
            .sin_addr.s_addr = INADDR_ANY,
            .sin_port = htons(p)};
        bind(srv, (struct sockaddr *)&sa, sizeof(sa));
        listen(srv, 5);

        int cli = accept(srv, NULL, NULL);
        tune_socket(cli);
        clock_gettime(CLOCK_MONOTONIC, &t0);

        // Receive header followed by matrix data
        header_t hdr;
        if (recv_all(cli, &hdr, sizeof(hdr)) < 0)
        {
            fprintf(stderr, "Slave %d: header recv failed\n", rank);
            exit(1);
        }

        int *data = malloc((size_t)n * n * sizeof(int));
        if (!data)
        {
            perror("malloc");
            exit(1);
        }

        if (recv_all(cli, data, (size_t)hdr.num_rows * n * sizeof(int)) < 0)
        {
            fprintf(stderr, "Slave %d: data recv failed\n", rank);
            exit(1);
        }
        close(cli);
        close(srv);

        char label[128];
        snprintf(label, sizeof(label),
                 "SLAVE %d: Received chunk (%d rows, subtree [%d..%d])",
                 rank, hdr.num_rows, hdr.tree_start, hdr.tree_end);

        if (n < 15)
        {
            print_matrix(data, hdr.num_rows, n, label);
        }

        // Forward the appropriate portions to sub-slaves in the tree
        int local_rows = divide_conquer(data, hdr.tree_start, hdr.tree_end,
                                        hdr.num_rows, ip, ports, n);

        snprintf(label, sizeof(label),
                 "SLAVE %d: My local chunk (%d rows kept)",
                 rank, local_rows);
        if (n < 15)
        {
            print_matrix(data, local_rows, n, label);
        }

        // Send ACK back to the Master (Port + 1000)
        int ack = connect_retry(ip[0], ports[0] + 1000, 100);
        send_all(ack, "ACK", 4);
        close(ack);

        clock_gettime(CLOCK_MONOTONIC, &t1);
        printf("SLAVE %d: Task complete. (%d local rows out of %d received, subtree [%d..%d])\n",
               rank, local_rows, hdr.num_rows, hdr.tree_start, hdr.tree_end);
        printf("SLAVE %d time elapsed %f\n", rank,
               (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9);
        free(data);
    }

    return 0;
}