#define SEC(name) __attribute__((section(name), used))
typedef unsigned char __u8;
typedef unsigned short __u16;
typedef unsigned int __u32;
typedef unsigned long long __u64;

#define BPF_MAP_TYPE_HASH 1
#define BPF_FUNC_map_lookup_elem 1
#define BPF_FUNC_skb_store_bytes 9
#define BPF_FUNC_l3_csum_replace 10
#define BPF_FUNC_l4_csum_replace 11
#define BPF_FUNC_redirect 23
#define BPF_FUNC_fib_lookup 69
#define BPF_F_RECOMPUTE_CSUM (1ULL << 0)
#define BPF_F_PSEUDO_HDR (1ULL << 4)
#define BPF_F_MARK_MANGLED_0 (1ULL << 5)
#define BPF_FIB_LKUP_RET_SUCCESS 0
#define TC_ACT_OK 0
#define ETH_P_IP 0x0800
#define IPPROTO_UDP 17
#define AF_INET 2

struct __sk_buff {
    __u32 len;
    __u32 pkt_type;
    __u32 mark;
    __u32 queue_mapping;
    __u32 protocol;
    __u32 vlan_present;
    __u32 vlan_tci;
    __u32 vlan_proto;
    __u32 priority;
    __u32 ingress_ifindex;
    __u32 ifindex;
    __u32 tc_index;
    __u32 cb[5];
    __u32 hash;
    __u32 tc_classid;
    __u32 data;
    __u32 data_end;
};

struct ethhdr {
    __u8 destination[6];
    __u8 source[6];
    __u16 protocol;
};

struct iphdr {
    __u8 ihl_version;
    __u8 tos;
    __u16 total_length;
    __u16 id;
    __u16 fragment_offset;
    __u8 ttl;
    __u8 protocol;
    __u16 checksum;
    __u32 source;
    __u32 destination;
};

struct udphdr {
    __u16 source;
    __u16 destination;
    __u16 length;
    __u16 checksum;
};

struct bpf_fib_lookup {
    __u8 family;
    __u8 l4_protocol;
    __u16 sport;
    __u16 dport;
    __u16 tot_len;
    __u32 ifindex;
    union {
        __u8 tos;
        __u32 flowinfo;
        __u32 rt_metric;
    };
    union {
        __u32 ipv4_src;
        __u32 ipv6_src[4];
    };
    union {
        __u32 ipv4_dst;
        __u32 ipv6_dst[4];
    };
    union {
        struct {
            __u16 h_vlan_proto;
            __u16 h_vlan_tci;
        };
        __u32 tbid;
    };
    __u8 smac[6];
    __u8 dmac[6];
};

struct fastpath_flow_key {
    __u32 ingress_ifindex;
    __u32 local_ip;
    __u32 remote_ip;
    __u16 local_port;
    __u16 remote_port;
    __u8 protocol;
    __u8 padding[3];
};

struct fastpath_flow_action {
    __u32 egress_ifindex;
    __u32 rewritten_src_ip;
    __u32 rewritten_dst_ip;
    __u16 rewritten_src_port;
    __u16 rewritten_dst_port;
    __u32 direction;
    __u64 generation;
    __u64 packets;
    __u64 bytes;
    __u64 redirect_errors;
};

struct bpf_map_def {
    __u32 type;
    __u32 key_size;
    __u32 value_size;
    __u32 max_entries;
    __u32 map_flags;
};

struct bpf_map_def SEC("maps") RTP_FLOWS = {
    .type = BPF_MAP_TYPE_HASH,
    .key_size = sizeof(struct fastpath_flow_key),
    .value_size = sizeof(struct fastpath_flow_action),
    .max_entries = 65536,
};

static void *(*bpf_map_lookup_elem)(void *map, const void *key) =
    (void *)BPF_FUNC_map_lookup_elem;
static long (*bpf_skb_store_bytes)(struct __sk_buff *skb, __u32 offset,
                                   const void *from, __u32 len, __u64 flags) =
    (void *)BPF_FUNC_skb_store_bytes;
static long (*bpf_l3_csum_replace)(struct __sk_buff *skb, __u32 offset,
                                   __u64 from, __u64 to, __u64 size) =
    (void *)BPF_FUNC_l3_csum_replace;
static long (*bpf_l4_csum_replace)(struct __sk_buff *skb, __u32 offset,
                                   __u64 from, __u64 to, __u64 flags) =
    (void *)BPF_FUNC_l4_csum_replace;
static long (*bpf_redirect)(__u32 ifindex, __u64 flags) =
    (void *)BPF_FUNC_redirect;
static long (*bpf_fib_lookup)(struct __sk_buff *skb,
                              struct bpf_fib_lookup *params,
                              int plen, __u32 flags) =
    (void *)BPF_FUNC_fib_lookup;

static __inline __u16 byte_swap_16(__u16 value) {
    return __builtin_bswap16(value);
}

SEC("classifier/rtp_fastpath")
int rtp_fastpath(struct __sk_buff *skb) {
    void *data = (void *)(unsigned long)skb->data;
    void *data_end = (void *)(unsigned long)skb->data_end;
    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end || eth->protocol != byte_swap_16(ETH_P_IP))
        return TC_ACT_OK;

    struct iphdr *ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end || (ip->ihl_version & 0x0f) != 5 ||
        ip->protocol != IPPROTO_UDP || (ip->fragment_offset & byte_swap_16(0x3fff)))
        return TC_ACT_OK;

    struct udphdr *udp = (void *)(ip + 1);
    if ((void *)(udp + 1) > data_end)
        return TC_ACT_OK;

    const __u32 original_src_ip = ip->source;
    const __u32 original_dst_ip = ip->destination;
    const __u16 original_src_port = udp->source;
    const __u16 original_dst_port = udp->destination;
    const __u16 original_total_length = ip->total_length;
    const __u8 original_tos = ip->tos;
    struct fastpath_flow_key key = {
        .ingress_ifindex = skb->ifindex,
        .local_ip = original_dst_ip,
        .remote_ip = original_src_ip,
        .local_port = original_dst_port,
        .remote_port = original_src_port,
        .protocol = IPPROTO_UDP,
    };
    struct fastpath_flow_action *action =
        bpf_map_lookup_elem(&RTP_FLOWS, &key);
    if (!action)
        return TC_ACT_OK;

    struct bpf_fib_lookup fib;
    __builtin_memset(&fib, 0, sizeof(fib));
    fib.family = AF_INET;
    fib.l4_protocol = IPPROTO_UDP;
    fib.sport = action->rewritten_src_port;
    fib.dport = action->rewritten_dst_port;
    fib.tot_len = byte_swap_16(original_total_length);
    fib.ifindex = skb->ifindex;
    fib.tos = original_tos;
    fib.ipv4_src = action->rewritten_src_ip;
    fib.ipv4_dst = action->rewritten_dst_ip;
    if (bpf_fib_lookup(skb, &fib, sizeof(fib), 0) !=
        BPF_FIB_LKUP_RET_SUCCESS) {
        __sync_fetch_and_add(&action->redirect_errors, 1);
        return TC_ACT_OK;
    }
    if (action->egress_ifindex && fib.ifindex != action->egress_ifindex) {
        __sync_fetch_and_add(&action->redirect_errors, 1);
        return TC_ACT_OK;
    }

    const __u32 ip_offset = sizeof(*eth);
    const __u32 udp_offset = ip_offset + sizeof(*ip);
    const __u64 l4_ip_flags = sizeof(__u32) | BPF_F_PSEUDO_HDR |
                              BPF_F_MARK_MANGLED_0;
    const __u64 l4_port_flags = sizeof(__u16) | BPF_F_MARK_MANGLED_0;

    if (bpf_l3_csum_replace(skb, ip_offset + 10, original_src_ip,
                            action->rewritten_src_ip, sizeof(__u32)) ||
        bpf_l3_csum_replace(skb, ip_offset + 10, original_dst_ip,
                            action->rewritten_dst_ip, sizeof(__u32)) ||
        bpf_l4_csum_replace(skb, udp_offset + 6, original_src_ip,
                            action->rewritten_src_ip, l4_ip_flags) ||
        bpf_l4_csum_replace(skb, udp_offset + 6, original_dst_ip,
                            action->rewritten_dst_ip, l4_ip_flags) ||
        bpf_l4_csum_replace(skb, udp_offset + 6, original_src_port,
                            action->rewritten_src_port, l4_port_flags) ||
        bpf_l4_csum_replace(skb, udp_offset + 6, original_dst_port,
                            action->rewritten_dst_port, l4_port_flags) ||
        bpf_skb_store_bytes(skb, ip_offset + 12,
                            &action->rewritten_src_ip, sizeof(__u32), 0) ||
        bpf_skb_store_bytes(skb, ip_offset + 16,
                            &action->rewritten_dst_ip, sizeof(__u32), 0) ||
        bpf_skb_store_bytes(skb, udp_offset,
                            &action->rewritten_src_port, sizeof(__u16), 0) ||
        bpf_skb_store_bytes(skb, udp_offset + 2,
                            &action->rewritten_dst_port, sizeof(__u16), 0) ||
        bpf_skb_store_bytes(skb, 0, fib.dmac, sizeof(fib.dmac), 0) ||
        bpf_skb_store_bytes(skb, 6, fib.smac, sizeof(fib.smac), 0)) {
        __sync_fetch_and_add(&action->redirect_errors, 1);
        return TC_ACT_OK;
    }

    __sync_fetch_and_add(&action->packets, 1);
    if (skb->len > sizeof(*eth) + sizeof(*ip) + sizeof(*udp))
        __sync_fetch_and_add(
            &action->bytes,
            skb->len - sizeof(*eth) - sizeof(*ip) - sizeof(*udp));
    return bpf_redirect(fib.ifindex, 0);
}

char LICENSE[] SEC("license") = "GPL";
