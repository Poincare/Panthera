cached_file = open('meta_cache_latency', 'r')
non_cached_file = open('non_meta_cache_latency', 'r')

def get_latencies(lines):
    res = []
    for l in lines:
        try:
            res.append(int(l.rstrip()))
        except ValueError:
            pass
    return res

cached_latencies = get_latencies(cached_file.readlines())
non_cached_latencies = get_latencies(non_cached_file.readlines())

res_file = open('meta_multinode_latencies.csv', 'w')
res_file.write("cached, noncached\n")
for i in range(0, len(cached_latencies)):
    try:
        res_file.write(str(cached_latencies[i]) + "," + str(non_cached_latencies[i]) + "\n")
    except IndexError:
        pass

