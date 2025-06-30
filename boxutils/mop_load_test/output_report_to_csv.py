import csv
import json
import sys


def main():
    jsonfile = sys.argv[1]
    csvfile = sys.argv[2]

    headers1 = [
        "target_rps",
    ]
    headers2 = [
        "average_latency_ms",
        "actual_rps",
        "req_count_total",
        "req_count_2xx",
        "req_count_4xx",
        "req_count_5xx",
        "p50_latency_ms",
        "p90_latency_ms",
        "p95_latency_ms",
        "p99_latency_ms",
        "server_average_latency_ms",
        "server_p50_latency_ms",
        "server_p90_latency_ms",
        "server_p95_latency_ms",
        "server_p99_latency_ms",
        "core_hour_per_million_req",
        "gpu_count_avg",
        "gpu_util_avg",
        "gpu_mem_util_avg",
        "max_gpu_util",
        "max_gpu_memory_util",
        "cpu_count_avg",
        "logical_cpu_count_avg",
        "cpu_util_avg",
        "max_cpu_util",
        "virtual_memory_used_avg",
        "start_time",
        "end_time",
    ]

    rd = json.loads(open(jsonfile, 'r'))
    with open(csvfile, "w", newline="") as f:
        w = csv.DictWriter(f, headers1 + headers2)
        w.writeheader()

        for d0 in rd:
            d1 = {
                **{h: d0[h] for h in headers1},
                **{h: d0["metrics"][h] for h in headers2}
            }
            w.writerow(d1)

    print("Success!")


if __name__ == '__main__':
    main()