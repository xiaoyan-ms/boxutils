from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
import csv
from dataclasses import dataclass
from datetime import datetime
import json
import math
import random
import sys
import threading
import time
import traceback
from typing import Any, Optional
import click
import multiprocessing as mp
import numpy as np
import requests
import hashlib
import uuid
from boxutils.common.common import MetricsTracker, TokenProvider


class HttpFailException(Exception):
    def __init__(self, code: int, msg: str):
        self.code = code
        self.msg = msg

    def __str__(self):
        return f'http failed, status code: {self.code}, response message: {self.msg}'


@dataclass
class _ReqMsg:
    reqid: str
    body: Any


@dataclass
class _RspMsg:
    duration: float
    status: int
    error: Optional[Exception]


def load_sample_file(filename: str) -> list[tuple[int, Any]]:
    fileid = int(hashlib.md5(filename.encode()).hexdigest(), 16)
    fileid = fileid % 2**32 * 2**48
    with open(filename, 'r') as f:
        samples = [
            (fileid+i+1, json.loads(x)) for i, x in enumerate(f.readlines()) if x.strip() != ''
        ]
        if len(samples) == 0:
            raise Exception('Sample count is 0')
        return samples


def request_handler_process(
    q_req: mp.Queue[_ReqMsg],
    q_rsp: mp.Queue[_RspMsg],
    endpoint: str,
    token: TokenProvider,
):
    session = requests.Session()
    pool = ThreadPoolExecutor(max_workers=64)
    try:
        while True:
            msg = q_req.get(block=True)
            pool.submit(inference, session, endpoint, token, msg.body, msg.reqid, q_rsp)
    except:
        traceback.print_exc()
        sys.exit(1)


def inference(session: requests.Session, endpoint: str, token: TokenProvider, body: Any, reqid: str, q_rsp: mp.Queue[_RspMsg]):
    try:
        headers = {
            "Authorization": f"Bearer {token.get()}",
            "x-ms-client-request-id": reqid,
        } 
        t1 = time.perf_counter()
        r = session.post(endpoint, json=body, headers=headers, timeout=10)
        len(r.text)
        t2 = time.perf_counter()
        
        if r.ok:
            msg = _RspMsg(
                duration=(t2-t1) * 1000,
                status=r.status_code,
                error=None,
            )
            q_rsp.put(msg)
        else:
            msg = _RspMsg(
                duration=(t2-t1) * 1000,
                status=r.status_code,
                error=HttpFailException(r.status_code, r.text)
            )
            q_rsp.put(msg)

    except Exception as e:
        t2 = time.perf_counter()
        msg = _RspMsg(
            duration=(t2-t1) * 1000,
            status=-1,
            error=e,
        )
        q_rsp.put(msg)


def result_collect_process(q_rsp: mp.Queue[_RspMsg], ):
    tracker = MetricsTracker()
    while True:
        msg = q_rsp.get(block=True)
        tracker.post({f'code_{msg.status}': msg.duration})



class ResponseCollecter:
    def __init__(self, q_rsp: mp.Queue[_RspMsg], output_file: str):
        self.q_rsp = q_rsp
        self.output_file = output_file
        self.output_io = open(output_file, "w", newline="")
        self.output_csv = csv.DictWriter(self.output_io, [
            "target_rps", "actual_rps", "req_count_total", "error_percent",  "p50_latency_ms", "p90_latency_ms", "p95_latency_ms", "p99_latency_ms", "start_time", "end_time"
            ])
        self.output_csv.writeheader()
        self.output_io.flush()
        self.last_flush_ts = time.time()
        self.last_print_sec = time.time()
        self.buff: list[_RspMsg] = []
        self.thread = threading.Thread(target=self.process, daemon=True)
        self.thread.start()
    
    def process(self):
        try:
            while True:
                msg = self.q_rsp.get(block=True)
                self.buff.append(msg)

                # error printing, with 1 for each second
                if not (200 <= msg.status < 300):
                    ts = time.time()
                    if ts - self.last_print_sec >= 1:
                        self.last_print_sec = ts
                        print(msg.error, file=sys.stderr)

        except:
            traceback.print_exc()
            sys.exit(1)
    

    def flush(self, target_rps: int) -> dict:
        prev_buff = self.buff
        self.buff = []
        prev_last_flush_ts = self.last_flush_ts
        self.last_flush_ts = time.time()
        
        req_count_total = len(prev_buff)
        req_count_error = len([x for x in prev_buff if not(200 <= x.status < 300)])
        error_percent = 100.0 * req_count_error / req_count_total
        actual_rps = req_count_total / max(self.last_flush_ts - prev_last_flush_ts, 1)
        start_time = datetime.fromtimestamp(prev_last_flush_ts)
        end_time = datetime.fromtimestamp(self.last_flush_ts)

        narr = np.array([x.duration for x in prev_buff])
        p50=float(np.percentile(narr, 50))
        p90=float(np.percentile(narr, 90))
        p95=float(np.percentile(narr, 95))
        p99=float(np.percentile(narr, 99))

        d = {
            "target_rps": target_rps, 
            "actual_rps": actual_rps, 
            "req_count_total": req_count_total, 
            "error_percent": error_percent, 
            "p50_latency_ms": p50, 
            "p90_latency_ms": p90, 
            "p95_latency_ms": p95, 
            "p99_latency_ms": p99, 
            "start_time": start_time, 
            "end_time": end_time,
        }
        self.output_csv.writerow(d)
        self.output_io.flush()
        return d


@click.command()
@click.option(
    "--endpoint",
    type=str,
    required=True,
)
@click.option(
    "--token-scope",
    type=str,
    required=False,
    default='https://ml.azure.com',
)
@click.option(
    "--sp-object-id",
    type=str,
    required=False,
    default='14fa3ef0-e086-4529-acc1-4865df4b727b',
)
@click.option(
    "--rps-step",
    type=int,
    required=True,
)
@click.option(
    "--rps-min",
    type=int,
    required=False,
    default=1,
)
@click.option(
    "--rps-max",
    type=int,
    required=False,
    default=-1,
)
@click.option(
    "--step-seconds",
    type=int,
    required=False,
    default=60,
)
@click.option(
    "--max-error-rate",
    type=float,
    required=False,
    default=10,
)
@click.option(
    "--sample-file",
    type=str,
    required=True,
)
@click.option(
    "--output-file",
    type=str,
    required=False,
    default='output.csv',
)
def main(
    endpoint: str,
    token_scope: str,
    sp_object_id: str,
    rps_step: int,
    rps_min: int,
    rps_max: int,
    step_seconds: int,
    max_error_rate: float,
    sample_file: str,
    output_file: str,
):
    samples = load_sample_file(sample_file)
    q_req = mp.Queue()
    q_rsp = mp.Queue()
    token = TokenProvider(sp_object_id, token_scope)
    token.get()

    for _ in range(mp.cpu_count() + 1):
        p = mp.Process(target=request_handler_process, args=(q_req, q_rsp, endpoint, token), daemon=True)
        p.start()

    collecter = ResponseCollecter(q_rsp, output_file)
    rps = rps_min
    while True:
        # send
        dur = 1.0 / max(rps, 1)
        cnt = math.ceil(step_seconds / dur)
        tcur = time.perf_counter()
        for _ in range(cnt):
            subid, body = random.choice(samples)
            reqid = str(uuid.UUID(int=int(uuid.uuid4()) // 2**80 *2**80 + subid))
            q_req.put(_ReqMsg(body=body, reqid=reqid))
            tcur += dur
            time.sleep(max(tcur - time.perf_counter(), 0))

        # collect
        d = collecter.flush(rps)
        print('----------------------------------------')
        print(f'start_time: {d["start_time"]}')
        print(f'end_time: {d["end_time"]}')
        print(f'target_rps: {d["target_rps"]}')
        print(f'actual_rps: {d["actual_rps"]}')
        print(f'req_count_total: {d["req_count_total"]}')
        print(f'error_percent: {d["error_percent"]}')
        print(f'p50_latency_ms: {d["p50_latency_ms"]}')
        print(f'p90_latency_ms: {d["p90_latency_ms"]}')
        print(f'p95_latency_ms: {d["p95_latency_ms"]}')
        print(f'p99_latency_ms: {d["p99_latency_ms"]}')
        print('----------------------------------------')
    
        # end
        error_percent = d["error_percent"]
        if error_percent > max_error_rate:
            print(f'Finish load testing, error percentage hit {error_percent} %')
            return
        
        if rps_max > 0 and rps > rps_max:
            print(f'Finish load testing, rps hit max {rps_max}')
            return

        # iterate
        rps += rps_step


if __name__ == "__main__":
    main()
