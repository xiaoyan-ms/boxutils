
from dataclasses import dataclass
import random
import threading
import time
import traceback
import click
import multiprocessing as mp

import requests

from boxutils.common.common import MetricsTracker, TokenProvider


@dataclass
class _ReqMsg:
    body: str


@dataclass
class _RspMsg:
    duration: float
    status: int
    error: str


def load_sample_file(filename: str) -> list[str]:
    with open(filename, 'r') as f:
        samples = [
            x.strip() for x in f.readlines() if x.strip() != ''
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
    while True:
        msg1 = q_req.get(block=True)
        headers = {
            "Authorization": f"Bearer {token.get()}",
        } 
        t1 = time.perf_counter()
        try:
            r = session.post(endpoint, data=msg1.body, headers=headers)
            t2 = time.perf_counter()
            msg2 = _RspMsg(
                duration=(t2-t1) * 1000,
                status=r.status_code,
                error='',
            )
            q_rsp.put(msg2)
        except Exception as e:
            t2 = time.perf_counter()
            msg2 = _RspMsg(
                duration=(t2-t1) * 1000,
                status=0,
                error=f'{e}',
            )
            q_rsp.put(msg2)



def result_collect_process(q_rsp: mp.Queue[_RspMsg]):
    tracker = MetricsTracker()
    while True:
        msg = q_rsp.get(block=True)
        tracker.post({f'code_{msg.status}': msg.duration})


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
    "--rps",
    type=int,
    required=True,
)
@click.option(
    "--sample-file",
    type=str,
    required=True,
)
def main(
    endpoint: str,
    token_scope: str,
    sp_object_id: str,
    rps: int,
    sample_file: str,
):
    samples = load_sample_file(sample_file)
    q_req = mp.Queue()
    q_rsp = mp.Queue()
    token = TokenProvider(sp_object_id, token_scope)
    token.get()

    for _ in range(mp.cpu_count() + 1):
        p = mp.Process(target=request_handler_process, args=(q_req, q_rsp, endpoint, token), daemon=True)
        p.start()

    t = threading.Thread(target=result_collect_process, args=(q_rsp,), daemon=True)
    t.start()

    dur = 1.0 / rps
    while True:
        text = random.choice(samples)
        q_req.put(_ReqMsg(body=text))
        time.sleep(dur)


if __name__ == "__main__":
    main()
