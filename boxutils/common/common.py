import numpy as np
from dataclasses import dataclass
from datetime import datetime
import time
import requests



@dataclass
class _TokenObj:
    access_token: str
    expires_on: int


class TokenProvider:
    def __init__(self, object_id: str, scope: str):
        self._object_id = object_id
        self._scope = scope
        self._cache: dict[str, _TokenObj] = {}

    def get(self) -> str:
        key = f"{self._object_id}|{self._scope}"
        val = self._cache.get(key)
        if val != None and val.expires_on > time.time() + 60:
            return val.access_token
        
        url = f'http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource={self._scope}&object_id={self._object_id}'
        r = requests.get(url, headers={'Metadata': 'true'})
        assert r.ok
        t = r.json()
        ob = _TokenObj(
            access_token=t['access_token'],
            expires_on=int(t['expires_on']),
        )
        self._cache[key] = ob
        return ob.access_token



@dataclass
class MetricsObj:
    dt: datetime
    name: str
    count: int
    avg: float
    p50: float
    p90: float
    p95: float
    p99: float
    p999: float


class MetricsTracker:
    def __init__(self):
        self._q: list[dict[str, float]] = []
        self._tcycle: int = 0


    def post(self, val: dict[str, float]):
        cycle = int(time.time() / 60)
        if self._tcycle == cycle:
            self._q.append(val)
        else:
            d = self._q
            self._q = [val]
            self._tcycle = cycle
            if d:
                self.flush(d)


    def flush(self, d: list[dict[str, float]]):
        kmap: dict[str, list[float]] = {}
        for kv in d:
            for k, v in kv.items():
                if k in kmap:
                    kmap[k].append(v)
                else:
                    kmap[k] = [v]

        dt = datetime.now()
        for k, arr in kmap.items():
            narr = np.array(arr)
            mobj = MetricsObj(
                dt=dt,
                name=k,
                count=narr.size,
                avg=float(np.average(narr)),
                p50=float(np.percentile(narr, 50)),
                p90=float(np.percentile(narr, 90)),
                p95=float(np.percentile(narr, 95)),
                p99=float(np.percentile(narr, 99)),
                p999=float(np.percentile(narr, 99.9)),
            )
            self.recieve(mobj)


    def recieve(self, m: MetricsObj):
        print(f'[{m.dt}] {m.name}, count: {m.count}, avg: {m.avg}, p50: {m.p50}, p90: {m.p90}, p95: {m.p95}, p99: {m.p99}, p999: {m.p999}')

    

