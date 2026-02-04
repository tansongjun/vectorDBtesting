import json
from volcengine.auth.SignerV4 import SignerV4
from volcengine.base.Request import Request
from volcengine.Credentials import Credentials

def prepare_request(method, path, ak, sk, host, region, service, params=None, data=None):
    r = Request()
    r.set_shema("https")
    r.set_method(method)
    r.set_host(host)
    r.set_path(path)

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Host": host,
    }
    r.set_headers(headers)

    if params:
        r.set_query(params)

    if data is not None:
        # sign the exact bytes you send
        r.set_body(json.dumps(data, separators=(",", ":"), ensure_ascii=False))

    # ✅ V2 data plane signing scope
    credentials = Credentials(ak, sk, service, region)
    SignerV4.sign(r, credentials)
    return r
