import requests
from tapdata_cli.log import logger


SERVER = str
URL = str


class RequestSession(requests.Session):

    def __init__(self, server: SERVER):
        self.base_url = f"http://{server}/api"
        self.params = {}
        super(RequestSession, self).__init__()

    def prepare_request(self, request: requests.Request) -> requests.PreparedRequest:
        request.url = self.base_url + request.url
        return super(RequestSession, self).prepare_request(request)


req = None


def set_req(server):
    global req
    req = RequestSession(server)
    return req


class Api:

    def response_json(self, res: requests.Response):
        if res.status_code < 200 or res.status_code >= 300:
            logger.warn("{}", res.text)
            logger.warn("request failed url: {}", res.url)
            return False
        else:
            return res.json()

    def get(self, id, **kwargs):
        res = req.get(self.url + f"/{id}", **kwargs)
        data = self.response_json(res)
        return data

    def put(self, id, **kwargs):
        res = req.put(self.url + f"/{id}", **kwargs)
        data = self.response_json(res)
        return data

    def post(self, data: dict, url_after=None, **kwargs):
        if url_after is not None:
            url = self.url + url_after
        else:
            url = self.url
        res = req.post(url, json=data, **kwargs)
        data = self.response_json(res)
        return data

    def delete(self, id, data):
        res = req.delete(self.url + f"/{id}", json=data)
        data = self.response_json(res)
        return data

    def list(self, **kwargs):
        res = req.get(self.url, **kwargs)
        data = self.response_json(res)
        return data

    def patch(self, url_after=None, **kwargs):
        if url_after is not None:
            url = self.url + url_after
        else:
            url = self.url
        res = req.patch(url, **kwargs)
        data = self.response_json(res)
        return data


class DataSourceApi(Api):

    url = "/Connections"


class MetadataInstancesApi(Api):

    url = "/MetadataInstances"


class DataFlowsApi(Api):

    url = "/DataFlows"


class TaskApi(Api):

    url = "/Task"


class MeasurementApi(Api):

    url = "/measurement"


class InspectApi(Api):

    url = "/task/auto-inspect-totals"
