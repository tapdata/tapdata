import requests


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
