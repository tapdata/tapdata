from typing import Iterator
import base64
import argparse
import git, psutil
import os
import json
from datetime import datetime

import requests

MAX_LENGTH = 30
CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
ROOT_PATH = "/".join([CURRENT_PATH, ".."])
OWNER = 'tapdata'
REPO = 'tapdata-enterprise'

def time_readable(seconds):
    seconds = int(seconds)
    if seconds < 60:
        return "{} seconds".format(seconds)
    minutes = round(seconds / 60, 1)
    if minutes < 60:
        return "{} minutes".format(minutes)
    hours = round(minutes / 60, 1)
    if hours <= 48:
        return "{} hours".format(hours)
    days = round(hours / 24, 1)
    return "{} days".format(days)


class GithubActionApi:

    def __init__(self, token: str, owner: str, repo: str, job: str):
        self.base_url = "https://api.github.com"
        self.headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github+json"}
        self.owner = owner
        self.repo = repo
        self.job = job

    def _get_jobs(self):
        response = requests.get(f"{self.base_url}/repos/{self.owner}/{self.repo}/actions/runs/{self.job}/jobs",
                                headers=self.headers)
        if response.status_code == 200:
            resp = []
            for j in response.json().get('jobs'):
                for s in j.get('steps'):
                    resp.append((j["name"], s))
            return resp
        else:
            raise Exception(
                f"Failed to get jobs for {self.owner}/{self.repo}, " +
                f"response test is: {response.text}, " +
                f"request url is: {response.request.url}"
            )

    @property
    def failed_steps(self):
        steps = self._get_jobs()
        failed_steps = []
        for step in steps:
            conclusion = step[1].get("conclusion")
            name = step[1].get("name")
            if conclusion == "failure":
                failed_steps.append(f"{name}")
        return failed_steps


class Git:

    def __init__(self, dir_path):
        self.repo = git.Repo(dir_path, search_parent_directories=True)
        self.commit = self.repo.head.object

    @property
    def commit_time(self):
        return datetime.fromtimestamp(self.commit.committed_date).strftime("%Y-%m-%d %H:%M:%S")

    @property
    def commit_message(self):
        return self.commit.message.replace('\n', '')

    @property
    def commit_author(self):
        return self.commit.author.name

    @property
    def commit_author_email(self):
        return self.commit.author.email


class Args:

    def __init__(self):
        self.git_obj = Git(ROOT_PATH)
        parse = argparse.ArgumentParser(description="send error info to feishu.")
        parse.add_argument('--addr', dest="addr", required=False, type=str, help="test env addr", default=None)
        parse.add_argument('--branch', dest="branch", required=False, type=str, help="github branch", default=None)
        parse.add_argument('--message_type', dest="message_type", required=False, type=str, help="feishu message type", default="build_fail_notice")
        parse.add_argument('--message', dest="message", required=False, type=str, help="feishu message", default="{}")
        parse.add_argument("--runner", dest="runner", required=True, type=str, help="github action runner name")
        parse.add_argument("--detail_url", dest="detail_url", required=True, type=str, help="detail url")
        parse.add_argument("--token", dest="token", required=True, type=str, help="github personal token")
        parse.add_argument("--job_id", dest="job_id", required=True, type=str, help="github action job id")
        parse.add_argument("--person_in_charge", dest="person_in_charge", required=False, type=str, help="person_in_charge of module", default="eyJwZW9wbGVfbGlzdCI6IFt7ImVtYWlsIjogImVyaW5AdGFwZGF0YS5pbyIsICJnaXRodWJfbmFtZSI6ICIifSwgeyJlbWFpbCI6ICJqZXJyeUB0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogImRyZWFtY29pbjE5OTgifSwgeyJlbWFpbCI6ICJiZWliZWlAdGFwZGF0YS5pbyIsICJnaXRodWJfbmFtZSI6ICJ4YnN1cmEifSwgeyJlbWFpbCI6ICJsZW9uQHRhcGRhdGEuaW8iLCAiZ2l0aHViX25hbWUiOiAib3BlbmxnIn0sIHsiZW1haWwiOiAicmlja0B0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogIndhZWlsdWEifSwgeyJlbWFpbCI6ICJzdGV2ZW5AdGFwZGF0YS5pbyIsICJnaXRodWJfbmFtZSI6ICIzMjA3Mzk1NSJ9LCB7ImVtYWlsIjogInplZEB0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogInplZDEyMDEifSwgeyJlbWFpbCI6ICJqYWNxdWVzQHRhcGRhdGEuaW8iLCAiZ2l0aHViX25hbWUiOiAiaml1eWV0eCJ9LCB7ImVtYWlsIjogImZhbm5pZUB0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogIkZhbm5pZUdpcmwifSwgeyJlbWFpbCI6ICJsaWFuZ3poaWthaUB0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogIlR3b1BlbnMifSwgeyJlbWFpbCI6ICJwYXluZUB0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogIkdvc3RKUiJ9LCB7ImVtYWlsIjogInh1ZmVpQHRhcGRhdGEuaW8iLCAiZ2l0aHViX25hbWUiOiAiY24teHVmZWkifSwgeyJlbWFpbCI6ICJ6ZXJvaHl1YW5AdGFwZGF0YS5pbyIsICJnaXRodWJfbmFtZSI6ICJ6ZXJvaHl1YW4ifSwgeyJlbWFpbCI6ICJsZW1vbkB0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogIm5pbmdtZW5nNzc3In0sIHsiZW1haWwiOiAiamFja2luQHRhcGRhdGEuaW8iLCAiZ2l0aHViX25hbWUiOiAiamFja2luLWNvZGUifSwgeyJlbWFpbCI6ICJkYXl1bkB0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogImRhbmllbDIwMDkifSwgeyJlbWFpbCI6ICJkZXJpbkB0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogIkRlcmluQ2hlbiJ9LCB7ImVtYWlsIjogImhhcnNlbkB0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogIkhhcnNlbkxpbiJ9LCB7ImVtYWlsIjogInNhbUB0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogInBseTAwMSJ9LCB7ImVtYWlsIjogImRleHRlckB0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogImR4dHJ6aGFuZyJ9LCB7ImVtYWlsIjogImFwbG9tYkB0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogImRvYnlicm9zIn0sIHsiZW1haWwiOiAiamFyYWRAdGFwZGF0YS5pbyIsICJnaXRodWJfbmFtZSI6ICJqYXJhZDA2MjgifSwgeyJlbWFpbCI6ICJnYXZpbkB0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogIiJ9LCB7ImVtYWlsIjogImxpbHlAdGFwZGF0YS5pbyIsICJnaXRodWJfbmFtZSI6ICIifSwgeyJlbWFpbCI6ICJtYXJrQHRhcGRhdGEuaW8iLCAiZ2l0aHViX25hbWUiOiAibWF6aHVhbmcxMTEifSwgeyJlbWFpbCI6ICJkYXZpZHh1QHRhcGRhdGEuaW8iLCAiZ2l0aHViX25hbWUiOiAibWF0dGhld3h1ZGEifSwgeyJlbWFpbCI6ICJqYXNvbkB0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogIiJ9LCB7ImVtYWlsIjogInJvYm90QHRhcGRhdGEuaW8iLCAiZ2l0aHViX25hbWUiOiAienNqbHUifV0sICJwZW9wbGVfaW5fY2hhcmdlIjogeyJidWlsZCBmcm9udGVuZCI6IFt7ImVtYWlsIjogInh1ZmVpQHRhcGRhdGEuaW8iLCAiZ2l0aHViX25hbWUiOiAiY24teHVmZWkifSwgeyJlbWFpbCI6ICJwYXluZUB0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogIkdvc3RKUiJ9XSwgImNvbXBpbGUgcGx1Z2luLWtpdCBtb2R1bGUiOiBbeyJlbWFpbCI6ICJqYXJhZEB0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogImphcmFkMDYyOCJ9LCB7ImVtYWlsIjogImRlcmluQHRhcGRhdGEuaW8iLCAiZ2l0aHViX25hbWUiOiAiRGVyaW5DaGVuIn1dLCAiY29tcGlsZSBvcGVuc291cmNlIG1hbmFnZXIiOiBbeyJlbWFpbCI6ICJ6ZWRAdGFwZGF0YS5pbyIsICJnaXRodWJfbmFtZSI6ICJ6ZWQxMjAxIn0sIHsiZW1haWwiOiAiemVyb2h5dWFuQHRhcGRhdGEuaW8iLCAiZ2l0aHViX25hbWUiOiAiemVyb2h5dWFuIn1dLCAiY29tcGlsZSBjb25uZWN0b3JzLWNvbW1vbiBtb2R1bGUiOiBbeyJlbWFpbCI6ICJqYXJhZEB0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogImphcmFkMDYyOCJ9XSwgImNvbXBpbGUgbWFuYWdlciBtb2R1bGUgaWYgYnJhbmNoIGlzIHJlbGVhc2UiOiBbeyJlbWFpbCI6ICJ6ZWRAdGFwZGF0YS5pbyIsICJnaXRodWJfbmFtZSI6ICJ6ZWQxMjAxIn0sIHsiZW1haWwiOiAiemVyb2h5dWFuQHRhcGRhdGEuaW8iLCAiZ2l0aHViX25hbWUiOiAiemVyb2h5dWFuIn1dLCAiY29tcGlsZSBtYW5hZ2VyIG1vZHVsZSBpZiBicmFuY2ggaXMgbm90IHJlbGVhc2UiOiBbeyJlbWFpbCI6ICJ6ZWRAdGFwZGF0YS5pbyIsICJnaXRodWJfbmFtZSI6ICJ6ZWQxMjAxIn0sIHsiZW1haWwiOiAiemVyb2h5dWFuQHRhcGRhdGEuaW8iLCAiZ2l0aHViX25hbWUiOiAiemVyb2h5dWFuIn1dLCAiY29tcGlsZSBpZW5naW5lIG1vZHVsZXMiOiBbeyJlbWFpbCI6ICJqYWNraW5AdGFwZGF0YS5pbyIsICJnaXRodWJfbmFtZSI6ICJqYWNraW4tY29kZSJ9XSwgImNvbXBpbGUgY29ubmVjdG9ycyBtb2R1bGUiOiBbeyJlbWFpbCI6ICJqYXJhZEB0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogImphcmFkMDYyOCJ9XSwgImNvbXBpbGUgdGFwZGF0YS1hZ2VudCBtb2R1bGUiOiBbeyJlbWFpbCI6ICJzdGV2ZW5AdGFwZGF0YS5pbyIsICJnaXRodWJfbmFtZSI6ICIzMjA3Mzk1NSJ9XSwgImNvbXBpbGUgYXBpc2VydmVyIG1vZHVsZSI6IFt7ImVtYWlsIjogInN0ZXZlbkB0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogIjMyMDczOTU1In1dLCAib3RoZXIiOiBbeyJlbWFpbCI6ICJqZXJyeUB0YXBkYXRhLmlvIiwgImdpdGh1Yl9uYW1lIjogImRyZWFtY29pbjE5OTgifV19fQ==")
        parse.add_argument("--app_id", dest="app_id", required=True, type=str, help="app id of feishu bot")
        parse.add_argument("--app_secret", dest="app_secret", required=True, type=str, help="app secret of feishu bot")
        parse.add_argument("--chat_id", dest="chat_id", required=True, type=str, help="chat id of feishu group")
        parse.add_argument("--commit_author", dest="commit_author", required=False, type=str, help="trigger author", default="")
        parse.add_argument("--commit_message", dest="commit_message", required=False, type=str, help="trigger commit", default="")
        parse.add_argument("--commit_time", dest="commit_time", required=False, type=str, help="trigger commit time", default="")
        parse.add_argument("--commit_author_email", dest="commit_author_email", required=False, type=str, help="trigger author email", default="")
        self.args = parse.parse_args()
        self.github_api = GithubActionApi(self.args.token, OWNER, REPO, self.args.job_id)

    @property
    def app_secret(self):
        return self.args.app_secret

    @property
    def app_id(self):
        return self.args.app_id

    @property
    def person_in_charge(self):
        return self.args.person_in_charge

    @property
    def branch(self):
        return self.args.branch

    @property
    def addr(self):
        return self.args.addr

    @property
    def runner(self):
        return self.args.runner

    @property
    def commit_time(self):
        return self.git_obj.commit_time if len(self.args.commit_time) == 0 else self.args.commit_time

    @property
    def commit_message(self):
        return self.git_obj.commit_message if len(self.args.commit_message) == 0 else self.args.commit_message

    @property
    def commit_author_email(self):
        return self.git_obj.commit_author_email if len(self.args.commit_author_email) == 0 else self.args.commit_author_email

    @property
    def commit_author(self):
        return self.git_obj.commit_author if len(self.args.commit_author) == 0 else self.args.commit_author

    @property
    def modules(self):
        return self.github_api.failed_steps

    @property
    def error_message(self):
        err_msg = self.args.error_message
        if self.args.error_message > MAX_LENGTH:
            err_msg = self.args.error_message[27:] + "..."
        return err_msg

    @property
    def detail_url(self):
        return self.args.detail_url

    @property
    def person_in_charge(self):
        return self.args.person_in_charge

    @property
    def chat_id(self):
        return self.args.chat_id

    @property
    def message_type(self):
        return self.args.message_type

    @property
    def message(self):
        return json.loads(self.args.message)


class Feishu:

    def __init__(self, app_id: str, app_secret: str):
        self.base_url = "https://open.feishu.cn/open-apis"
        self.headers = {"content-type": "application/json; charset=utf-8"}
        self.app_id = app_id
        self.app_secret = app_secret
        self._get_tenant_access_token()

    def _get_tenant_access_token(self):
        url = self.base_url + "/auth/v3/tenant_access_token/internal"
        data = {
            "app_secret": self.app_secret,
            "app_id": self.app_id
        }
        response = requests.post(url, headers=self.headers, json=data)
        if response.json()['code'] == 0:
            token = response.json()['tenant_access_token']
            self.headers.update({
                "Authorization": f"Bearer {token}"
            })
        else:
            raise Exception(response.text)


class FeishuMessage(Feishu):
    """
    @describe: A bot to send message to feishu
    @author: Jerry
    @file: feishu_notice.py
    @version:
    @time: 2022/07/29
    """

    def __init__(self,
                 title: str,
                 content: Iterator[Iterator],
                 chat_id: str,
                 app_secret: str,
                 app_id: str,
                 title_color: str = "red",
                 ) -> None:
        """
        @param title: message title
        @param title_color: title color if FeishuMessage.send_card method is call
        @param chat_id: feishu chat id
        @param app_id: app_id
        @param app_secret: app_secret
        @param content:
            message content data struct,
            example:
                [[{"tag": "text", "text": "项目有更新: "}, {"tag": "a", "text": "请查看", "href": "https://xxx.com/"}]]
        """
        self.title = title
        self.content = content
        self.title_color = title_color
        self.chat_id = chat_id
        super().__init__(app_id, app_secret)

    def _make_request_body(self, msg_content: dict):
        body = {
            "msg_type": "post",
            "receive_id": self.chat_id,
        }
        body.update(msg_content)
        content = body.get("content")
        content = json.dumps(content)
        body["content"] = content
        return body

    def _make_send_message_request_body(self):
        body = {
            "content": {
                "post": {
                    "zh_cn": {
                        "title": self.title,
                        "content": self.content
                    }
                }
            }
        }
        return self._make_request_body(body)

    def _make_send_card_request_body(self):
        body = {
            "msg_type": "interactive",
            "content": {
                "config": {
                    "wide_screen_mode": True,
                    "enable_forward": True
                },
                "header": {
                    "template": self.title_color,
                    "title": {
                        "content": self.title,
                        "tag": "plain_text"
                    }
                },
                "elements": self.content
            }
        }
        return self._make_request_body(body)

    def _request(self, data):
        params = {"receive_id_type": "user_id"}
        if self.chat_id.startswith("oc_"):
            params = {"receive_id_type": "chat_id"}
        res = requests.post(self.base_url + "/im/v1/messages", json=data, headers=self.headers, params=params)
        print(res.text)
        if res.json().get("code") == 0:
            print("send message success.")
        else:
            print(
                f"send message failed, "
                f"request body is: {json.dumps(json.loads(res.request.body), indent=4)}",
                f"response body is: {json.dumps(json.loads(res.json()))}"
            )

    def send_message(self):
        data = self._make_send_message_request_body()
        self._request(data)

    def send_card(self):
        data = self._make_send_card_request_body()
        self._request(data)


class FeishuPersonInCharge(Feishu):
    """
    format person in charge
    """

    def __init__(self, app_id: str, app_secret: str, charge_b64: str):
        self.charge_map = json.loads(base64.b64decode(charge_b64))
        super().__init__(app_id, app_secret)

    def get_open_id(self, emails: list) -> Iterator[str]:
        url = self.base_url + "/contact/v3/users/batch_get_id"
        params = {
            "user_id_type": "open_id"
        }
        data = {
            "emails": emails,
        }
        openid_list = []
        response = requests.post(url, headers=self.headers, params=params, json=data)
        if response.json()['code'] == 0:
            for user_info in response.json()['data']["user_list"]:
                openid = user_info.get("user_id") if user_info.get("user_id") else user_info.get("email").split("@")[0]
                openid_list.append(openid)
        else:
            raise Exception(
                f"Failed to get open id, response text is: {response.text}, request body: {response.request.body}")
        return openid_list

    @staticmethod
    def _get_email_from_charge_map(charge_map: list):
        emails = []
        for user in charge_map:
            if user.get("email") is not None:
                emails.append(user.get("email"))
        return emails

    def get_people_in_charge(self, module: str) -> Iterator[str]:
        charge = self.charge_map["people_in_charge"].get(module)
        if charge is None:
            people_in_charge = self.charge_map["people_in_charge"]["other"]
        else:
            people_in_charge = charge
        emails = self._get_email_from_charge_map(people_in_charge)
        return self.get_open_id(emails)

    def _search_name(self, name: str, field: list) -> bool:
        if name.lower() in field.lower():
            return True
        return False

    def get_by_github_name(self, name):
        for user in self.charge_map["people_list"]:
            if self._search_name(name, user["github_name"]) or self._search_name(name, user["email"]):
                return self.get_open_id([user["email"]])
        return None


class Card:

    def __init__(self, args_obj: Args, ):
        self.args_obj = args_obj
        self.person_in_charge = FeishuPersonInCharge(
            app_id=self.args_obj.app_id,
            app_secret=self.args_obj.app_secret,
            charge_b64=self.args_obj.person_in_charge
        )

    def _get_at(self, module):
        open_id_list = self.person_in_charge.get_people_in_charge(module)
        at_string = ""
        for open_id in open_id_list:
            if open_id.startswith("ou_") and "@" not in open_id:
                at_string += f"<at id={open_id}></at>"
            else:
                at_string += f"@{open_id}"
        return at_string

    def _get_at_name(self):
        openid = self.person_in_charge.get_by_github_name(self.args_obj.commit_author)
        if openid is None:
            return f"@berry"
            raise Exception(f"Can't find {self.args_obj.commit_author} in FEISHU_NOTICE_LIST, please update")
        openid = openid[0]
        if openid.startswith("ou_") and "@" not in openid:
            at_string = f"<at id={openid}></at>"
        else:
            at_string = f"@{openid}"
        return at_string

    def _format_test_fields(self):
        f = {
            "is_short": False,
            "text": {
                "content": "",
                "tag": "lark_md",
            }
        }
        message = self.args_obj.message
        fields = []
        fields += [{
            "is_short": True,
            "text": {
                "content": "**测试环境:**\nGithub CI 环境",
                "tag": "lark_md"
            }
        }, {
            "is_short": True,
            "text": {
                "content": "**机器硬件配置:**\ncpu: {}, memory: {}GB".format(psutil.cpu_count(logical=False), round(psutil.virtual_memory().total/1024/1024/1024, 1)),
                "tag": "lark_md"
            }
        }]
        fields.append(f)
        fields += [{
            "is_short": True,
            "text": {
                "content": f"**分支名称**: {self.args_obj.branch}",
                "tag": "lark_md"
            }
        }, {
            "is_short": True,
            "text": {
                "content": f"**构建任务**: {self.args_obj.runner}",
                "tag": "lark_md"
            }
        }]
        fields.append(f)
        fields += [
            {
                "is_short": True,
                "text": {
                    "content": "\n**编译结果**: {}".format(message.get("build_result")),
                    "tag": "lark_md"
                }
            },
        ]
        fields += [
            {
                "is_short": True,
                "text": {
                    "content": "**环境启动结果**: {}".format(message.get("start_result")),
                    "tag": "lark_md"
                }
            },
        ]
        fields.append(f)
        fields += [
            {
                "is_short": True,
                "text": {
                    "content": "**单测情况总览**:\n总数: {}, 通过: {}".format(message.get("ut_sum"), message.get("ut_pass")),
                    "tag": "lark_md"
                }
            },
        ]
        fields += [
            {
                "is_short": True,
                "text": {
                    "content": "**单测耗时**:\n{}".format(time_readable(message.get("ut_cost_time"))),
                    "tag": "lark_md"
                }
            },
        ]
        fields.append(f)
        fields += [
            {
                "is_short": True,
                "text": {
                    "content": "**集成测试情况总览**:\n总数: {}, 通过: {}".format(message.get("it_sum"), message.get("it_pass")),
                    "tag": "lark_md"
                }
            },
        ]
        fields += [
            {
                "is_short": True,
                "text": {
                    "content": "**集成测试耗时**:\n{}".format(time_readable(message.get("it_cost_time"))),
                    "tag": "lark_md"
                }
            },
        ]
        fields.append(f)
        fields += [
            {
                "is_short": False,
                "text": {
                    "content": "**集成测试用例详情**:\n" + "\n".join(message.get("its")),
                    "tag": "lark_md"
                }
            },
        ]
        if message.get("uts") is not None:
            fields.append(f)
            fields += [
                {
                    "is_short": False,
                    "text": {
                        "content": "**失败单测详情**:\n" + "\n".join(message.get("uts")),
                        "tag": "lark_md"
                    }
                },
            ]
        return {
            "fields": fields,
            "tag": "div",
        }


    def _format_fields(self):
        fields = []
        for module in self.args_obj.modules:
            fields += [{
                "is_short": False,
                "text": {
                    "content": "",
                    "tag": "lark_md"
                }
            }, {
                "is_short": True,
                "text": {
                    "content": f"**报错模块**\n{module}",
                    "tag": "lark_md"
                }
            }, {
                "is_short": True,
                "text": {
                    "content": f"**模块负责人**\n{self._get_at(module)}",
                    "tag": "lark_md"
                }
            }]
        fields = [{
            "is_short": True,
            "text": {
                "content": f"**分支名称**\n{self.args_obj.branch}",
                "tag": "lark_md"
            }
        }, {
            "is_short": True,
            "text": {
                "content": f"**构建任务**\n{self.args_obj.runner}",
                "tag": "lark_md"
            }
        }] + fields + [{
            "is_short": False,
            "text": {
                "content": "",
                "tag": "lark_md"
            }
        }, {
            "is_short": True,
            "text": {
                "content": f"**提交人**\n{self._get_at_name()}",
                "tag": "lark_md"
            }
        }, {
            "is_short": True,
            "text": {
                "content": f"**提交人邮箱**\n{self.args_obj.commit_author_email}",
                "tag": "lark_md"
            }
        }, {
            "is_short": False,
            "text": {
                "content": "",
                "tag": "lark_md"
            }
        }, {
            "is_short": True,
            "text": {
                "content": f"**提交时间**\n{self.args_obj.commit_time}",
                "tag": "lark_md"
            }
        }, {
            "is_short": True,
            "text": {
                "content": f"**提交信息**\n{self.args_obj.commit_message}",
                "tag": "lark_md"
            }
        }]
        return {
            "fields": fields,
            "tag": "div",
        }

    def _format_night_build_message_detail_button(self, message):
        button_text = ""
        if message.get("pass", False):
            return {
                "actions": [{
                    "tag": "button",
                    "text": {
                        "content": "当前开发分支工作基本正常",
                        "tag": "lark_md"
                    },
                    "url": self.args_obj.detail_url,
                    "type": "default",
                    "value": {}
                }],
                "tag": "action"
            }
        return {
            "actions": [{
                "tag": "button",
                "text": {
                    "content": "点击查看用例详情",
                    "tag": "lark_md"
                },
                "url": self.args_obj.detail_url,
                "type": "default",
                "value": {}
            }],
            "tag": "action"
        }


    def _format_error_message_detail_button(self):
        return {
            "actions": [{
                "tag": "button",
                "text": {
                    "content": "点击查看报错信息",
                    "tag": "lark_md"
                },
                "url": self.args_obj.detail_url,
                "type": "default",
                "value": {}
            }],
            "tag": "action"
        }

    def todict(self):
        if self.args_obj.message_type == "build_fail_notice":
            return [
                self._format_fields(),
                self._format_error_message_detail_button(),
            ]
        if self.args_obj.message_type == "night_build_notice":
            return [
                self._format_test_fields(),
                self._format_night_build_message_detail_button(self.args_obj.message)
            ]


if __name__ == "__main__":
    args = Args()
    card = Card(args)
    color = "red"
    if args.message_type == "build_fail_notice":
        title = args.commit_author + ", 你提交的代码构建失败了, 请尽快处理"
    if args.message_type == "night_build_notice":
        p = "未通过"
        if args.message.get("pass", False):
            color = "green"
            p = "通过"
        if args.addr is not None:
            title = "环境 {} 自动化测试: {}".format(args.addr, p)
        else:
            title = "开发分支 {} 自动化测试: {}".format(args.branch, p)

    FeishuMessage(
        title, card.todict(), args.chat_id, app_id=args.app_id,
        app_secret=args.app_secret, title_color=color,
    ).send_card()

