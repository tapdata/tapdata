package com.tapdata.tm.webhook.controller;


import com.tapdata.tm.webhook.server.WebHookAdapterService;
import com.tapdata.tm.webhook.server.WebHookHttpUtilService;
import com.tapdata.tm.webhook.server.WebHookService;
import com.tapdata.tm.webhook.vo.WebHookInfoVo;

class WebHookControllerTest {
    WebHookController webHookController;
    WebHookService<WebHookInfoVo> webHookService;
    WebHookAdapterService webHookAdapter;
    WebHookHttpUtilService webHookHttpUtil;
}