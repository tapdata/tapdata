package com.tapdata.tm.httpUtils;

import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.utils.HttpUtils;
import org.apache.http.client.ClientProtocolException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * http客户端
 */
public class HttpClientTest extends BaseJunit {
    @Autowired
    HttpUtils httpUtilsService;


    @Test
    public void testSendPostDataByMap() throws ClientProtocolException, IOException {
        String url = "http://localhost:8080/httpService/sendPostDataByMap";
        Map<String, String> map = new HashMap<String, String>();
        map.put("name", "wyj");
        map.put("city", "南京");
    }

    @Test
    public void testSendPostDataByJson() throws ClientProtocolException, IOException {
        String url = "http://localhost:8080/httpService/sendPostDataByJson";
        Map<String, String> map = new HashMap<String, String>();
        map.put("name", "wyj");
        map.put("city", "南京");
    }

    @Test
    public void testSendGetData() throws ClientProtocolException, IOException {
        Map headerMap = new HashMap();
        headerMap.put("user_id", "613f37dbb043b8350a668f4d");
        String body = httpUtilsService.sendGetData("user", headerMap);
        System.out.println("响应结果：" + body);
    }

}
