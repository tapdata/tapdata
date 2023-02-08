package com.tapdata.tm.mp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.mp.entity.MpAccessToken;
import com.tapdata.tm.mp.service.MpService;
import com.tapdata.tm.sdk.util.IOUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2023/1/18 上午10:52
 */
public class TestMp {

//    @Test
    public void testRequestAccessToken() {

        MpAccessToken mpAccessToken = requestNewAccessToken();
        Assertions.assertNotNull(mpAccessToken);
        Assertions.assertNotNull(mpAccessToken.getAccessToken());
        Assertions.assertNotNull(mpAccessToken.getExpiresIn());
        Assertions.assertNotNull(mpAccessToken.getExpiresAt());
    }

    private MpAccessToken requestNewAccessToken() {

        Logger log = LoggerFactory.getLogger(TestMp.class);
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();

        String getAccessTokenUrl = "https://api.weixin.qq.com/cgi-bin/token";

        try {
            URI uri = new URIBuilder(getAccessTokenUrl)
                    .addParameter("grant_type", "client_credential")
                    .addParameter("appid", "")
                    .addParameter("secret", "").build();

            HttpGet httpGet = new HttpGet(uri);
            CloseableHttpResponse response = httpClient.execute(httpGet);

            String body = IOUtil.readAsString(response.getEntity().getContent());
            if (response.getStatusLine().getStatusCode() == 200 && StringUtils.isNotBlank(body)) {
                Map<String, ?> data = JsonUtil.parseJsonUseJackson(body, new TypeReference<Map<String, Object>>(){});
                if (data != null && data.containsKey("access_token")) {
                    String accessToken = (String) data.get("access_token");
                    Integer expiresIn = (Integer) data.get("expires_in");

                    MpAccessToken mpAccessToken = new MpAccessToken();
                    mpAccessToken.setAccessToken(accessToken);
                    mpAccessToken.setExpiresIn(expiresIn); // Second
                    mpAccessToken.setExpiresAt(System.currentTimeMillis() + expiresIn * 1000);

                    return mpAccessToken;

                } else if (data != null && data.containsKey("errcode")){
                    Integer errCode = (Integer) data.get("errcode");
                    String errMsg = (String) data.get("errmsg");
                    log.error("Refresh access token failed, error code is {}, error message is {}", errCode, errMsg);
                }
            } else {
                log.error("Refresh access token failed, response {} {}",
                        response.getStatusLine().getStatusCode(), body);
            }

        } catch (URISyntaxException e) {
            log.error("Build get access token uri failed on refresh weChat access token", e);
        } catch (IOException e) {
            log.error("Refresh access token failed", e);
        }
        return null;
    }

//    @Test
    public void testSendMessage() {
        Logger log = LoggerFactory.getLogger(TestMp.class);
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();

        MpAccessToken mpAccessToken = requestNewAccessToken();
        Assertions.assertNotNull(mpAccessToken);

        String url = "https://api.weixin.qq.com/cgi-bin/message/template/send";
        try {
            URI uri = new URIBuilder(url)
                    .addParameter("access_token", mpAccessToken.getAccessToken()).build();

            MpService.Message message = new MpService.Message();
            message.setTouser("owKgc5iQfTJEOWkM2no5BPq_OBgE");
            message.setClient_msg_id("msg_id_" + System.currentTimeMillis());
            message.setTemplate_id("MNH8Ml2qyEA9xvi3wjElVxLmXgDePEcsnqm0PjxVPKc");
            message.setData(new HashMap<>());
            message.getData().put("first", new MpService.MsgArgument("您收到一条告警通知。", "#00FF00"));
            message.getData().put("content", new MpService.MsgArgument("告警内容", "#00FF00"));
            message.getData().put("occurtime", new MpService.MsgArgument("2023-01-18 15:13:00", "#00FF00"));
            message.getData().put("remark",
                    new MpService.MsgArgument("任务“测试任务”因错误xxxx导致停止", "#0000FF"));

            String jsonData = JsonUtil.toJsonUseJackson(message);

            HttpPost httpPost = new HttpPost(uri);
            StringEntity httpEntity = new StringEntity(jsonData, "UTF-8");
            httpEntity.setContentType("application/json; charset=UTF-8;");
            httpPost.setEntity(httpEntity);
            CloseableHttpResponse response = httpClient.execute(httpPost);

            String body = IOUtil.readAsString(response.getEntity().getContent());
            if (response.getStatusLine().getStatusCode() == 200 && StringUtils.isNotBlank(body)) {
                Map<String, ?> data = JsonUtil.parseJsonUseJackson(body, new TypeReference<Map<String, Object>>(){});

                Assertions.assertNotNull(data);
                System.out.println(body);

            } else {
                log.error("Send we chat message failed, response {} {}",
                        response.getStatusLine().getStatusCode(), body);
                Assertions.fail("Send we chat message failed");
            }
        } catch (URISyntaxException e) {
            log.error("Build send message uri failed on send weChat message", e);
        } catch (UnsupportedEncodingException e) {
            log.error("Build send message body failed on send weChat message", e);
        } catch (IOException e) {
            log.error("Send weChat message failed", e);
        }
    }

    @Test
    public void validateAccessToken() {
        Logger log = LoggerFactory.getLogger(TestMp.class);
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();

        String accessToken = "65_A9wc3lydta7AGfuQ3yz-yfzhKn6SW6ir3-A88wGVC48MnXe9vYfA4mg8Aneblo4JBISIgFrtHeu3jOoW4jlHSPCJ5jPwB_i8hSlXDePlO6D0u06j9-vcxYTeA_UTTEbAEADDO";

        String getAccessTokenUrl = "https://api.weixin.qq.com/cgi-bin/user/get";

        try {
            URI uri = new URIBuilder(getAccessTokenUrl)
                    .addParameter("access_token", accessToken).build();

            HttpGet httpGet = new HttpGet(uri);
            CloseableHttpResponse response = httpClient.execute(httpGet);

            String body = IOUtil.readAsString(response.getEntity().getContent());
            log.debug(body);
            if (response.getStatusLine().getStatusCode() == 200 && StringUtils.isNotBlank(body)) {
                Map<String, ?> data = JsonUtil.parseJsonUseJackson(body, new TypeReference<Map<String, Object>>(){});
                String errCode = data != null && data.get("errcode") != null ? data.get("errcode").toString() : null;
                String errMsg = data != null && data.get("errmsg") != null ? data.get("errmsg").toString() : null;
                if (!"0".equals(errCode)) {
                    Assertions.fail(errMsg);
                }
            } else {
                Assertions.fail(body);
            }

        } catch (URISyntaxException e) {
            log.error("Build get access token uri failed on refresh weChat access token", e);
        } catch (IOException e) {
            log.error("Refresh access token failed", e);
        }
    }
}
