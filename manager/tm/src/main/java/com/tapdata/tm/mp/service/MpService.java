package com.tapdata.tm.mp.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.mp.entity.MpAccessToken;
import com.tapdata.tm.mp.repository.MpAccessTokenRepository;
import com.tapdata.tm.sdk.util.IOUtil;
import com.tapdata.tm.utils.SendStatus;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.http.*;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2023/1/17 下午3:18
 */
@Service
@Slf4j
public class MpService {

    private final CloseableHttpClient httpClient;

    @Value("${weChat.mp.appId:}")
    private String appId;
    @Value("${weChat.mp.appSecret:}")
    private String appSecret;

    @Autowired
    private MpAccessTokenRepository repository;

    /**
     * {{first.DATA}}
     *
     * 告警内容：{{content.DATA}}
     * 告警发生时间：{{occurtime.DATA}}
     * {{remark.DATA}}
     */
    public static final String ALARM_TEMPLATE_ID = "MNH8Ml2qyEA9xvi3wjElVxLmXgDePEcsnqm0PjxVPKc";

    public MpService() {
        this.httpClient = HttpClientBuilder.create().addInterceptorLast(new HttpRequestInterceptor() {
            @Override
            public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
                log.debug("> {} {}", request.getRequestLine().getMethod(), request.getRequestLine().getUri());
                Arrays.stream(request.getAllHeaders()).forEach(header -> {
                    log.debug("> {}: {}", header.getName(), header.getValue());
                });
            }
        }).addInterceptorFirst(new HttpResponseInterceptor() {
            @Override
            public void process(HttpResponse response, HttpContext context) throws HttpException, IOException {
                log.debug("< {} {}", response.getStatusLine().getStatusCode(), response.getStatusLine().getProtocolVersion());
                Arrays.stream(response.getAllHeaders()).forEach(header -> {
                    log.debug("> {}: {}", header.getName(), header.getValue());
                });
            }
        }).build();
    }

    public boolean enableWeChat() {
        return StringUtils.isNotBlank(this.appId) && StringUtils.isNotBlank(this.appSecret);
    }

    public String getType() {
        return "wechat";
    }

    public void sendMessage(String openid, Message message) {

        Validate.notNull(openid, "OpenId can't be empty on send weChat message.");
        //Validate.notNull(templateId, "TemplateId can't be empty on send weChat message.");
        Validate.notNull(message, "Message can't be empty on send weChat message.");
        /*Validate.notNull(title, "MessageData can't be empty on send weChat message.");
        Validate.notNull(content, "MessageData can't be empty on send weChat message.");
        Validate.notNull(alarmTime, "MessageData can't be empty on send weChat message.");*/

        /*Validate.isTrue(title.length() <= 10, "The message title must be less than 20 in length");
        Validate.isTrue(remark.length() <= 20, "The message title must be less than 20 in length");*/

        for (int i = 0; i < 5; i++) {

            MpAccessToken mpAccessToken = getAccessToken();
            if (mpAccessToken == null) {
                refreshAccessToken();
                mpAccessToken = getAccessToken();
            }

            if (mpAccessToken == null) {
                log.error("Send we chat message failed, can't found access token from mp.");
                return;
            }

            String url = "https://api.weixin.qq.com/cgi-bin/message/template/send";
            try {
                URI uri = new URIBuilder(url)
                        .addParameter("access_token", mpAccessToken.getAccessToken()).build();

                String jsonData = JsonUtil.toJsonUseJackson(message);;

                log.debug("{}/5 Send notify to weChat {}: {}", i+1, openid, jsonData);

                HttpPost httpPost = new HttpPost(uri);
                StringEntity httpEntity = new StringEntity(jsonData, "UTF-8");
                httpEntity.setContentType("application/json; charset=UTF-8;");
                httpPost.setEntity(httpEntity);
                CloseableHttpResponse response = httpClient.execute(httpPost);

                String body = IOUtil.readAsString(response.getEntity().getContent());
                if (response.getStatusLine().getStatusCode() == 200 && StringUtils.isNotBlank(body)) {
                    Map<String, ?> data = JsonUtil.parseJsonUseJackson(body, new TypeReference<Map<String, Object>>(){});
                    log.debug(body);
                    String errorCode = data != null && data.get("errcode") != null ? data.get("errcode").toString() : "";
                    if ("40001".equals(errorCode) || "42001".equals(errorCode)) {
                        // invalid credential, access_token is invalid or not latest
                        // refresh access token and try send
                        refreshAccessToken(true);
                    } else if ("0".equals(errorCode)){
                        break;
                    } else {
                        log.error("Send weChat message failed and try send, response {}", body);
                    }
                } else {
                    log.error("Send weChat message failed, response {} {}",
                            response.getStatusLine().getStatusCode(), body);
                }

                try {
                    // try send on after 5 second
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    log.error("Interrupted weChat send message thread.");
                }
            } catch (URISyntaxException e) {
                log.error("Build send message uri failed on send weChat message", e);
            } catch (UnsupportedEncodingException e) {
                log.error("Build send message body failed on send weChat message", e);
            } catch (IOException e) {
                log.error("Send weChat message failed", e);
            }
        }
    }

    /**
     * 推送告警信息到微信
     * @param openid
     * @param title
     * @param content
     * @param alarmTime
     */
    public SendStatus sendAlarmMsg(String openid, String title, String content, Date alarmTime) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        MpService.Message message = new MpService.Message();
        message.setTouser(openid);
        message.setClient_msg_id("msg_id_" + System.currentTimeMillis());
        message.setTemplate_id(ALARM_TEMPLATE_ID);
        message.setData(new HashMap<>());
        message.getData().put("first", new MpService.MsgArgument(title, null));
        message.getData().put("content", new MpService.MsgArgument(content, null));
        message.getData().put("occurtime", new MpService.MsgArgument(sdf.format(alarmTime), null));
        message.getData().put("remark",
                new MpService.MsgArgument("详情请登录“Tapdata Cloud V3.0 控制台”查看", "#FD9804"));
        this.sendMessage(openid, message);
        SendStatus sendStatus = new SendStatus();
        sendStatus.setStatus("true");
        return sendStatus;
    }

    /**
     * 轮询检查 access token的有效期，如果 access token 在5分钟内过期就执行刷新动作
     */
    @Scheduled(fixedDelay = 30000)
    @SchedulerLock(name="refreshWeChatAccessToken", lockAtLeastFor = "PT30S", lockAtMostFor = "PT30S")
    public synchronized void refreshAccessToken() {
        refreshAccessToken(false);
    }

    public synchronized void refreshAccessToken(boolean force) {

        if (!this.enableWeChat()) {
            return;
        }

        MpAccessToken accessToken = getAccessToken();
        // 过期时间小于 5分钟
        if (force || accessToken == null ||
                accessToken.getExpiresAt() - System.currentTimeMillis() <= 300000 ) {

            MpAccessToken mpAccessToken = requestNewAccessToken();
            if (mpAccessToken != null) {
                mpAccessToken.setName("weChatAccessToken");
                mpAccessToken.setLastUpdAt(new Date());
                Query query = Query.query(Criteria.where("name").is("weChatAccessToken"));
                repository.upsert(query, mpAccessToken);
            }
        } else {
            log.debug("WeChat access token is valid");
        }

    }

    public MpAccessToken getAccessToken() {
        Query query = Query.query(Criteria.where("name").is("weChatAccessToken"));
        query.with(Sort.by(Sort.Order.desc("_id")));
        return repository.findOne(query).orElse(null);
    }

    private MpAccessToken requestNewAccessToken() {

        log.debug("Refresh mp weChat access token");
        String getAccessTokenUrl = "https://api.weixin.qq.com/cgi-bin/token";

        try {
            URI uri = new URIBuilder(getAccessTokenUrl)
                    .addParameter("grant_type", "client_credential")
                    .addParameter("appid", this.appId)
                    .addParameter("secret", this.appSecret).build();

            HttpGet httpGet = new HttpGet(uri);
            CloseableHttpResponse response = httpClient.execute(httpGet);

            String body = IOUtil.readAsString(response.getEntity().getContent());
            log.debug("Refresh token response data {}", body);
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

    @PreDestroy
    public void destroy() {
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Getter
    @Setter
    public static class MiniProgram {
        private String appid;
        private String pagepath;

    }

    @Getter
    @Setter
    public static class Message {
        private String touser;
        private String template_id;
        private String url;
        private MiniProgram miniprogram;
        private String client_msg_id;
        private Map<String, MsgArgument> data;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MsgArgument {
        private String value;
        private String color;
    }
}
