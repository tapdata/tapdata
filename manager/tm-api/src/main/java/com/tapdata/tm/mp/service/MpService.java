package com.tapdata.tm.mp.service;

import com.tapdata.tm.mp.entity.MpAccessToken;
import com.tapdata.tm.utils.SendStatus;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.PreDestroy;
import java.util.Date;
import java.util.Map;

public interface MpService {
    /**
     * {{first.DATA}}
     * <p>
     * 告警内容：{{content.DATA}}
     * 告警发生时间：{{occurtime.DATA}}
     * {{remark.DATA}}
     */
    String ALARM_TEMPLATE_ID = "MNH8Ml2qyEA9xvi3wjElVxLmXgDePEcsnqm0PjxVPKc";

    boolean enableWeChat();

    String getType();

    void sendMessage(String openid, Message message);

    SendStatus sendAlarmMsg(String openid, String title, String content, Date alarmTime);

    @Scheduled(fixedDelay = 30000)
    @SchedulerLock(name = "refreshWeChatAccessToken", lockAtLeastFor = "PT30S", lockAtMostFor = "PT30S")
    void refreshAccessToken();

    void refreshAccessToken(boolean force);

    MpAccessToken getAccessToken();

    @PreDestroy
    void destroy();

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
        private Map<String, MpService.MsgArgument> data;
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
