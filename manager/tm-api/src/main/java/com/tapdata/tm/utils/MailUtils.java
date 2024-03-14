package com.tapdata.tm.utils;

import cn.hutool.extra.mail.MailAccount;
import cn.hutool.extra.mail.MailUtil;
import com.alibaba.fastjson.JSON;
import com.tapdata.tm.Settings.dto.MailAccountDto;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import com.tapdata.tm.message.service.BlacklistService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface MailUtils {
    
    Integer CLOUD_MAIL_LIMIT = 10;
    String SEND_STATUS_FALSE = "false";
    org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(MailUtils.class);
    SendStatus sendHtmlMail(String subject, List<String> toList, String username, String agentName, String emailHref, String maiContent);

    SendStatus sendHtmlMail(List<String> toList, String username, String agentName, String emailHref, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum);

    SendStatus sendHtmlMail(List<String> toList, String username, String agentName, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum, String sourceId);

    SendStatus sendHtmlMail(String to, String username, String serverName, String title, String mailContent);

    String getAgentClick(String serverName, MsgTypeEnum msgTypeEnum);

    String getHrefClick(String sourceId, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum);

    SendStatus sendValidateCode(String to, String receiveUer, String validateCode);

    void sendMail(String to, Document doc, String title);

    void sendMail(String to, SendStatus sendStatus, Document doc, String title);

    String getMailContent(SystemEnum systemEnum, MsgTypeEnum msgTypeEnum);
    static void sendHtmlEmail(MailAccountDto parms, List<String> adressees, String title, String content) {
    }
    default String getMailTitle(SystemEnum systemEnum, MsgTypeEnum msgTypeEnum) {
        return "";
    }
    public static String readHtmlToString(String htmlFileName) {
        return "";
    }
}
