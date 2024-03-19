package com.tapdata.tm.utils;

import com.tapdata.tm.Settings.dto.MailAccountDto;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import org.jsoup.nodes.Document;

import java.util.List;

public interface IMailUtils {
    SendStatus sendHtmlMail(String subject, List<String> toList, String username, String agentName, String emailHref, String maiContent);


    /**
     * 发送html形式的邮件
     */
    SendStatus sendHtmlMail(List<String> toList, String username, String agentName, String emailHref, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum);


    /**
     * 发送html形式的邮件
     */
    SendStatus sendHtmlMail(List<String> toList, String username, String agentName, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum, String sourceId);
    SendStatus sendHtmlMail(String to, String username, String serverName, String title, String mailContent );
    String getAgentClick(String serverName, MsgTypeEnum msgTypeEnum);
    String getHrefClick(String sourceId, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum);

    /**
     * 发送html形式的邮件
     */
    SendStatus sendValidateCode(String to, String receiveUer, String validateCode);

    void sendMail(String to, Document doc, String title);

    void sendMail(String to, SendStatus sendStatus, Document doc, String title);
    String readHtmlToString(String htmlFileName);
    String getMailContent(SystemEnum systemEnum, MsgTypeEnum msgTypeEnum);


    String getMailTitle(SystemEnum systemEnum, MsgTypeEnum msgTypeEnum);

    /**
     * 发送HTML邮件
     */
    void sendHtmlEmail(MailAccountDto parms, List<String> adressees, String title, String content);
}
