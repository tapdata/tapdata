package com.tapdata.tm.utils;

import cn.hutool.extra.mail.MailAccount;
import cn.hutool.extra.mail.MailUtil;
import com.alibaba.fastjson.JSON;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.dto.MailAccountDto;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import com.tapdata.tm.message.service.BlacklistService;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.IDataPermissionHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.*;
@Component
public class MailUtils {

    public final static Integer CLOUD_MAIL_LIMIT = 10;
    private static IMailUtils iMailUtils;

    public MailUtils(IMailUtils utils) {
        MailUtils.iMailUtils = utils;
    }

    /**
     * 发送html形式的邮件
     */
    public SendStatus sendHtmlMail(String subject, List<String> toList, String username, String agentName, String emailHref, String maiContent) {
        return iMailUtils.sendHtmlMail(subject, username, agentName, emailHref, maiContent);
    }


    /**
     * 发送html形式的邮件
     */
    public SendStatus sendHtmlMail(List<String> toList, String username, String agentName, String emailHref, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum) {
        return iMailUtils.sendHtmlMail(toList, username, agentName, emailHref, systemEnum, msgTypeEnum);
    }


    /**
     * 发送html形式的邮件
     */
    public SendStatus sendHtmlMail(List<String> toList, String username, String agentName, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum, String sourceId) {
        return iMailUtils.sendHtmlMail(toList, username, agentName, systemEnum, msgTypeEnum, sourceId);
    }

    /**
     * 企业版发送的通知邮件，没有点击连接
     */
    public SendStatus sendHtmlMail(String to, String username, String serverName, String title, String mailContent ) {
        return iMailUtils.sendHtmlMail(to, username, serverName, title, mailContent);
    }


    /**
     * 获取发送出去邮件的点击连接
     *
     * @return
     */
    public String getAgentClick(String serverName, MsgTypeEnum msgTypeEnum) {
        return iMailUtils.getAgentClick(serverName, msgTypeEnum);
    }


    /**
     * 获取发送出去邮件的点击连接
     *
     * @param sourceId
     * @return
     */
    public String getHrefClick(String sourceId, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum) {
        return iMailUtils.getHrefClick(sourceId, systemEnum, msgTypeEnum);
    }

    /**
     * 发送html形式的邮件
     */
    public SendStatus sendValidateCode(String to, String receiveUer, String validateCode) {
        return iMailUtils.sendValidateCode(to, receiveUer, validateCode);
    }

    public void sendMail(String to, Document doc, String title) {
        iMailUtils.sendMail(to, doc, title);
    }

    public void sendMail(String to, SendStatus sendStatus, Document doc, String title) {
        iMailUtils.sendMail(to, sendStatus, doc, title);
    }


    /**
     * 读取html文件为String
     *
     * @param htmlFileName
     * @return
     * @throws Exception
     */
    public static String readHtmlToString(String htmlFileName) {
        return iMailUtils.readHtmlToString(htmlFileName);
    }
    public String getMailContent(SystemEnum systemEnum, MsgTypeEnum msgTypeEnum) {
        return iMailUtils.getMailContent(systemEnum, msgTypeEnum);
    }


    public static String getMailTitle(SystemEnum systemEnum, MsgTypeEnum msgTypeEnum) {
        return iMailUtils.getMailTitle(systemEnum, msgTypeEnum);
    }

    /**
     * 发送HTML邮件
     */
    public static void sendHtmlEmail(MailAccountDto parms, List<String> adressees, String title, String content) {
        iMailUtils.sendHtmlEmail(parms, adressees, title, content);
    }
}