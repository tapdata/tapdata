package com.tapdata.tm.utils;

import com.tapdata.tm.Settings.dto.MailAccountDto;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.exception.TapOssNonSupportFunctionException;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.nodes.Document;
import org.springframework.stereotype.Component;

import java.util.*;


/**
 * todo Setting表的邮箱配置，是怎么样初始化进去的  邮箱已经变 了，应该怎么修改
 * SMTP 地址
 * smtp.feishu.cn
 * IMAP/SMTP 密码
 * K6U5MH5aIeq94Lno
 * SMTP 端口号（SSL）
 * 465
 * IMAP 地址
 * imap.feishu.cn
 * SMTP 端口号（starttls）
 * 587
 * IMAP 端口号
 * 993
 */
@Slf4j
@Component
public class MailUtilsImpl implements IMailUtils{
    @Override
    public SendStatus sendHtmlMail(String subject, List<String> toList, String username, String agentName, String emailHref, String maiContent) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public SendStatus sendHtmlMail(List<String> toList, String username, String agentName, String emailHref, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public SendStatus sendHtmlMail(List<String> toList, String username, String agentName, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum, String sourceId) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public SendStatus sendHtmlMail(String to, String username, String serverName, String title, String mailContent) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public String getAgentClick(String serverName, MsgTypeEnum msgTypeEnum) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public String getHrefClick(String sourceId, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public SendStatus sendValidateCode(String to, String receiveUer, String validateCode) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void sendMail(String to, Document doc, String title) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void sendMail(String to, SendStatus sendStatus, Document doc, String title) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public String readHtmlToString(String htmlFileName) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public String getMailContent(SystemEnum systemEnum, MsgTypeEnum msgTypeEnum) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public String getMailTitle(SystemEnum systemEnum, MsgTypeEnum msgTypeEnum) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void sendHtmlEmail(MailAccountDto parms, List<String> adressees, String title, String content) {
        throw new BizException("TapOssNonSupportFunctionException");
    }
}
