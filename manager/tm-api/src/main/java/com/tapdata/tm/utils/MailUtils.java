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

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;


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
public class MailUtils {

    private String host;
    private Integer port;

    private String user;
    private String sendAddress;

    private String password;

    public final static Integer CLOUD_MAIL_LIMIT = 10;
    @Autowired
    SettingsService settingsService;

    @Autowired
    BlacklistService blacklistService;

    private static List<String> productList;
    @Value("#{'${spring.profiles.include:idaas}'.split(',')}")
    private void setProductList(List<String> versionList){
        productList = versionList;
    }

    public final static String SEND_STATUS_FALSE = "false";
    /**
     * 发送html形式的邮件
     */
    public SendStatus sendHtmlMail(String subject, List<String> toList, String username, String agentName, String emailHref, String maiContent) {
        SendStatus sendStatus = new SendStatus(SEND_STATUS_FALSE, "");
        List<String> notInBlacklistAddress = checkNotInBlacklistAddress(toList,sendStatus);
        if(CollectionUtils.isEmpty(notInBlacklistAddress)){
            return sendStatus;
        }
        // 读取html模板
        String html = readHtmlToString("mailTemplate.html");

        // 写入模板内容
        Document doc = Jsoup.parse(html);
        doc.getElementById("username").html(username);

        if (StringUtils.isEmpty(agentName)) {
            sendStatus.setErrorMessage("agentName 为空");
            return sendStatus;
        }
        doc.getElementById("sysName").html("Your Agent：");
        doc.getElementById("agentName").html(agentName);
        doc.getElementById("mailContent").html(maiContent);
        doc.getElementById("clickHref").attr("href", emailHref);

        String result = doc.toString();
        Properties props = new Properties();
        props.put("mail.smtp.host", "");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        props.put("mail.smtp.socketFactory.fallback", "true");
        Session session = Session.getDefaultInstance(props);
        session.setDebug(true);

        Transport transport = null;
        MimeMessage message = new MimeMessage(session);
        try {
            //初始化发送邮件配置
            this.initMailConfig();
            message.setFrom(new InternetAddress(this.sendAddress));// 设置发件人的地址
            InternetAddress[] internetAddressList = getInternetAddress(notInBlacklistAddress);
            message.setRecipients(Message.RecipientType.TO, internetAddressList);// 设置收件人,并设置其接收类型为TO
            message.setSubject(subject);// 设置标题
            message.setContent(result, "text/html;charset=UTF-8"); // 设置邮件内容类型为html
            message.setSentDate(new Date());// 设置发信时间
            message.saveChanges();// 存储邮件信息

            // 发送邮件
            transport = session.getTransport("smtp");
            if (null != port) {
                transport.connect(host, port, user, password);
            } else {
                transport.connect(host, user, password);
            }
            transport.sendMessage(message, message.getAllRecipients());

            //发送邮件成功，status置为true
            sendStatus.setStatus("true");
        } catch (Exception e) {
            log.error("邮件发送异常", e);
            sendStatus.setErrorMessage(e.getMessage());
        } finally {
            if (null != transport) {
                try {
                    transport.close();//关闭连接
                } catch (MessagingException e) {
                    log.error("发送邮件 ，transport 关闭异常", e);
                }
            }
        }
        return sendStatus;
    }


    /**
     * 发送html形式的邮件
     */
    public SendStatus sendHtmlMail(List<String> toList, String username, String agentName, String emailHref, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum) {
        SendStatus sendStatus = new SendStatus(SEND_STATUS_FALSE, "");
        List<String> notInBlacklistAddress = checkNotInBlacklistAddress(toList,sendStatus);
        if(CollectionUtils.isEmpty(notInBlacklistAddress)){
            return sendStatus;
        }
        // 读取html模板
        String html = readHtmlToString("mailTemplate.html");

        // 写入模板内容
        Document doc = Jsoup.parse(html);
        doc.getElementById("username").html(username);
        doc.getElementById("agentName").html(agentName);

        if (SystemEnum.AGENT.equals(systemEnum)) {
            doc.getElementById("sysName").html("Your Agent：");
        } else if (SystemEnum.DATAFLOW.equals(systemEnum) || SystemEnum.SYNC.equals(systemEnum) || SystemEnum.MIGRATION.equals(systemEnum)) {
            doc.getElementById("sysName").html("Your Task：");
        }

        String mailContent = getMailContent(systemEnum, msgTypeEnum);
        doc.getElementById("mailContent").html(mailContent);
        doc.getElementById("clickHref").attr("href", emailHref);

        String result = doc.toString();
        Properties props = new Properties();
        props.put("mail.smtp.host", "");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        props.put("mail.smtp.socketFactory.fallback", "true");
        Session session = Session.getDefaultInstance(props);
        session.setDebug(true);

        Transport transport = null;
        MimeMessage message = new MimeMessage(session);
        try {
            //初始化发送邮件配置
            this.initMailConfig();
            message.setFrom(new InternetAddress(this.sendAddress));// 设置发件人的地址
            InternetAddress[] internetAddressList = getInternetAddress(notInBlacklistAddress);
            message.setRecipients(Message.RecipientType.TO, internetAddressList);// 设置收件人,并设置其接收类型为TO

            String title = getMailTitle(systemEnum, msgTypeEnum);
            message.setSubject(title);// 设置标题
            message.setContent(result, "text/html;charset=UTF-8"); // 设置邮件内容类型为html
            message.setSentDate(new Date());// 设置发信时间
            message.saveChanges();// 存储邮件信息

            // 发送邮件
            transport = session.getTransport("smtp");
            if (null != port) {
                transport.connect(host, port, user, password);
            } else {
                transport.connect(host, user, password);
            }
            transport.sendMessage(message, message.getAllRecipients());

            //发送邮件成功，status置为true
            sendStatus.setStatus("true");
        } catch (Exception e) {
            log.error("邮件发送异常", e);
            sendStatus.setErrorMessage(e.getMessage());
        } finally {
            if (null != transport) {
                try {
                    transport.close();//关闭连接
                } catch (MessagingException e) {
                    log.error("发送邮件 ，transport 关闭异常", e);
                }
            }
        }
        return sendStatus;
    }


    /**
     * 发送html形式的邮件
     */
    public SendStatus sendHtmlMail(List<String> toList, String username, String agentName, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum, String sourceId) {
        SendStatus sendStatus = new SendStatus(SEND_STATUS_FALSE, "");
        List<String> notInBlacklistAddress = checkNotInBlacklistAddress(toList,sendStatus);
        if(CollectionUtils.isEmpty(notInBlacklistAddress)){
            return sendStatus;
        }
        // 读取html模板
        String html = readHtmlToString("mailTemplate.html");

        // 写入模板内容
        Document doc = Jsoup.parse(html);
        doc.getElementById("username").html(username);
        doc.getElementById("agentName").html(agentName);

        if (SystemEnum.AGENT.equals(systemEnum)) {
            doc.getElementById("sysName").html("Your Agent：");
        } else if (SystemEnum.DATAFLOW.equals(systemEnum) || SystemEnum.SYNC.equals(systemEnum) || SystemEnum.MIGRATION.equals(systemEnum)) {
            doc.getElementById("sysName").html("Your Task：");
        }

        String mailContent = getMailContent(systemEnum, msgTypeEnum);
        doc.getElementById("mailContent").html(mailContent);

        String emailHref = getHrefClick(sourceId, systemEnum, msgTypeEnum);

        doc.getElementById("clickHref").attr("href", emailHref);

        String result = doc.toString();
        Properties props = new Properties();
        props.put("mail.smtp.host", "");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        props.put("mail.smtp.socketFactory.fallback", "true");
        Session session = Session.getDefaultInstance(props);
        session.setDebug(true);

        Transport transport = null;
        MimeMessage message = new MimeMessage(session);
        try {
            //初始化发送邮件配置
            this.initMailConfig();
            message.setFrom(new InternetAddress(this.sendAddress));// 设置发件人的地址
            InternetAddress[] internetAddressList = getInternetAddress(notInBlacklistAddress);
            message.setRecipients(Message.RecipientType.TO, internetAddressList);// 设置收件人,并设置其接收类型为TO

            String title = getMailTitle(systemEnum, msgTypeEnum);
            message.setSubject(title);// 设置标题
            message.setContent(result, "text/html;charset=UTF-8"); // 设置邮件内容类型为html
            message.setSentDate(new Date());// 设置发信时间
            message.saveChanges();// 存储邮件信息

            // 发送邮件
            transport = session.getTransport("smtp");
            if (null != port) {
                transport.connect(host, port, user, password);
            } else {
                transport.connect(host, user, password);
            }
            transport.sendMessage(message, message.getAllRecipients());

            //发送邮件成功，status置为true
            sendStatus.setStatus("true");
        } catch (Exception e) {
            log.error("邮件发送异常", e);
            sendStatus.setErrorMessage(e.getMessage());
        } finally {
            if (null != transport) {
                try {
                    transport.close();//关闭连接
                } catch (MessagingException e) {
                    log.error("发送邮件 ，transport 关闭异常", e);
                }
            }
        }
        return sendStatus;
    }

    /**
     * 企业版发送的通知邮件，没有点击连接
     */
    public SendStatus sendHtmlMail(String to, String username, String serverName, String title, String mailContent ) {
        if (blacklistService.inBlacklist(to)) {
            return new SendStatus(SEND_STATUS_FALSE, String.format("Email %s in blacklist.", to));
        }
        return new SendStatus("true", "");
    }


    /**
     * 获取发送出去邮件的点击连接
     *
     * @return
     */
    public String getAgentClick(String serverName, MsgTypeEnum msgTypeEnum) {
        Object hostUrl = settingsService.getByCategoryAndKey(CategoryEnum.SMTP, KeyEnum.EMAIL_HREF).getValue();
        String clickHref = "https://cloud.tapdata.net/console/#/";

        if (MsgTypeEnum.CONNECTION_INTERRUPTED.equals(msgTypeEnum)) {
            clickHref = hostUrl + "instance?keyword=" + serverName;
        } else if (MsgTypeEnum.CONNECTED.equals(msgTypeEnum)) {
            clickHref = hostUrl + "instance?keyword=" + serverName;
        } else if (MsgTypeEnum.WILL_RELEASE_AGENT.equals(msgTypeEnum)) {
            clickHref = hostUrl.toString();
        } else if (MsgTypeEnum.RELEASE_AGENT.equals(msgTypeEnum)) {
            clickHref = hostUrl.toString();
        }
        return clickHref;
    }


    /**
     * 获取发送出去邮件的点击连接
     *
     * @param sourceId
     * @return
     */
    public String getHrefClick(String sourceId, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum) {
        Object hostUrl = settingsService.getByCategoryAndKey(CategoryEnum.SMTP, KeyEnum.EMAIL_HREF).getValue();
        String clickHref = "https://cloud.tapdata.net/console/#/";

        if (SystemEnum.AGENT.equals(systemEnum)) {
            if (MsgTypeEnum.CONNECTION_INTERRUPTED.equals(msgTypeEnum)) {
//                http://sit.cloud.tapdata.net/console/#/instance?keyword=dfs-agent-17e2d9e3eb6
                clickHref = hostUrl + "instance?keyword=" + sourceId;
            } else if (MsgTypeEnum.CONNECTED.equals(msgTypeEnum)) {
                clickHref = hostUrl + "instanceDetails?id=" + sourceId;
            } else if (MsgTypeEnum.WILL_RELEASE_AGENT.equals(msgTypeEnum)) {
                clickHref = hostUrl.toString();
            } else if (MsgTypeEnum.RELEASE_AGENT.equals(msgTypeEnum)) {
                clickHref = hostUrl.toString();
            }
        } else if (SystemEnum.MIGRATION.equals(systemEnum)) {
            if (MsgTypeEnum.STOPPED_BY_ERROR.equals(msgTypeEnum)) {
//                http://sit.cloud.tapdata.net/console/#/task/61d688b55fe7526dc84d6c19/monitor
                clickHref = hostUrl + "task/" + sourceId + "/monitor";
            } else if (MsgTypeEnum.CONNECTED.equals(msgTypeEnum)) {
                clickHref = hostUrl + "task/" + sourceId + "/monitor";
            }
        } else if (SystemEnum.SYNC.equals(systemEnum)) {
            if (MsgTypeEnum.STOPPED_BY_ERROR.equals(msgTypeEnum)) {
            }
        } else if (SystemEnum.DATAFLOW.equals(systemEnum)) {
            if (MsgTypeEnum.STOPPED_BY_ERROR.equals(msgTypeEnum)) {
            } else if (MsgTypeEnum.CONNECTED.equals(msgTypeEnum)) {
            }
        }
        return clickHref;
    }

    /**
     * 发送html形式的邮件
     */
    public SendStatus sendValidateCode(String to, String receiveUer, String validateCode) {
        return new SendStatus("true", "");
    }

    public void sendMail(String to, Document doc, String title) {

    }

    public void sendMail(String to, SendStatus sendStatus, Document doc, String title) {

    }

    @NotNull
    private MailAccount getMailAccount() {
        MailAccount account = new MailAccount();
        return account;
    }


    /**
     * 读取html文件为String
     *
     * @param htmlFileName
     * @return
     * @throws Exception
     */
    public static String readHtmlToString(String htmlFileName) {
        InputStream is = MailUtils.class.getClassLoader().getResourceAsStream(htmlFileName);
        try(Reader reader = new InputStreamReader(is, "UTF-8");) {
            if (is == null) {
                log.error("未找到模板文件");
            }
            StringBuilder sb = new StringBuilder();
            int bufferSize = 1024;
            char[] buffer = new char[bufferSize];
            int length = 0;
            while ((length = reader.read(buffer, 0, bufferSize)) != -1) {
                sb.append(buffer, 0, length);
            }
            return sb.toString();
        } catch (IOException e) {
            log.error("发送邮件异常", e);
        }
        return "";
    }

    /**
     * SMTP 地址
     * smtp.feishu.cn
     * IMAP/SMTP 密码
     * jF7SAd4lfIzGBFpm
     * SMTP 端口号（SSL）
     * 465
     * IMAP 地址
     * imap.feishu.cn
     * SMTP 端口号（starttls）
     * 587
     * IMAP 端口号
     * 993
     */
    private void initMailConfig() {
        String host = String.valueOf(settingsService.getByCategoryAndKey("SMTP", "smtp.server.host"));
        String port = String.valueOf(settingsService.getByCategoryAndKey("SMTP", "smtp.server.port"));
        String username = String.valueOf(settingsService.getByCategoryAndKey("SMTP", "smtp.server.user"));
        String sendAddress = String.valueOf(settingsService.getByCategoryAndKey("SMTP", "email.send.address"));
        String password = String.valueOf(settingsService.getByCategoryAndKey("SMTP", "smtp.server.password"));

        this.host = host;
        if (StringUtils.isNotEmpty(port)) {
            this.port = Integer.valueOf(port);
        }
        this.sendAddress = sendAddress;
        this.user = username;
        this.password = password;

    }

    /**
     * obj.emailHref = server.daasSettings.emailHref || 'https://cloud.tapdata.net/console/#/';
     * emailObj.agent.event_data.connectionInterrupted.message = connectionInterruptedMessage;
     * emailObj.agent.event_data.connected.message = connectedMessage;
     * emailObj.agent.event_data.willReleaseAgent.message = willReleaseAgent;
     * emailObj.agent.event_data.releaseAgent.message = releaseAgent;
     * <p>
     * emailObj.migration.event_data.stoppedByError.message = errorMessage;
     * <p>
     * emailObj.sync.event_data.stoppedByError.message = errorMessage;
     * <p>
     * emailObj.dataFlow.event_data.stoppedByError.message = errorMessage;
     * <p>
     *
     * @param systemEnum
     * @param msgTypeEnum
     * @return
     */
    public String getMailContent(SystemEnum systemEnum, MsgTypeEnum msgTypeEnum) {
        String mailContent = "";
        if (SystemEnum.AGENT.equals(systemEnum)) {
            if (MsgTypeEnum.CONNECTION_INTERRUPTED.equals(msgTypeEnum)) {
                mailContent = "Status has changed from running to offline, your tasks may become abnormal, please check it.";
            } else if (MsgTypeEnum.CONNECTED.equals(msgTypeEnum)) {
                mailContent = "Status has changed from offline to running, You can now continue with your data journey !";
            } else if (MsgTypeEnum.WILL_RELEASE_AGENT.equals(msgTypeEnum)) {
                mailContent = "It has been over a week since your last usage, and the compute resources will be automatically reclaimed tomorrow evening at 20:00. If you wish to continue using them, please log in to the system before the cleanup to automatically extend your usage time.";
            } else if (MsgTypeEnum.RELEASE_AGENT.equals(msgTypeEnum)) {
                mailContent = "It has been over a week since your last usage, and the compute resources have been automatically reclaimed. If you wish to continue using them, you can recreate them through the guided setup process.";
            }

        } else if (SystemEnum.MIGRATION.equals(systemEnum)) {
            if (MsgTypeEnum.STOPPED_BY_ERROR.equals(msgTypeEnum)) {
                mailContent = "Task has stopped by error, please check it.";
            } else if (MsgTypeEnum.DELETED.equals(msgTypeEnum)) {
                mailContent = "Task has been deleted";
            } else if (MsgTypeEnum.PAUSED.equals(msgTypeEnum)) {
                mailContent = "Task has paused";
            } else if (MsgTypeEnum.STARTED.equals(msgTypeEnum)) {
                mailContent = "Task has started";
            }
        } else if (SystemEnum.SYNC.equals(systemEnum)) {
            if (MsgTypeEnum.STOPPED_BY_ERROR.equals(msgTypeEnum)) {
                mailContent = "Task has stopped by error, please check it.";
            } else if (MsgTypeEnum.DELETED.equals(msgTypeEnum)) {
                mailContent = "Task has been deleted";
            } else if (MsgTypeEnum.PAUSED.equals(msgTypeEnum)) {
                mailContent = "Task has paused";
            } else if (MsgTypeEnum.STARTED.equals(msgTypeEnum)) {
                mailContent = "Task has started";
            }
        } else if (SystemEnum.DATAFLOW.equals(systemEnum)) {
            if (MsgTypeEnum.STOPPED_BY_ERROR.equals(msgTypeEnum)) {
                mailContent = "Task has stopped by error, please check it.";
            } else if (MsgTypeEnum.CONNECTED.equals(msgTypeEnum)) {
                mailContent = "Task Status has changed to Running";
            }
        }
        return mailContent;
    }


    /**
     * emailObj.agent.event_data.connectionInterrupted.title = '【Tapdata】Agent离线提醒';
     * emailObj.agent.event_data.connected.title = '【Tapdata】Agent状态恢复提醒';
     * emailObj.migration.event_data.stoppedByError.title = '【Tapdata】运行任务出错提醒';
     * emailObj.sync.event_data.stoppedByError.title = '【Tapdata】运行任务出错提醒';
     * emailObj.dataFlow.event_data.stoppedByError.title = '【Tapdata】运行任务出错提醒';
     * emailObj.dataFlow.event_data.stoppedByError.title = '【Tapdata】运行任务出错提醒';
     * emailObj.agent.event_data.willReleaseAgent.title = '【Tapdata】测试Agent资源即将回收提醒';
     * emailObj.agent.event_data.releaseAgent.title = '【Tapdata】测试Agent资源回收提醒';
     *
     * @param systemEnum
     * @param msgTypeEnum
     * @return
     */
    public static String getMailTitle(SystemEnum systemEnum, MsgTypeEnum msgTypeEnum) {
        String mailTitle = "";
        if (SystemEnum.AGENT.equals(systemEnum)) {
            if (MsgTypeEnum.CONNECTION_INTERRUPTED.equals(msgTypeEnum)) {
                mailTitle = "【Tapdata Cloud】Agent is offline";
            } else if (MsgTypeEnum.CONNECTED.equals(msgTypeEnum)) {
                mailTitle = "【Tapdata Cloud】Agent is online";


            } else if (MsgTypeEnum.WILL_RELEASE_AGENT.equals(msgTypeEnum)) {
                mailTitle = "【Tapdata Cloud】Test Agent resource will be recycled";
            } else if (MsgTypeEnum.RELEASE_AGENT.equals(msgTypeEnum)) {
                mailTitle = "【Tapdata Cloud】Test Agent resource has been recycled";
            }

        } else if (SystemEnum.MIGRATION.equals(systemEnum)) {
            if (MsgTypeEnum.STOPPED_BY_ERROR.equals(msgTypeEnum)) {
                mailTitle = "【Tapdata Cloud】Task has stopped by error";

            }
        } else if (SystemEnum.SYNC.equals(systemEnum)) {
            if (MsgTypeEnum.STOPPED_BY_ERROR.equals(msgTypeEnum)) {
                mailTitle = "【Tapdata Cloud】Task has stopped by error";
            }
        } else if (SystemEnum.DATAFLOW.equals(systemEnum)) {
            if (MsgTypeEnum.STOPPED_BY_ERROR.equals(msgTypeEnum)) {
                mailTitle = "【Tapdata Cloud】Task has stopped by error";
            } else if (MsgTypeEnum.CONNECTED.equals(msgTypeEnum)) {
                mailTitle = "【Tapdata Cloud】Task Status has changed to Running";
            }
        }
        Map<String, Object> oemConfig = OEMReplaceUtil.getOEMConfigMap("email/replace.json");
        mailTitle = OEMReplaceUtil.replace(mailTitle, oemConfig);
        return mailTitle;
    }

    /**
     * 发送HTML邮件
     */
    public static void sendHtmlEmail(MailAccountDto parms, List<String> adressees, String title, String content) {
        if (CollectionUtils.isEmpty(adressees)) return;

        BlacklistService blacklistService = SpringContextHelper.getBean(BlacklistService.class);
        if (blacklistService != null) {
            List<String> notInBlacklistAddress = adressees.stream().filter(to -> !blacklistService.inBlacklist(to)).collect(Collectors.toList());
            if (log.isDebugEnabled()) {
                log.debug("Blacklist filter address {}, {}", adressees, notInBlacklistAddress);
            }
            adressees = notInBlacklistAddress;
            //adressees.removeAll(blacklist);
            if (CollectionUtils.isEmpty(adressees)) {
                return;
            }
        } else {
            log.warn("Check blacklist failed before send email, not found BlacklistService.");
        }

        boolean flag = true;
        if (StringUtils.isAnyBlank(parms.getHost(), parms.getFrom(),parms.getUser(), parms.getPass()) || CollectionUtils.isEmpty(adressees)) {
            log.error("mail account info empty, params:{}", JSON.toJSONString(parms));
            flag = false;
        } else {
            sendEmailSmtp(parms, adressees, title, content, flag);
        }
        log.debug("mail send status：{}", flag ? "suc" : "error");
    }

    protected static void sendEmailSmtp(MailAccountDto parms, List<String> adressees, String title, String content, boolean flag) {
        final String username = parms.getUser();
        final String password = parms.getPass();

        Properties properties = System.getProperties();
        properties.put("mail.smtp.host", parms.getHost());
        properties.put("mail.smtp.port", parms.getPort());
        properties.put("mail.smtp.auth", "true");
        if ("SSL".equals(parms.getProtocol()) || "TLS".equals(parms.getProtocol())) {
            properties.put("mail.smtp.starttls.enable", "true");
        } else {
            properties.put("mail.smtp.starttls.enable", "false");
        }

        Session session = Session.getInstance(properties, new Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password);
            }
        });
        try {
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(parms.getFrom()));

            Address[] tos = null;
            tos = new InternetAddress[adressees.size()];
            for (int i = 0; i < adressees.size(); i++) {
                tos[i] = new InternetAddress(adressees.get(i));
            }
            message.setRecipients(Message.RecipientType.TO, tos);

            Map<String, Object> oemConfig = OEMReplaceUtil.getOEMConfigMap("email/replace.json");
            title = OEMReplaceUtil.replace(title, oemConfig);
            content = OEMReplaceUtil.replace(assemblyMessageBody(content), oemConfig);
            message.setSubject(title, "UTF-8");
            MimeBodyPart text = new MimeBodyPart();
            text.setContent(content, "text/html;charset=UTF-8");
            MimeMultipart mimeMultipart = new MimeMultipart();
            mimeMultipart.addBodyPart(text);
            mimeMultipart.setSubType("related");
            message.setContent(mimeMultipart);

            Transport.send(message);
        } catch (Exception e) {
            log.error("mail send error：{}", e.getMessage(), e);
            flag = false;
        }
        log.debug("mail send status：{}", flag ? "suc" : "error");
    }

    protected static String assemblyMessageBody(String message) {
        //is cloud env
        boolean isCloud = productList != null && productList.contains("dfs");
        String cloud = "";
        if(isCloud){
            cloud = "Cloud";
        }
        return "<!DOCTYPE html>\n" +
                "<html>\n" +
                "<head>\n" +
                "<meta charset=\"utf-8\" />\n" +
                "</head>\n" +
                "<body>\n" +
                "Hello there,<br />\n" +
                "<br />\n" +
                message +
                "</p>\n" +
                "<br />" +
                "<br />" +
                "This mail was sent by Tapdata "+
                cloud+"."+
                "</body>\n" +
                "</html>";
    }

    protected List<String> checkNotInBlacklistAddress(List<String> toList,SendStatus sendStatus){
        List<String> notInBlacklistAddress = toList.stream().filter(to -> !blacklistService.inBlacklist(to)).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(notInBlacklistAddress)) {
            sendStatus.setErrorMessage(String.format("Email %s in blacklist.", toList));
        }
        return notInBlacklistAddress;
    }

    protected InternetAddress[] getInternetAddress(List<String> notInBlacklistAddress) throws UnsupportedEncodingException {
        List<InternetAddress> addressList = new ArrayList<>();
        for(String address : notInBlacklistAddress){
            InternetAddress internetAddress =  new InternetAddress(address,this.user);
            addressList.add(internetAddress);
        }
        return addressList.toArray(new InternetAddress[notInBlacklistAddress.size()]);
    }

}
