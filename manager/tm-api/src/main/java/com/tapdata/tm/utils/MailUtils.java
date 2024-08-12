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
import org.jetbrains.annotations.Nullable;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;


/**
 * Setting表的邮箱配置，是怎么样初始化进去的  邮箱已经变 了，应该怎么修改
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

    public static final Integer CLOUD_MAIL_LIMIT = 10;

    @Autowired
    SettingsService settingsService;
    @Autowired
    BlacklistService blacklistService;

    private static List<String> productList;
    @Value("#{'${spring.profiles.include:idaas}'.split(',')}")
    private void setProductList(List<String> versionList){
        this.productList = versionList;
    }

    public static final String SEND_STATUS_FALSE = "false";
    public static final String MAIL_TEMPLATE = "mailTemplate.html";
    public static final String USER_NAME = "username";
    public static final String SYS_NAME = "sysName";
    public static final String SYS_NAME_AGENT = "Your Agent：";
    public static final String SYS_NAME_TASK = "Your Task：";
    public static final String AGENT_NAME = "agentName";
    public static final String MAIL_CONTENT = "mailContent";
    public static final String CLICK_HREF = "clickHref";
    public static final String EMAIL_SEND_EXCEPTION = "邮件发送异常";
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
        String html = readHtmlToString(MAIL_TEMPLATE);

        // 写入模板内容
        Document doc = Jsoup.parse(html);
        doc.getElementById(USER_NAME).html(username);

        if (StringUtils.isEmpty(maiContent) || StringUtils.isEmpty(maiContent.trim())) {
            sendStatus.setErrorMessage("mailContent 为空");
            return sendStatus;
        }
        if (StringUtils.isEmpty(agentName)) {
            sendStatus.setErrorMessage("agentName 为空");
            return sendStatus;
        }
        doc.getElementById(SYS_NAME).html(SYS_NAME_AGENT);
        doc.getElementById(AGENT_NAME).html(agentName);
        doc.getElementById(MAIL_CONTENT).html(maiContent);
        doc.getElementById(CLICK_HREF).attr("href", emailHref);
        sendEmail(doc, sendStatus, notInBlacklistAddress, subject, EMAIL_SEND_EXCEPTION);
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
        String html = readHtmlToString(MAIL_TEMPLATE);

        // 写入模板内容
        Document doc = Jsoup.parse(html);
        doc.getElementById(USER_NAME).html(username);
        doc.getElementById(AGENT_NAME).html(agentName);

        if (SystemEnum.AGENT.equals(systemEnum)) {
            doc.getElementById(SYS_NAME).html(SYS_NAME_AGENT);
        } else if (SystemEnum.DATAFLOW.equals(systemEnum) || SystemEnum.SYNC.equals(systemEnum) || SystemEnum.MIGRATION.equals(systemEnum)) {
            doc.getElementById(SYS_NAME).html(SYS_NAME_TASK);
        }

        String mailContent = getMailContent(systemEnum, msgTypeEnum);
        doc.getElementById(MAIL_CONTENT).html(mailContent);
        doc.getElementById(CLICK_HREF).attr("href", emailHref);

        sendEmail(doc, sendStatus, notInBlacklistAddress, getMailTitle(systemEnum, msgTypeEnum), EMAIL_SEND_EXCEPTION);
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
        String html = readHtmlToString(MAIL_TEMPLATE);

        // 写入模板内容
        Document doc = Jsoup.parse(html);
        doc.getElementById(USER_NAME).html(username);
        doc.getElementById(AGENT_NAME).html(agentName);

        if (SystemEnum.AGENT.equals(systemEnum)) {
            doc.getElementById(SYS_NAME).html(SYS_NAME_AGENT);
        } else if (SystemEnum.DATAFLOW.equals(systemEnum) || SystemEnum.SYNC.equals(systemEnum) || SystemEnum.MIGRATION.equals(systemEnum)) {
            doc.getElementById(SYS_NAME).html(SYS_NAME_TASK);
        }

        String mailContent = getMailContent(systemEnum, msgTypeEnum);
        doc.getElementById(MAIL_CONTENT).html(mailContent);

        String emailHref = getHrefClick(sourceId, systemEnum, msgTypeEnum);

        doc.getElementById(CLICK_HREF).attr("href", emailHref);
        sendEmail(doc, sendStatus, notInBlacklistAddress, getMailTitle(systemEnum, msgTypeEnum), EMAIL_SEND_EXCEPTION);
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
    protected void initMailConfig() {
        String hostFromDB = String.valueOf(settingsService.getByCategoryAndKey("SMTP", "smtp.server.host"));
        String portFromDB = String.valueOf(settingsService.getByCategoryAndKey("SMTP", "smtp.server.port"));
        String usernameFromDB = String.valueOf(settingsService.getByCategoryAndKey("SMTP", "smtp.server.user"));
        String sendAddressFromDB = String.valueOf(settingsService.getByCategoryAndKey("SMTP", "email.send.address"));
        String passwordFromDB = String.valueOf(settingsService.getByCategoryAndKey("SMTP", "smtp.server.password"));

        this.host = hostFromDB;
        if (StringUtils.isNotEmpty(portFromDB)) {
            this.port = Integer.valueOf(portFromDB);
        }
        this.sendAddress = sendAddressFromDB;
        this.user = usernameFromDB;
        this.password = passwordFromDB;

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
    public static boolean sendHtmlEmail(MailAccountDto parms, List<String> adressees, String title, String content) {
        adressees = filterBlackList(adressees);
        if (adressees == null) return false;

        boolean flag = true;
        if (StringUtils.isAnyBlank(parms.getHost(), parms.getFrom(),parms.getUser(), parms.getPass()) || CollectionUtils.isEmpty(adressees)) {
            log.error("mail account info empty, params:{}", JSON.toJSONString(parms));
            flag = false;
        } else {
            if (StringUtils.isNotBlank(parms.getProxyHost()) && 0 != parms.getProxyPort()) {
                return sendEmailForProxy(parms, adressees, title, content, flag);
            }
            try {
                MailAccount account = new MailAccount();
                account.setHost(parms.getHost());
                account.setPort(parms.getPort());
                account.setAuth(true);
                account.setFrom(parms.getFrom());
                account.setUser(parms.getUser());
                account.setPass(parms.getPass());
                if ("SSL".equals(parms.getProtocol())) {
                    // 使用SSL安全连接
                    account.setSslEnable(true);
                    //指定实现javax.net.SocketFactory接口的类的名称,这个类将被用于创建SMTP的套接字
                    account.setSocketFactoryClass("javax.net.ssl.SSLSocketFactory");
                } else if ("TLS".equals(parms.getProtocol())) {
                    account.setStarttlsEnable(true);
                    account.setSocketFactoryClass("javax.net.ssl.SSLSocketFactory");
                } else {
                    account.setSslEnable(false);
                    account.setStarttlsEnable(false);
                }

                //如果设置为true,未能创建一个套接字使用指定9的套接字工厂类将导致使用java.net.Socket创建的套接字类, 默认值为true
                account.setSocketFactoryFallback(true);
                // 指定的端口连接到在使用指定的套接字工厂。如果没有设置,将使用默认端口456
                account.setSocketFactoryPort(465);
                Map<String, Object> oemConfig = OEMReplaceUtil.getOEMConfigMap("email/replace.json");
                title = OEMReplaceUtil.replace(title, oemConfig);
                content = OEMReplaceUtil.replace(assemblyMessageBody(content), oemConfig);
                MailUtil.send(account, adressees, title ,content, true);
            } catch (Exception e) {
                log.error("mail send error：{}", e.getMessage(), e);
                flag = false;
            }
        }
        log.debug("mail send status：{}", flag ? "suc" : "error");
        return flag;
    }

    @Nullable
    protected static List<String> filterBlackList(List<String> adressees) {
        if (CollectionUtils.isEmpty(adressees)) return null;

        BlacklistService blacklistService = SpringContextHelper.getBean(BlacklistService.class);
        if (blacklistService != null) {
            List<String> notInBlacklistAddress = adressees.stream().filter(to -> !blacklistService.inBlacklist(to)).collect(Collectors.toList());
            if (log.isDebugEnabled()) {
                log.debug("Blacklist filter address {}, {}", adressees, notInBlacklistAddress);
            }
            adressees = notInBlacklistAddress;
            //adressees.removeAll(blacklist);
            if (CollectionUtils.isEmpty(adressees)) {
                return null;
            }
        } else {
            log.warn("Check blacklist failed before send email, not found BlacklistService.");
        }
        return adressees;
    }

    protected static boolean sendEmailForProxy(MailAccountDto parms, List<String> adressees, String title, String content, boolean flag) {
        final String username = parms.getUser();
        final String password = parms.getPass();

        Properties properties = new Properties();
        properties.put("mail.smtp.host", parms.getHost());
        properties.put("mail.smtp.port", parms.getPort());
        properties.put("mail.smtp.auth", "true");
        if ("SSL".equals(parms.getProtocol())){
            properties.put("mail.smtp.ssl.enable", "true");
        } else if ("TLS".equals(parms.getProtocol())) {
            properties.put("mail.smtp.starttls.enable", "true");
        } else {
            properties.put("mail.smtp.ssl.enable", "false");
            properties.put("mail.smtp.starttls.enable", "false");
        }
        //set proxy server
        properties.put("mail.smtp.socks.host", parms.getProxyHost());
        properties.put("mail.smtp.socks.port", parms.getProxyPort());

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
        return flag;
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

    /**
     * 发送html形式的邮件, 重置密码
     */
    public SendStatus sendValidateCodeForResetPWD(String to, String username, String validateCode) {
        SendStatus sendStatus = new SendStatus(SEND_STATUS_FALSE, "");
        String html = readHtmlToString("resetPasswordTemplate.html");
        Document doc = Jsoup.parse(html);
        doc.getElementById(USER_NAME).html(username);
        doc.getElementById("code").html(validateCode);
        doc.getElementById("account").html(to);
        doc.getElementById("validateTimes").html("5");
        sendEmail(doc, sendStatus, Lists.newArrayList(to), "修改密码-验证码", "Send validate code email failed before reset password");
        return sendStatus;
    }

    protected Transport connectSMTP(Session session) throws MessagingException {
        Transport transport = session.getTransport("smtp");
        if (null != port) {
            transport.connect(host, port, user, password);
        } else {
            transport.connect(host, user, password);
        }
        return transport;
    }

    protected MimeMessage message(Session session, List<String> internetAddress, String emailContent, String emailSubject) throws MessagingException, UnsupportedEncodingException {
        MimeMessage message = new MimeMessage(session);
        this.initMailConfig();
        message.setFrom(new InternetAddress(this.sendAddress));
        InternetAddress[] internetAddressList = getInternetAddress(internetAddress);
        message.setRecipients(Message.RecipientType.TO, internetAddressList);

        message.setContent(emailContent, "text/html;charset=UTF-8");
        message.setSentDate(new Date());
        message.saveChanges();
        message.setSubject(emailSubject);
        return message;
    }

    protected void closeTransport(Transport transport) {
        if (null != transport) {
            try {
                transport.close();//关闭连接
            } catch (MessagingException e) {
                log.error("发送邮件 ，transport 关闭异常", e);
            }
        }
    }

    protected Session emailSession() {
        Properties props = new Properties();
        props.put("mail.smtp.host", "");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        props.put("mail.smtp.ssl.checkserveridentity", "true");
        props.put("mail.smtp.socketFactory.fallback", "true");
        Session session = Session.getDefaultInstance(props);
        session.setDebug(true);
        return session;
    }

    protected void sendEmail(Document doc, SendStatus sendStatus, List<String> list, String subject, String errorMessage) {
        String result = doc.toString();
        Session session = emailSession();
        Transport transport = null;
        try {
            MimeMessage message = message(session, list, result, subject);
            transport = connectSMTP(session);
            transport.sendMessage(message, message.getAllRecipients());
            sendStatus.setStatus("true");
        } catch (Exception e) {
            log.error(errorMessage, e);
            sendStatus.setErrorMessage(e.getMessage());
        } finally {
            closeTransport(transport);
        }
    }
}
