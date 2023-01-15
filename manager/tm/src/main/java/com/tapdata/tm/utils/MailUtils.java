package com.tapdata.tm.utils;

import cn.hutool.extra.mail.MailAccount;
import cn.hutool.extra.mail.MailUtil;
import com.alibaba.fastjson.JSON;
import com.tapdata.tm.Settings.dto.MailAccountDto;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jsoup.nodes.Document;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.List;
import java.util.Objects;


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



    /**
     * 发送html形式的邮件
     */
    public SendStatus sendHtmlMail(String to, String username, String agentName, String emailHref, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum) {
        return new SendStatus("true", "");
    }


    /**
     * 发送html形式的邮件
     */
    public SendStatus sendHtmlMail(String to, String username, String agentName, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum, String sourceId) {

        return new SendStatus("true", "");
    }


    /**
     * 企业版发送的通知邮件，没有点击连接
     */
    public SendStatus sendHtmlMail(String to, String username, String serverName, String title, String mailContent ) {
        return new SendStatus("true", "");
    }


    /**
     * 发送html形式的邮件
     */
    public SendStatus sendHtmlMail(String to, String username, String serverName, String title, String mailContent, String emailHref) {
        return new SendStatus("true", "");
    }


    /**
     * 获取发送出去邮件的点击连接
     *
     * @return
     */
    public String getAgentClick(String serverName, MsgTypeEnum msgTypeEnum) {

        return "";
    }


    /**
     * 获取发送出去邮件的点击连接
     *
     * @param sourceId
     * @return
     */
    public String getHrefClick(String sourceId, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum) {
        return "";
    }

    public String getHrefClick(String sourceId, String subId, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum) {
        return "";
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
        InputStream is = null;
        Reader reader = null;
        try {
            is = MailUtils.class.getClassLoader().getResourceAsStream(htmlFileName);
            if (is == null) {
                log.error("未找到模板文件");
            }
            reader = new InputStreamReader(is, "UTF-8");
            StringBuilder sb = new StringBuilder();
            int bufferSize = 1024;
            char[] buffer = new char[bufferSize];
            int length = 0;
            while ((length = reader.read(buffer, 0, bufferSize)) != -1) {
                sb.append(buffer, 0, length);
            }
            return sb.toString();
        } catch (UnsupportedEncodingException e) {
            log.error("发送邮件异常", e);
        } catch (IOException e) {
            log.error("发送邮件异常", e);
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (IOException e) {
                log.error("关闭io流异常", e);
            }
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                log.error("关闭io流异常", e);
            }
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
                mailContent = "状态已由运行中变为离线，可能会影响您的任务正常运行，请及时处理。";
            } else if (MsgTypeEnum.CONNECTED.equals(msgTypeEnum)) {
                mailContent = "状态已由离线变为运行中，状态恢复正常。";
            } else if (MsgTypeEnum.WILL_RELEASE_AGENT.equals(msgTypeEnum)) {
                mailContent = "因超过一周未使用即将在明天晚上20:00自动回收，如果您需要继续使用可以在清理前登录系统自动延长使用时间。";
            } else if (MsgTypeEnum.RELEASE_AGENT.equals(msgTypeEnum)) {
                mailContent = "因超过一周未使用已自动回收，如果您需要继续使用可通过新手引导再次创建。";
            }

        } else if (SystemEnum.MIGRATION.equals(systemEnum)) {
            if (MsgTypeEnum.STOPPED_BY_ERROR.equals(msgTypeEnum)) {
                mailContent = "运行出错，任务已停止运行，请及时处理。";
            } else if (MsgTypeEnum.DELETED.equals(msgTypeEnum)) {
                mailContent = "任务已经被删除";
            } else if (MsgTypeEnum.PAUSED.equals(msgTypeEnum)) {
                mailContent = "任务已停止";
            } else if (MsgTypeEnum.STARTED.equals(msgTypeEnum)) {
                mailContent = "任务已启动";
            }
        } else if (SystemEnum.SYNC.equals(systemEnum)) {
            if (MsgTypeEnum.STOPPED_BY_ERROR.equals(msgTypeEnum)) {
                mailContent = "运行出错，任务已停止运行，请及时处理。";
            } else if (MsgTypeEnum.DELETED.equals(msgTypeEnum)) {
                mailContent = "任务已经被删除";
            } else if (MsgTypeEnum.PAUSED.equals(msgTypeEnum)) {
                mailContent = "任务已停止";
            } else if (MsgTypeEnum.STARTED.equals(msgTypeEnum)) {
                mailContent = "任务已启动";
            }
        } else if (SystemEnum.DATAFLOW.equals(systemEnum)) {
            if (MsgTypeEnum.STOPPED_BY_ERROR.equals(msgTypeEnum)) {
                mailContent = "运行出错，任务已停止运行，请及时处理。";
            } else if (MsgTypeEnum.CONNECTED.equals(msgTypeEnum)) {
                mailContent = "任务状态变为运行中";
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
                mailTitle = "【Tapdata】Agent离线提醒";
            } else if (MsgTypeEnum.CONNECTED.equals(msgTypeEnum)) {
                mailTitle = "【Tapdata】Agent状态恢复提醒";


            } else if (MsgTypeEnum.WILL_RELEASE_AGENT.equals(msgTypeEnum)) {
                mailTitle = "【Tapdata】测试Agent资源即将回收提醒";
            } else if (MsgTypeEnum.RELEASE_AGENT.equals(msgTypeEnum)) {
                mailTitle = "【Tapdata】测试Agent资源回收提醒";
            }

        } else if (SystemEnum.MIGRATION.equals(systemEnum)) {
            if (MsgTypeEnum.STOPPED_BY_ERROR.equals(msgTypeEnum)) {
                mailTitle = "【Tapdata】运行任务出错提醒";

            }
        } else if (SystemEnum.SYNC.equals(systemEnum)) {
            if (MsgTypeEnum.STOPPED_BY_ERROR.equals(msgTypeEnum)) {
                mailTitle = "【Tapdata】运行任务出错提醒";
            }
        } else if (SystemEnum.DATAFLOW.equals(systemEnum)) {
            if (MsgTypeEnum.STOPPED_BY_ERROR.equals(msgTypeEnum)) {
                mailTitle = "【Tapdata】运行任务出错提醒";
            } else if (MsgTypeEnum.CONNECTED.equals(msgTypeEnum)) {
                mailTitle = "【Tapdata】运行任务提醒";
            }
        }
        return mailTitle;
    }

    /**
     * 发送HTML邮件
     */
    public static void sendHtmlEmail(MailAccountDto parms, List<String> adressees, String title, String content) {
        boolean flag = true;
        if (StringUtils.isAnyBlank(parms.getHost(), parms.getFrom(),parms.getUser(), parms.getPass())) {
            log.error("mail account info empty, params:{}", JSON.toJSONString(parms));
            flag = false;
        } else {
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
                MailUtil.send(account, adressees, title, assemblyMessageBody(content), true);
            } catch (Exception e) {
                log.error("mail send error：{}", e.getMessage(), e);
                flag = false;
            }
        }
        log.debug("mail send status：{}", flag ? "suc" : "error");
    }

    protected static String assemblyMessageBody(String message) {
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
                "This mail was sent by Tapdata. " +
                "</body>\n" +
                "</html>";
    }

}
