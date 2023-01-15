package com.tapdata.tm.utils;

import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import com.tapdata.tm.message.dto.MessageDto;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SmsUtils {


    /**
     *      * const TemplateCode = {
     *      * 	connectionInterrupted:"SMS_219753221",
     *      * 	connected:"SMS_219753223",
     *      * 	stoppedByError:"SMS_219748341",
     *      * 	releaseAgent:"SMS_222870037",
     *      * 	willReleaseAgent:"SMS_222870036"
     *      * };
     */
    public static final String CONNECTION_INTERRUPT = "SMS_219753221";
    public static final String CONNECTED = "SMS_219753223";
    public static final String JOB_ERROR_STOP = "SMS_219748341";
    public static final String RELEASE_AGENT = "SMS_222870037";
    public static final String WILL_RELEASE_AGENT = "SMS_222870036";



    private static final String TEMPLATE_PARAM_AGENT_NAME = "AgentName";
    private static final String TEMPLATE_PARAM_JOB_NAME = "JobName";


    /**
     * 发送短信
     *
     * @param templateCode 短信模板ID。请在控制台模板管理页面模板CODE一列查看。
     *                     SMS_219753221 链接中断
     *                     SMS_219753223 已经链接
     *                     SMS_219748341 任务出错停止
     *                     templateParam.AgentName agent名称
     *                     templateParam.JobName 任务名称
     * @param phoneNumbers 接收短信的手机号码。国内短信：11位手机号码。
     */
    public static SendStatus sendShortMessage(String templateCode, String phoneNumbers, String system, String jobName) {
        return new SendStatus("true", "");
    }
    private static String getTelParamNameByMessageSystem(String systemEnum) {
        if (SystemEnum.AGENT.getValue().equals(systemEnum)) {
            return TEMPLATE_PARAM_AGENT_NAME;
        } else if (SystemEnum.MIGRATION.getValue().equals(systemEnum)) {
            return TEMPLATE_PARAM_JOB_NAME;
        }
        return "";
    }


    @Deprecated
    public static String getTemplateCode(MessageDto messageDto) {
        String smsTemplateCode = "";
        String msgType = messageDto.getMsg();
        if (MsgTypeEnum.CONNECTED.getValue().equals(messageDto.getMsg())) {
            smsTemplateCode = SmsUtils.CONNECTED;
        } else if (MsgTypeEnum.CONNECTION_INTERRUPTED.getValue().equals(msgType)) {
            smsTemplateCode = SmsUtils.CONNECTION_INTERRUPT;
        } else if (MsgTypeEnum.STOPPED_BY_ERROR.getValue().equals(msgType)) {

        }
        return smsTemplateCode;
    }


    /**
     * const TemplateCode = {
     * 	connectionInterrupted:"SMS_219753221",
     * 	connected:"SMS_219753223",
     * 	stoppedByError:"SMS_219748341",
     * 	releaseAgent:"SMS_222870037",
     * 	willReleaseAgent:"SMS_222870036"
     * };
     * @param msgTypeEnum
     * @return
     */
    public static String getTemplateCode(MsgTypeEnum msgTypeEnum) {
        String smsTemplateCode = "";
        String msgType = msgTypeEnum.getValue();
        if (MsgTypeEnum.CONNECTED.getValue().equals(msgType)) {
            smsTemplateCode = SmsUtils.CONNECTED;
        } else if (MsgTypeEnum.CONNECTION_INTERRUPTED.getValue().equals(msgType)) {
            smsTemplateCode = SmsUtils.CONNECTION_INTERRUPT;
        } else if (MsgTypeEnum.STOPPED_BY_ERROR.getValue().equals(msgType)) {
            smsTemplateCode = SmsUtils.JOB_ERROR_STOP;
        }
        else if ((MsgTypeEnum.WILL_RELEASE_AGENT.getValue().equals(msgType))){
            smsTemplateCode=SmsUtils.WILL_RELEASE_AGENT;
        }
        else if ((MsgTypeEnum.RELEASE_AGENT.getValue().equals(msgType))){
            smsTemplateCode=SmsUtils.RELEASE_AGENT;
        }
        return smsTemplateCode;
    }


    public static String getTemplateCode(String msgType) {
        String smsTemplateCode = "";
        if (MsgTypeEnum.CONNECTED.getValue().equals(msgType)) {
            smsTemplateCode = SmsUtils.CONNECTED;
        } else if (MsgTypeEnum.CONNECTION_INTERRUPTED.getValue().equals(msgType)) {
            smsTemplateCode = SmsUtils.CONNECTION_INTERRUPT;
        } else if (MsgTypeEnum.STOPPED_BY_ERROR.getValue().equals(msgType)) {
            smsTemplateCode = SmsUtils.JOB_ERROR_STOP;
        }
        else if ((MsgTypeEnum.WILL_RELEASE_AGENT.getValue().equals(msgType))){
            smsTemplateCode=SmsUtils.WILL_RELEASE_AGENT;
        }
        else if ((MsgTypeEnum.RELEASE_AGENT.getValue().equals(msgType))){
            smsTemplateCode=SmsUtils.RELEASE_AGENT;
        }
        return smsTemplateCode;
    }





}
