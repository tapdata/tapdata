package com.tapdata.tm.sms;

import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import com.tapdata.tm.message.dto.MessageDto;
import com.tapdata.tm.utils.SendStatus;

public interface SmsService {
    /**
     * * const TemplateCode = {
     * * 	connectionInterrupted:"SMS_219753221",
     * * 	connected:"SMS_219753223",
     * * 	stoppedByError:"SMS_219748341",
     * * 	releaseAgent:"SMS_222870037",
     * * 	willReleaseAgent:"SMS_222870036"
     * * };
     */
    String CONNECTION_INTERRUPT = "SMS_219753221";
    String CONNECTED = "SMS_219753223";
    String JOB_ERROR_STOP = "SMS_219748341";
    String RELEASE_AGENT = "SMS_222870037";
    String WILL_RELEASE_AGENT = "SMS_222870036";
    String TASK_ABNORMITY_NOTICE = "SMS_269600641";
    String TASK_NOTICE = "SMS_269900128";
    static final String TEMPLATE_PARAM_AGENT_NAME = "AgentName";
    static final String TEMPLATE_PARAM_JOB_NAME = "JobName";

    public static String getTelParamNameByMessageSystem(String systemEnum) {
        if (SystemEnum.AGENT.getValue().equals(systemEnum)) {
            return TEMPLATE_PARAM_AGENT_NAME;
        } else if (SystemEnum.MIGRATION.getValue().equals(systemEnum)) {
            return TEMPLATE_PARAM_JOB_NAME;
        }
        return "";
    }


    boolean enableSms();

    String getType();

    SendStatus sendShortMessage(String templateCode, String phoneNumbers, String system, String jobName);

    SendStatus sendShortMessage(String templateCode, String phoneNumbers, String templateParam);

    String getTemplateCode(MessageDto messageDto);

    String getTemplateCode(MsgTypeEnum msgTypeEnum);
    default String getTemplateCode(String msgType) {
        String smsTemplateCode = "";
        return smsTemplateCode;
    }
}
