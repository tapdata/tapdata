package com.tapdata.tm.sms;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.dysmsapi.model.v20170525.SendSmsRequest;
import com.aliyuncs.dysmsapi.model.v20170525.SendSmsResponse;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import com.tapdata.tm.message.dto.MessageDto;
import com.tapdata.tm.utils.SendStatus;
import com.tapdata.manager.common.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;

@Slf4j
@Service
public class SmsService {

    @Value("${aliyun.accessKey:}")
    private String accessKeyId;
    @Value("${aliyun.accessSecret:}")
    private String accessKeySecret;

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

    public static final String TASK_ABNORMITY_NOTICE="SMS_269600641";

    public static final String TASK_NOTICE="SMS_269520595";



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
    public SendStatus sendShortMessage(String templateCode, String phoneNumbers, String system, String jobName) {

        SendStatus sendStatus = new SendStatus("false", "");

        if (StringUtils.isBlank(accessKeyId) || StringUtils.isBlank(accessKeySecret)) {
            sendStatus.setErrorMessage("Send sms fail, please configure {aliyun.accessKey} " +
                    "and {aliyun.accessKeySecret} in application config file.");
            return sendStatus;
        }

        SendSmsResponse sendSmsResponse = null;
        try {
            IAcsClient acsClient=  buildSendSms();
            //组装请求对象
            SendSmsRequest request = new SendSmsRequest();
            //使用post提交
            request.setMethod(MethodType.POST);

            //必填:待发送手机号。支持以逗号分隔的形式进行批量调用，批量上限为1000个手机号码,批量调用相对于单条调用及时性稍有延迟,验证码类型的短信推荐使用单条调用的方式；
            // 发送国际/港澳台消息时，接收号码格式为国际区号+号码，如“85200000000”
            request.setPhoneNumbers(phoneNumbers);

            //必填:短信签名-可在短信控制台中找到
            request.setSignName("Tapdata");

            //必填:短信模板-可在短信控制台中找到，发送国际/港澳台消息时，请使用国际/港澳台短信模版
            request.setTemplateCode(templateCode);

            //可选:模板中的变量替换JSON串,如模板内容为"亲爱的${name},您的验证码为${code}"时,此处的值为
            //友情提示:如果JSON中需要带换行符,请参照标准的JSON协议对换行符的要求,比如短信内容中包含\r\n的情况在JSON中需要表示成\\r\\n,否则会导致JSON在服务端解析失败
            //参考：request.setTemplateParam("{\"变量1\":\"值1\",\"变量2\":\"值2\",\"变量3\":\"值3\"}")
            String telParamName = getTelParamNameByMessageSystem(system);
            request.setTemplateParam("{\"" + telParamName + "\":\"" + jobName + "\"}");

            //可选-上行短信扩展码(扩展码字段控制在7位或以下，无特殊需求用户请忽略此字段)
            //request.setSmsUpExtendCode("90997");
            //可选:outId为提供给业务方扩展字段,最终在短信回执消息中将此值带回给调用者
            //        request.setOutId("yourOutId");

            //请求失败这里会抛ClientException异常
            sendSmsResponse = acsClient.getAcsResponse(request);
            if (null != sendSmsResponse && sendSmsResponse.getCode() != null && sendSmsResponse.getCode().equals("OK")) {
                sendStatus.setStatus("true");
                log.info("短信发送成功");
            }
            else {
                sendStatus.setErrorMessage(sendSmsResponse.getMessage());
            }
        } catch (Exception e) {
            log.error("发送短信异常", e);
            sendStatus.setErrorMessage(e.getMessage());
        }
        return sendStatus;
    }

    private IAcsClient buildSendSms() throws ClientException {
        try {
            System.setProperty("sun.net.client.defaultConnectTimeout", "10000");
            System.setProperty("sun.net.client.defaultReadTimeout", "10000");
            //初始化ascClient需要的几个参数
            final String product = "Dysmsapi";//短信API产品名称（短信产品名固定，无需修改）
            final String domain = "dysmsapi.aliyuncs.com";//短信API产品域名（接口地址固定，无需修改）
            //初始化ascClient,暂时不支持多region（请勿修改）
            IClientProfile profile = DefaultProfile.getProfile("cn-hangzhou", accessKeyId,
                    accessKeySecret);
            DefaultProfile.addEndpoint("cn-hangzhou", "cn-hangzhou", product, domain);
            IAcsClient acsClient = new DefaultAcsClient(profile);
            return acsClient;
        } catch (Exception e) {
            log.error("Build sendSms fail", e);
            throw e;
        }
    }


    public SendStatus sendShortMessage(String templateCode, String phoneNumbers, String templateParam) {
        SendStatus sendStatus = new SendStatus("false", "");
        log.info("sendShortMessage  starting");
        if (StringUtils.isBlank(accessKeyId) || StringUtils.isBlank(accessKeySecret)) {
            sendStatus.setErrorMessage("Send sms fail, please configure {aliyun.accessKey} " +
                    "and {aliyun.accessKeySecret} in application config file.");
            return sendStatus;
        }

        SendSmsResponse sendSmsResponse = null;
        try {
            IAcsClient acsClient = buildSendSms();
            //组装请求对象
            SendSmsRequest request = new SendSmsRequest();
            //使用post提交
            request.setMethod(MethodType.POST);

            //必填:待发送手机号。支持以逗号分隔的形式进行批量调用，批量上限为1000个手机号码,批量调用相对于单条调用及时性稍有延迟,验证码类型的短信推荐使用单条调用的方式；
            // 发送国际/港澳台消息时，接收号码格式为国际区号+号码，如“85200000000”
            request.setPhoneNumbers(phoneNumbers);

            //必填:短信签名-可在短信控制台中找到
            request.setSignName("Tapdata");

            //必填:短信模板-可在短信控制台中找到，发送国际/港澳台消息时，请使用国际/港澳台短信模版
            request.setTemplateCode(templateCode);
            request.setTemplateParam(templateParam);
            sendSmsResponse = acsClient.getAcsResponse(request);
            log.info("send sms request");
            log.info("sendSmsResponse{}",sendSmsResponse.toString());
            if (null != sendSmsResponse && sendSmsResponse.getCode() != null && sendSmsResponse.getCode().equals("OK")) {
                sendStatus.setStatus("true");
                log.info("短信发送成功");
            }
            else {
                sendStatus.setErrorMessage(sendSmsResponse.getMessage());
            }
        } catch (Exception e) {
            log.error("发送短信异常", e);
            sendStatus.setErrorMessage(e.getMessage());
        }
        return sendStatus;





    }
    public static String getTelParamNameByMessageSystem(String systemEnum) {
        if (SystemEnum.AGENT.getValue().equals(systemEnum)) {
            return TEMPLATE_PARAM_AGENT_NAME;
        } else if (SystemEnum.MIGRATION.getValue().equals(systemEnum)) {
            return TEMPLATE_PARAM_JOB_NAME;
        }
        return "";
    }


    public String getTemplateCode(MessageDto messageDto) {
        String smsTemplateCode = "";
        String msgType = messageDto.getMsg();
        if (MsgTypeEnum.CONNECTED.getValue().equals(messageDto.getMsg())) {
            smsTemplateCode = SmsService.CONNECTED;
        } else if (MsgTypeEnum.CONNECTION_INTERRUPTED.getValue().equals(msgType)) {
            smsTemplateCode = SmsService.CONNECTION_INTERRUPT;
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
    public String getTemplateCode(MsgTypeEnum msgTypeEnum) {
        String smsTemplateCode = "";
        String msgType = msgTypeEnum.getValue();
        if (MsgTypeEnum.CONNECTED.getValue().equals(msgType)) {
            smsTemplateCode = SmsService.CONNECTED;
        } else if (MsgTypeEnum.CONNECTION_INTERRUPTED.getValue().equals(msgType)) {
            smsTemplateCode = SmsService.CONNECTION_INTERRUPT;
        } else if (MsgTypeEnum.STOPPED_BY_ERROR.getValue().equals(msgType)) {
            smsTemplateCode = SmsService.JOB_ERROR_STOP;
        }
        else if ((MsgTypeEnum.WILL_RELEASE_AGENT.getValue().equals(msgType))){
            smsTemplateCode= SmsService.WILL_RELEASE_AGENT;
        }
        else if ((MsgTypeEnum.RELEASE_AGENT.getValue().equals(msgType))){
            smsTemplateCode= SmsService.RELEASE_AGENT;
        }
        return smsTemplateCode;
    }


    public static String getTemplateCode(String msgType) {
        String smsTemplateCode = "";
        if (MsgTypeEnum.CONNECTED.getValue().equals(msgType)) {
            smsTemplateCode = SmsService.CONNECTED;
        } else if (MsgTypeEnum.CONNECTION_INTERRUPTED.getValue().equals(msgType)) {
            smsTemplateCode = SmsService.CONNECTION_INTERRUPT;
        } else if (MsgTypeEnum.STOPPED_BY_ERROR.getValue().equals(msgType)) {
            smsTemplateCode = SmsService.JOB_ERROR_STOP;
        }
        else if ((MsgTypeEnum.WILL_RELEASE_AGENT.getValue().equals(msgType))){
            smsTemplateCode= SmsService.WILL_RELEASE_AGENT;
        }
        else if ((MsgTypeEnum.RELEASE_AGENT.getValue().equals(msgType))){
            smsTemplateCode= SmsService.RELEASE_AGENT;
        }
        return smsTemplateCode;
    }





}
