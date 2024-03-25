package com.tapdata.tm.sms;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import com.tapdata.tm.message.dto.MessageDto;
import com.tapdata.tm.utils.SendStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class SmsServiceImpl implements SmsService {
    @Override
    public boolean enableSms() {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public String getType() {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public SendStatus sendShortMessage(String templateCode, String phoneNumbers, String system, String jobName) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public SendStatus sendShortMessage(String templateCode, String phoneNumbers, String templateParam) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public String getTemplateCode(MessageDto messageDto) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public String getTemplateCode(MsgTypeEnum msgTypeEnum) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public String getTemplateCode(String msgType) {
        throw new BizException("TapOssNonSupportFunctionException");
    }
}
