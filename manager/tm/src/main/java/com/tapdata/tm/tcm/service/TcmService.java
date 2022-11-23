package com.tapdata.tm.tcm.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.tcm.dto.ResponseMessage;
import com.tapdata.tm.tcm.dto.UserInfoDto;
import com.tapdata.tm.utils.HttpUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
public class TcmService {
    @Value("${tcm.url:http://192.168.1.189:31103}")
    private String TMC_URL = "";

    public UserInfoDto getUserInfo(String userId) {
        UserInfoDto userInfoDto = null;
        Map headerMap = new HashMap();
        headerMap.put("user_id", userId);
        String responseStr = HttpUtils.sendGetData(TMC_URL + "/api/tcm/user", headerMap);
        if (StringUtils.isNotEmpty(responseStr)) {
            ResponseMessage responseMessage = JsonUtil.parseJson(responseStr, ResponseMessage.class);
            if (ResponseMessage.OK.equals(responseMessage.getCode())) {
                Map dataMap = (Map) responseMessage.getData();
                userInfoDto= BeanUtil.mapToBean(dataMap,UserInfoDto.class,false, CopyOptions.create());
            } else {
                log.error("tcm处理异常。responseMessage：{}", responseMessage);
            }
        }
        return userInfoDto;
    }
}
