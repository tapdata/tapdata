package com.tapdata.tm.tcm.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import com.alibaba.fastjson.JSON;
import com.google.gson.reflect.TypeToken;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.tcm.dto.PaidPlanRes;
import com.tapdata.tm.tcm.dto.ResponseMessage;
import com.tapdata.tm.tcm.dto.UserInfoDto;
import com.tapdata.tm.utils.HttpUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Date;
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

    public Object getVersionInfo(String version) {
        Object result = null;
        String responseStr = HttpUtils.sendGetData(TMC_URL + "/api/tcm/productRelease/version/tapdataAgent/" + version, null);
        if (StringUtils.isNotEmpty(responseStr)) {
            ResponseMessage responseMessage = JsonUtil.parseJson(responseStr, ResponseMessage.class);
            if (ResponseMessage.OK.equals(responseMessage.getCode())) {
                result = responseMessage.getData();
            } else {
                log.error("tcm处理异常。responseMessage：{}", responseMessage);
            }
        }
        return result;
    }

    public Object getDownloadUrl(String userId) {
        Object result = null;
        Map<String, String> headerMap = new HashMap();
        headerMap.put("user_id", userId);
        String responseStr = HttpUtils.sendGetData(TMC_URL + "/api/tcm/productRelease/downloadUrl/latest", headerMap);
        if (StringUtils.isNotEmpty(responseStr)) {
            ResponseMessage responseMessage = JsonUtil.parseJson(responseStr, ResponseMessage.class);
            if (ResponseMessage.OK.equals(responseMessage.getCode())) {
                result = responseMessage.getData();
            } else {
                log.error("tcm处理异常。responseMessage：{}", responseMessage);
            }
        }
        return result;
    }

    public PaidPlanRes describeUserPaidPlan(String userId) {
        Map<String, String> headerMap = new HashMap();
        headerMap.put("user_id", userId);
        String responseStr = HttpUtils.sendGetData(TMC_URL + "/api/tcm/user/paidPlan", headerMap);
        if (StringUtils.isNotEmpty(responseStr)) {
            ResponseMessage<PaidPlanRes> responseMessage = JsonUtil.parseJson(responseStr, new TypeToken<ResponseMessage<PaidPlanRes>>(){}.getType());
            if (ResponseMessage.OK.equals(responseMessage.getCode())) {
                return responseMessage.getData();
            } else {
                log.error("Query user paid plan failed {}({})", responseMessage.getCode(), responseMessage.getMessage());
            }
        }
        return null;
    }

    public void updateUploadStatus(Map map) {
        Object data = map.get("data");
        Object userId = ((Map) data).get("userId");
        String responseStr = HttpUtils.sendPostData(TMC_URL + "/api/tcm/updateUploadStatus",
                JSON.toJSONString(data), userId.toString());
        if (StringUtils.isNotEmpty(responseStr)) {
            ResponseMessage<String> responseMessage = JsonUtil.parseJson(responseStr, new TypeToken<ResponseMessage<PaidPlanRes>>() {
            }.getType());
            if (ResponseMessage.OK.equals(responseMessage.getCode())) {
                Object responseMsg = responseMessage.getData();
                if ("success".equals(responseMsg.toString())) {
                    log.error("Update UploadStatus failed {}({})", responseMessage.getCode(), responseMsg);
                }
            } else {
                log.error("Update UploadStatus failed {}({})", responseMessage.getCode(), responseMessage.getMessage());
            }
        }
    }

    public Date getLatestProductReleaseCreateTime() {
        String responseStr = HttpUtils.sendGetData(TMC_URL + "/api/tcm/productRelease/create_time/latest    ", null);
        if (StringUtils.isNotEmpty(responseStr)) {
            ResponseMessage responseMessage = JsonUtil.parseJson(responseStr, ResponseMessage.class);
            if (ResponseMessage.OK.equals(responseMessage.getCode())) {
                Map dataMap = (Map) responseMessage.getData();
                return (Date) dataMap.get("createTime");
            } else {
                log.error("tcm处理异常。responseMessage：{}", responseMessage);
            }
        }
        return null;
    }

}
