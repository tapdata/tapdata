package com.tapdata.tm.proxy.service.impl;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.proxy.dto.SubscribeDto;
import com.tapdata.tm.proxy.dto.SubscribeResponseDto;
import io.tapdata.modules.api.net.entity.SubscribeToken;
import io.tapdata.pdk.core.utils.CommonUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * @Author: Gavin
 * @Date: 2022/10/14
 * @Description:
 */
public class ProxyService {
    public static final String key = "asdfFSDJKFHKLASHJDKQJWKJehrklHDFJKSMhkj3h24jkhhJKASDH723ty4jkhasdkdfjhaksjdfjfhJDJKLHSAfadsf";
    public static ProxyService create(){
        return new ProxyService();
    }
    public SubscribeResponseDto generateSubscriptionToken(SubscribeDto subscribeDto, UserDetail userDetail) {
        return this.generateSubscriptionToken(subscribeDto, key);
    }
    private SubscribeResponseDto generateSubscriptionToken(SubscribeDto subscribeDto, String key) {
        if(subscribeDto == null)
            throw new BizException("SubscribeDto is null");
        if(subscribeDto.getSubscribeId() == null)
            throw new BizException("SubscribeId is null");
        if(subscribeDto.getService() == null)
            subscribeDto.setService("engine");
        if(subscribeDto.getExpireSeconds() == null)
            throw new BizException("SubscribeDto expireSeconds is null");
        SubscribeToken subscribeToken = new SubscribeToken();
        subscribeToken.setSubscribeId(subscribeDto.getSubscribeId());
        subscribeToken.setService(subscribeDto.getService());
        subscribeToken.setExpireAt(System.currentTimeMillis() + (subscribeDto.getExpireSeconds() * 1000));
        byte[] tokenBytes = null;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            subscribeToken.to(baos);
            tokenBytes = baos.toByteArray();
        } catch (IOException e) {
            throw new BizException("Serialize SubscribeDto failed, " + e.getMessage());
        }
        String token = null;
        try {
            token = new String(Base64.getUrlEncoder().encode(CommonUtils.encryptWithRC4(tokenBytes, key)), StandardCharsets.US_ASCII);
        } catch (Exception e) {
            throw new BizException("Encrypt SubscribeDto failed, " + e.getMessage());
        }

        SubscribeResponseDto subscribeResponseDto = new SubscribeResponseDto();
        subscribeResponseDto.setToken(token);
        return subscribeResponseDto;
    }
}
