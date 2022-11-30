package com.tapdata.tm.proxy.service.impl;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.proxy.dto.SubscribeDto;
import com.tapdata.tm.proxy.dto.SubscribeResponseDto;
import com.tapdata.tm.sdk.auth.HmacSHA256Signer;
import com.tapdata.tm.sdk.util.Base64Util;
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
    public static final String KEY = "asdfFSDJKFHKLASHJDKQJWKJehrklHDFJKSMhkj3h24jkhhJKASDH723ty4jkhasdkdfjhaksjdfjfhJDJKLHSAfadsf";

    public String generateStaticToken(String userId, String secret) {

//        final String secret = "xxxxxx";

//        String userId = "userId";

        byte[] digestData = new HmacSHA256Signer().sign(userId + secret, secret);

        StringBuilder result = new StringBuilder();
        for (byte aByte : digestData) {
            result.append(String.format("%02x", aByte));
        }
        String sign = Base64Util.encode(result.toString().getBytes());

        String token = String.format("%s.%s", Base64Util.encode(userId.getBytes()), sign);
        System.out.println(token);
        return token;

    }

    public SubscribeResponseDto generateSubscriptionToken(SubscribeDto subscribeDto, UserDetail userDetail, String staticToken, String requestUri) {
        return this.generateSubscriptionToken(subscribeDto, KEY, staticToken, requestUri);
    }
    private SubscribeResponseDto generateSubscriptionToken(SubscribeDto subscribeDto, String key, String staticToken, String requestUri) {
        if(subscribeDto == null)
            throw new BizException("SubscribeDto is null");
        if(subscribeDto.getSubscribeId() == null)
            throw new BizException("SubscribeId is null");
        if(subscribeDto.getService() == null)
            subscribeDto.setService("engine");
        subscribeDto.setExpireSeconds(Integer.MAX_VALUE);
        if(subscribeDto.getExpireSeconds() == null)
            throw new BizException("SubscribeDto expireSeconds is null");
        SubscribeToken subscribeToken = new SubscribeToken();
        subscribeToken.setSubscribeId(subscribeDto.getSubscribeId());
        subscribeToken.setService(subscribeDto.getService());
        subscribeToken.setExpireAt(System.currentTimeMillis() + (subscribeDto.getExpireSeconds() * 1000L));
        byte[] tokenBytes;
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

        if(staticToken != null) {
            int pos = requestUri.indexOf("/api/");
            if(pos < 0)
                throw new BizException("requestUri doesn't contains \"/api/\", requestUri: " + requestUri);
            String prefix = requestUri.substring(0, pos);
            token = prefix + "/api/proxy/callback/" + token + "?__token=" + staticToken;
        } else {
            token = "/api/proxy/callback/" + token;
        }
        SubscribeResponseDto subscribeResponseDto = new SubscribeResponseDto();
        subscribeResponseDto.setToken(token);
        return subscribeResponseDto;
    }
}
