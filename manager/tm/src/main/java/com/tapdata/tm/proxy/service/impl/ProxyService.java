package com.tapdata.tm.proxy.service.impl;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.proxy.dto.SubscribeDto;
import com.tapdata.tm.proxy.dto.SubscribeResponseDto;
import com.tapdata.tm.proxy.dto.SubscribeURLDto;
import com.tapdata.tm.proxy.dto.SubscribeURLResponseDto;
import com.tapdata.tm.sdk.auth.HmacSHA256Signer;
import com.tapdata.tm.sdk.util.Base64Util;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.FormatUtils;
import io.tapdata.modules.api.net.entity.SubscribeToken;
import io.tapdata.modules.api.net.entity.SubscribeURLToken;
import io.tapdata.pdk.core.utils.CommonUtils;

import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.apache.http.HttpStatus.SC_BAD_REQUEST;
import static org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.apache.http.HttpStatus.SC_UNAUTHORIZED;

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

    public SubscribeResponseDto generateSubscriptionToken(SubscribeDto subscribeDto, String staticToken) {
        return this.generateSubscriptionToken(subscribeDto, KEY, staticToken);
    }

    private SubscribeResponseDto generateSubscriptionToken(SubscribeDto subscribeDto, final String key, String staticToken) {
        if (subscribeDto == null)
            throw new BizException("SubscribeDto is null");
        if (subscribeDto.getSubscribeId() == null)
            throw new BizException("SubscribeId is null");
        if (subscribeDto.getService() == null)
            subscribeDto.setService("engine");

        SubscribeResponseDto subscribeResponseDto = new SubscribeResponseDto();
        Integer expireSeconds = subscribeDto.getExpireSeconds();
        if (expireSeconds == null || expireSeconds < 1) {
            expireSeconds = Integer.MAX_VALUE;
            subscribeResponseDto.setExpireSeconds("long-term");
        } else {
            subscribeResponseDto.setExpireSeconds(String.valueOf(expireSeconds));
        }

        SubscribeToken subscribeToken = new SubscribeToken();
        subscribeToken.setSubscribeId(subscribeDto.getSubscribeId());
        subscribeToken.setService(subscribeDto.getService());
        subscribeToken.setExpireAt(System.currentTimeMillis() + (expireSeconds * 1000L));
        if (null != subscribeDto.getSupplierKey()) {
            subscribeToken.setSupplierKey(subscribeDto.getSupplierKey());
        }
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

        if (staticToken != null) {
            token = token + "?__token=" + staticToken;
        } /*else {
            token = "/api/proxy/callback/" + token;
        }*/
        subscribeResponseDto.setToken(token);
        return subscribeResponseDto;
    }

    public SubscribeURLResponseDto generateSubscriptionURL(SubscribeURLDto subscribeDto, String staticToken) {

        if (subscribeDto == null)
            throw new BizException("SubscribeDto is null");
        if (subscribeDto.getSubscribeId() == null)
            throw new BizException("SubscribeId is null");
        if (subscribeDto.getService() == null)
            subscribeDto.setService("engine");

        SubscribeURLToken subscribeToken = new SubscribeURLToken();
        //subscribeDto.setExpireSeconds(Integer.MAX_VALUE);

        subscribeToken.setSubscribeId(subscribeDto.getSubscribeId());
        subscribeToken.setService(subscribeDto.getService());
        subscribeToken.setExpireAt(System.currentTimeMillis() + (Integer.MAX_VALUE * 1000L));
        subscribeToken.setSupplierKey(subscribeDto.getSupplierKey());
        subscribeToken.setRandomId(subscribeDto.getRandomId());
        subscribeToken.setUserId(subscribeDto.getUserId());
        if (null != subscribeDto.getExpireSeconds()) {
            subscribeToken.setExpireSeconds(Long.parseLong(String.valueOf(subscribeDto.getExpireSeconds())));
        } else {
            subscribeToken.setExpireSeconds(Long.parseLong(String.valueOf(Integer.MAX_VALUE)));
        }

        byte[] tokenBytes;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            subscribeToken.to(baos);
            tokenBytes = baos.toByteArray();
        } catch (IOException e) {
            throw new BizException("Serialize SubscribeDto failed, " + e.getMessage());
        }
        String token = null;
        try {
            token = new String(Base64.getUrlEncoder().encode(CommonUtils.encryptWithRC4(tokenBytes, KEY)), StandardCharsets.US_ASCII);
        } catch (Exception e) {
            throw new BizException("Encrypt SubscribeDto failed, " + e.getMessage());
        }

        if (staticToken != null) {
            token = token + "?__token=" + staticToken;
        } /*else {
            token = "/api/proxy/callback/" + token;
        }*/
        SubscribeURLResponseDto subscribeResponseDto = new SubscribeURLResponseDto();
        subscribeResponseDto.setToken(token);
        subscribeResponseDto.setPath("api/proxy/refresh");
        return subscribeResponseDto;
    }

    public boolean validateSubscribeToken(
            final SubscribeToken subscribeDto,
            final String token,
            final HttpServletResponse response,
            final String TAG) throws IOException {
        byte[] data = null;
        try {
            data = CommonUtils.decryptWithRC4(Base64.getUrlDecoder().decode(token.getBytes(StandardCharsets.US_ASCII)), ProxyService.KEY);
        } catch (Exception e) {
            response.sendError(SC_UNAUTHORIZED, "Token illegal");
            return false;
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
            subscribeDto.from(bais);
        } catch (IOException e) {
            response.sendError(SC_INTERNAL_SERVER_ERROR, "Deserialize token failed, " + e.getMessage());
            TapLogger.info(TAG, FormatUtils.format("Deserialize token failed, key = {}", ProxyService.KEY));
            return false;
        }
//        Map<String, Object> claims = JWTUtils.getClaims(key, token);
        String service = subscribeDto.getService();
        String subscribeId = subscribeDto.getSubscribeId();
        Long expireAt = subscribeDto.getExpireAt();
        if (service == null || subscribeId == null) {
            response.sendError(SC_BAD_REQUEST, FormatUtils.format("Illegal arguments for subscribeId {}, subscribeId {}", service, subscribeId));
            return false;
        }
        if (expireAt != null && System.currentTimeMillis() > expireAt) {
            response.sendError(SC_UNAUTHORIZED, " The Hook link has expired or is no longer valid. Please use the refresh URL to retrieve it again ");
            return false;
        }
        return true;
    }


}
