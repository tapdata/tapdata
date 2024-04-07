package com.tapdata.tm.utils;

import com.alibaba.ttl.TransmittableThreadLocal;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;

public class CommonUtil {
    public static TransmittableThreadLocal<MockRequest> requestTransmittableThreadLocal = new TransmittableThreadLocal<MockRequest>();

    public static void shareRequest(MockRequest request) {
        requestTransmittableThreadLocal.set(request);
    }

    public static MockRequest getRequest() {
        return requestTransmittableThreadLocal.get();
    }

    public static void remove() {
        requestTransmittableThreadLocal.remove();
    }

}
