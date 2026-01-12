package com.tapdata.tm.v2.api.monitor.utils;

import com.tapdata.tm.apiCalls.entity.ApiCallEntity;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/29 18:27 Create
 * @description
 */
public final class ApiMetricsDelayInfoUtil {

    private ApiMetricsDelayInfoUtil() {

    }

    public static boolean checkByCode(String code, String tag) {
        if (null == code) {
            return false;
        }
        int codNumber;
        try {
            codNumber = Integer.parseInt(code);
        } catch (NumberFormatException e) {
            return false;
        }
        if (codNumber == 404) {
            return !ApiCallEntity.HttpStatusType.PUBLISH_FAILED_404.getCode().equals(tag);
        }
        return codNumber >= 200 && codNumber < 300;
    }
}
