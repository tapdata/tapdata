package com.tapdata.tm.config;

import ch.qos.logback.core.PropertyDefinerBase;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/16 下午7:38
 * @description
 */
public class CanonicalHostNamePropertyDefiner extends PropertyDefinerBase {

    private static String hostname;

    @Override
    public String getPropertyValue() {
        return getHostname();
    }

    public static String getHostname() {
        if (hostname != null)
            return hostname;

        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            hostname = BigInteger.valueOf(new Date().getTime()).toString(32);
        }

        return hostname;
    }
}
