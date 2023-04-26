/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2019. All rights reserved.
 */

package io.tapdata.connector.mrs.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * KerberoUtil
 *
 * @author
 * @since 8.0.0
 */
public class KerberosUtil {
    private static final Logger logger = LoggerFactory.getLogger(KerberosUtil.class);
    /**
     * JAVA_VENDER
     */
    public static final String JAVA_VENDER = "java.vendor";
    /**
     * IBM_FLAG
     */
    public static final String IBM_FLAG = "IBM";
    /**
     * CONFIG_CLASS_FOR_IBM
     */
    public static final String CONFIG_CLASS_FOR_IBM = "com.ibm.security.krb5.internal.Config";
    /**
     * CONFIG_CLASS_FOR_SUN
     */
    public static final String CONFIG_CLASS_FOR_SUN = "sun.security.krb5.Config";
    /**
     * METHOD_GET_INSTANCE
     */
    public static final String METHOD_GET_INSTANCE = "getInstance";
    /**
     * METHOD_GET_DEFAULT_REALM
     */
    public static final String METHOD_GET_DEFAULT_REALM = "getDefaultRealm";
    /**
     * DEFAULT_REALM
     */
    public static final String DEFAULT_REALM = "HADOOP.COM";

    /**
     * Get Krb5 Domain Realm
     */
    public static String getKrb5DomainRealm() {
        Class<?> krb5ConfClass;
        String peerRealm = null;
        try {
            if (System.getProperty(JAVA_VENDER).contains(IBM_FLAG)) {
                krb5ConfClass = Class.forName(CONFIG_CLASS_FOR_IBM);
            } else {
                krb5ConfClass = Class.forName(CONFIG_CLASS_FOR_SUN);
            }

            Method getInstanceMethod = krb5ConfClass.getMethod(METHOD_GET_INSTANCE);
            Object kerbConf = getInstanceMethod.invoke(krb5ConfClass);

            Method getDefaultRealmMethod = krb5ConfClass.getDeclaredMethod(METHOD_GET_DEFAULT_REALM);
            if (getDefaultRealmMethod.invoke(kerbConf) instanceof String) {
                peerRealm = (String) getDefaultRealmMethod.invoke(kerbConf);
            }
            logger.info("Get default realm successfully, the realm is : {}", peerRealm);

        } catch (ClassNotFoundException e) {
            peerRealm = DEFAULT_REALM;
            logger.warn("Get default realm failed, use default value : " + DEFAULT_REALM);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            peerRealm = DEFAULT_REALM;
            logger.warn("Get default realm failed, use default value : " + DEFAULT_REALM);
        }

        return peerRealm;
    }
}
