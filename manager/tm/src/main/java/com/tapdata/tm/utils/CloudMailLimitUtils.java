package com.tapdata.tm.utils;

import org.apache.commons.lang3.StringUtils;

public class CloudMailLimitUtils {
    public final static Integer CLOUD_MAIL_LIMIT = 10;
    public static int getCloudMailLimit(){
        String count = System.getProperty("cloud_mail_limit");
        try{
            if(StringUtils.isNotBlank(count)){
                return Integer.parseInt(count);
            }
        }catch (Exception e){
            return CLOUD_MAIL_LIMIT;
        }
        return CLOUD_MAIL_LIMIT;
    }
}
