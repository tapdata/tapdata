package com.tapdata.tm.utils;

import org.apache.commons.lang3.StringUtils;

public class EngineVersionUtil {
    public static final String VERSION_3_8_0 = "3.8.0";

    public static boolean checkEngineTransFormSchema(String version) {
        try{
            if(StringUtils.isNotEmpty(version)){
                String[] versions =  version.split("-",2);
                return compareVersions(versions[0],VERSION_3_8_0);
            }
        }catch (Exception e){
            return false;
        }
        return false;
    }

    public static Boolean compareVersions(String version,String targetVersion) {
        if (version.startsWith("v")) {
            version = version.substring(1);
        }

        String[] levels = version.split("\\.");
        String[] targetLevels = targetVersion.split("\\.");

        int length = Math.max(levels.length, targetLevels.length);

        for (int i = 0; i < length; i++) {
            int v1 = i < levels.length ? Integer.parseInt(levels[i]) : 0;
            int v2 = i < targetLevels.length ? Integer.parseInt(targetLevels[i]) : 0;

            if (v1 < v2) {
                return false;
            }
            if (v1 > v2) {
                return true;
            }
        }
        return true;
    }

}
