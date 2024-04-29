package com.tapdata.tm.task.service.batchin.constant;

import com.tapdata.tm.task.service.batchin.ParseBaseVersionRelMigImpl;
import com.tapdata.tm.task.service.batchin.ParseRelMig;
import com.tapdata.tm.task.service.batchin.ParseRelMig13OrMoreImpl;
import com.tapdata.tm.task.service.batchin.ParseUnKnowVersionRelMigFile;

public enum ParseRelMigFileVersionMapping {
    V1_2_0("1.2.0", ParseBaseVersionRelMigImpl.class),
    V1_3_0("1.3.0", ParseRelMig13OrMoreImpl.class),
    UN_KNOW("*", ParseUnKnowVersionRelMigFile.class)
    ;
    String version;
    Class<? extends ParseRelMig> value;
    ParseRelMigFileVersionMapping(String ver, Class<? extends ParseRelMig> value) {
        this.version = ver;
        this.value = value;
    }

    public String getVersion() {
        return version;
    }

    public static Class<? extends ParseRelMig> getInstance(String versionStr) {
        if (null == versionStr) return UN_KNOW.value;
        ParseRelMigFileVersionMapping[] values = values();
        for (ParseRelMigFileVersionMapping value : values) {
            if (value.getVersion().equalsIgnoreCase(versionStr)) return value.value;
        }
        return UN_KNOW.value;
    }
}
