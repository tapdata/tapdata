package com.tapdata.tm.commons.schema;

import org.apache.commons.lang3.StringUtils;

/**
 * @Author: Zed
 * @Date: 2021/9/9
 * @Description:
 */
public enum DataSourceEnum {
    mongodb,
    aliyun_mongodb,
    tencent_mongodb,
    gridfs,
    kafka,
    mq,
    dummy;

    public static boolean isMongoDB(String databaseType) {
        return StringUtils.equalsAnyIgnoreCase(databaseType, mongodb.name(), aliyun_mongodb.name(),tencent_mongodb.name());
    }

    public static boolean isGridFs(String databaseType) {
        return StringUtils.equalsAnyIgnoreCase(databaseType, gridfs.name());
    }

    public static boolean isMetaTypeCollection(String databaseType) {
        return StringUtils.equalsAnyIgnoreCase(databaseType,
                mongodb.name(), aliyun_mongodb.name(),tencent_mongodb.name(), kafka.name(), mq.name(), "dummy db"
        );
    }
}
