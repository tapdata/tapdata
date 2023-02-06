package com.tapdata.tm.Settings.constant;

import com.tapdata.tm.Settings.service.SettingsService;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @Author: Zed
 * @Date: 2021/9/13
 * @Description: 系统设置枚举
 */
public enum SettingsEnum {
    /** 数据源连接是否可以重复 */
    CONNECTIONS_CREAT_DUPLICATE_SOURCE("Connections","creatDuplicateSource"),
    /** 数据校验参数 */
    INSPECT_SETTING("Inspect","InspectSetting"),
    /** 任务心跳超时时长 单位毫秒 */
    JOB_HEART_TIMEOUT("Job","jobHeartTimeout"),
    /** 任务时差 */
    JOB_LAG_TIME("Job","lagTime"),
    /**  共享增量存储模式 */
    SHARE_CDC_PERSISTENCE_MODE("share_cdc","share_cdc_persistence_mode"),
    /**  共享增量内存缓存行数 */
    SHARE_CDC_PERSISTENCE_MEMORY_SIZE("share_cdc","share_cdc_persistence_memory_size"),
    /**  存储MongoDB的连接名称 */
    SHARE_CDC_PERSISTENCE_MONGODB_URI_DB("share_cdc","share_cdc_persistence_mongodb_uri_db"),
    /** 存储MongoDB的表名 */
    SHARE_CDC_PERSISTENCE_MONGODB_COLLECTION("share_cdc","share_cdc_persistence_mongodb_collection"),
    /** RocksDB存储的本地路径 */
    SHARE_CDC_PERSISTENCE_ROCKSDB_PATH("share_cdc","share_cdc_persistence_rocksdb_path"),
    SHARE_CDC_TTL_DAY("share_cdc","share_cdc_ttl_day"),

    WORKER_HEART_OVERTIME(CategoryEnum.WORKER.getValue(), KeyEnum.WORKER_HEART_TIMEOUT.getValue()),
    SMTP_PASSWORD("SMTP", "smtp.server.password"),

    SCHEDULED_LOAD_SCHEMA_COUNT_THRESHOLD("schema", "scheduled.load.schema.count.threshold")
    ;

    @Getter
    private final String category;
    @Getter
    private final String key;

    SettingsEnum(String category, String key) {
        this.category = category;
        this.key = key;
    }

    public String getValue() {
        return SettingUtil.getValue(this.category, this.key);
    }

    public String getValue(String defualt) {
        String value = SettingUtil.getValue(this.category, this.key);
        if (StringUtils.isBlank(value)) {
            return defualt;
        }
        return value.trim();
    }

    public int getIntValue() {
        return Integer.parseInt(SettingUtil.getValue(this.category, this.key));
    }

    public int getIntValue(int defualt) {
        String value = SettingUtil.getValue(this.category, this.key);
        if (StringUtils.isBlank(value)) {
            return defualt;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (Exception e) {
            return defualt;
        }
    }


    public boolean getBoolValue() {
        return Boolean.parseBoolean(SettingUtil.getValue(this.category, this.key));
    }

    public boolean getBoolValue(boolean defualt) {
        String value = SettingUtil.getValue(this.category, this.key);
        if (StringUtils.isBlank(value)) {
            return defualt;
        }
        return Boolean.parseBoolean(value.trim());
    }

    public double getDoubleValue() {
        return Double.parseDouble(SettingUtil.getValue(this.category, this.key));
    }

    public double getDoubleValue(double defualt) {
        String value = SettingUtil.getValue(this.category, this.key);
        if (StringUtils.isBlank(value)) {
            return defualt;
        }
        return Double.parseDouble(value.trim());
    }
}

@Component
class SettingUtil {
    private static SettingsService settingsService;

    @Autowired
    SettingUtil(SettingsService service) {
        settingsService = service;
    }

    static String getValue(String category, String key) {
        return String.valueOf(settingsService.getByCategoryAndKey(category, key));
    }

}
