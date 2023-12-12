package com.tapdata.tm.schedule.util;

import com.tapdata.tm.Settings.dto.SettingsDto;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.ScheduleTimeEnum;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.metadatainstance.vo.SourceTypeEnum;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Slf4j
@Service
@Setter(onMethod_ = {@Autowired})
public class LoadSchemaScheduleUtil {
    protected static final String CONNECTION_SCHEMA_UPDATE_HOUR = "connection_schema_update_hour";
    protected static final String DEFAULT_UPDATE_HOUR = "2:00";
    protected static final String CONNECTION_SCHEMA_UPDATE_INTERVAL = "connection_schema_update_interval";
    protected static final int DEFAULT_UPDATE_INTERVAL = 1;
    protected static final String[] keys = {CONNECTION_SCHEMA_UPDATE_HOUR, CONNECTION_SCHEMA_UPDATE_INTERVAL};

    public void loadSettings(SettingsService settingsService, Map<String, Object> settingMap) {
        if (null == settingsService || null == settingMap) return;
        List<SettingsDto> settings = settingsService.findALl("1", whereForSettings(keys));
        if (null != settings) {
            settings.stream().filter(Objects::nonNull).forEach(s -> {
                String key = s.getKey();
                switch (key) {
                    case CONNECTION_SCHEMA_UPDATE_HOUR : //unit: hour
                        String updateHour = getStringFromSetting(s, DEFAULT_UPDATE_HOUR);
                        settingMap.put(CONNECTION_SCHEMA_UPDATE_HOUR, getUpdateHour(updateHour, ScheduleTimeEnum.TWO.getValue()));
                        break;
                    case CONNECTION_SCHEMA_UPDATE_INTERVAL: //unit: day
                        int updateInterval = getIntValueFromSetting(s,  DEFAULT_UPDATE_INTERVAL);
                        settingMap.put(CONNECTION_SCHEMA_UPDATE_INTERVAL, updateInterval);
                        break;
                }
            });
        }
    }

    public void doLoadSchema(DataSourceConnectionDto dataSource,
                             Map<String, UserDetail> userDetailMap,
                             MetadataInstancesService metadataInstancesService,
                             Map<String, Object> settingMap,
                             DataSourceService dataSourceService) {
        UserDetail user = userDetailMap.get(dataSource.getUserId());
        Criteria criteria = criteria(dataSource.getId().toHexString());
        long count = metadataInstancesService.count(new Query(criteria));
        int time = ScheduleTimeEnum.getHour(dataSource.getSchemaUpdateHour());
        int interval = (Integer) Optional.ofNullable(settingMap.get(CONNECTION_SCHEMA_UPDATE_INTERVAL)).orElse(DEFAULT_UPDATE_INTERVAL);
        //数据源没有配置，直接使用全局配置; 数据源配置了，直接使用配置的加载频率
        if (time == ScheduleTimeEnum.FALSE.getValue()) {
            return;
        }
        if (time == ScheduleTimeEnum.DEFAULT.getValue()) {
            time = (Integer) Optional.ofNullable(settingMap.get(CONNECTION_SCHEMA_UPDATE_HOUR)).orElse(ScheduleTimeEnum.TWO.getValue());
        }
        loadSchemaOnce(dataSource,
                count,
                interval,
                time,
                user, dataSourceService);
    }

    protected void loadSchemaOnce(DataSourceConnectionDto dataSource, long count, int intervalDay, int time, UserDetail user, DataSourceService service) {
        if (null == dataSource || null == service) return;
        long sleepTime = count / 1000;
        LocalDateTime now = LocalDateTime.now();
        int hour = now.getHour();
        Date loadSchemaTime = dataSource.getLoadSchemaTime();
        long lastLoadTime = 0;
        if (null != loadSchemaTime) {
            lastLoadTime = loadSchemaTime.getTime();
        }
        if (time == hour && now() - lastLoadTime >= intervalDay * 24 * 60 * 60 * 1000) {
            service.sendTestConnection(dataSource, true, true, user);
            //比较大的表，需要sleep一下
            sleep(sleepTime);
        }
    }

    protected int getUpdateHour(String updateHourStr, int defaultValue) {
        String[] hourSplit = null;
        if (null != updateHourStr && updateHourStr.contains(":")) {
            hourSplit = updateHourStr.split(":");
        } else {
            hourSplit = new String[]{updateHourStr};
        }
        try {
            return Integer.parseInt(hourSplit[0]);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    protected void sleep(long sleepTime) {
        try {
            sleepTime = sleepTime > 60 ? 60 : sleepTime;
            sleepMill(sleepTime * 1000);
        } catch (InterruptedException e) {
            //not do anything
        }
    }

    protected void sleepMill(long time) throws InterruptedException {
        Thread.sleep(time);
    }

    protected long now() {
        return System.currentTimeMillis();
    }

    protected int getIntValueFromSetting(SettingsDto settings, int defaultValue) {
        if (null == settings) return defaultValue;
        Object value = settings.getValue();
        if (!(value instanceof Number)) {
            if (value instanceof String) {
                try {
                    return Integer.parseInt((String) value);
                } catch (Exception e) {
                    return defaultValue;
                }
            }
            return defaultValue;
        }
        Number numberValue = (Number)value;
        return numberValue.intValue();
    }

    protected String getStringFromSetting(SettingsDto settings, String defaultValue) {
        if (null == settings) return defaultValue;
        Object value = settings.getValue();
        if (!(value instanceof String)) {
            return defaultValue;
        }
        return (String)value;
    }

    protected Query whereForSettings(String[] keys) {
        if (null == keys || keys.length == 0) return new Query();
        return new Query(Criteria.where("key").in(keys));
    }

    protected Criteria criteria(String dataSourceId) {
        return Criteria.where("is_deleted").ne(true)
                .and("source._id").is(dataSourceId)
                .and("sourceType").is(SourceTypeEnum.SOURCE.name())
                .and("meta_type").ne("database")
                .and("taskId").exists(false);
    }
}
