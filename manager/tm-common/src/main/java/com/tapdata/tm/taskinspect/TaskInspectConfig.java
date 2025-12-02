package com.tapdata.tm.taskinspect;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.tapdata.tm.taskinspect.config.*;
import com.tapdata.tm.taskinspect.cons.TimeCheckModeEnum;
import com.tapdata.tm.taskinspect.config.TableFilter;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Optional;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/12 16:07 Create
 */
@Getter
@Setter
public class TaskInspectConfig implements IConfig<TaskInspectConfig> {
    public static final String FIELD_ENABLE = "enable";
    public static final String FIELD_MODE = "mode";
    public static final String FIELD_INTELLIGENT = "intelligent";
    public static final String FIELD_CUSTOM = "custom";
    public static final String FIELD_QUEUE_CAPACITY = "queueCapacity";
    public static final String FIELD_CDC_TIMEOUT = "cdcTimeout";
    public static final String FIELD_CHECK_NO_PK_TABLE = "checkNoPkTable";
    public static final String FIELD_TIME_CHECK_MODE = "timeCheckMode";
    public static final String FIELD_TABLE_FILTER = "tableFilter";

    private Boolean enable;                  // 是否开启校验
    private TaskInspectMode mode;            // 校验模式
    private Intelligent intelligent;         // 智能校验配置
    private Custom custom;                   // 自定义校验配置
    private Integer queueCapacity;           // 队列最大容量
    private Long cdcTimeout;                 // 增量校验在队列中超时，不作延迟等待
    private Boolean checkNoPkTable;          // 校验无主键表
    private TimeCheckModeEnum timeCheckMode; // 时间校验模式
    private TableFilter tableFilter;         // 表过滤

    @Override
    public TaskInspectConfig init(int depth) {
        setEnable(init(getEnable(), false));
        setMode(init(getMode(), TaskInspectMode.CLOSE));
        setCustom(init(getCustom(), depth, Custom.class));
        setIntelligent(init(getIntelligent(), depth, Intelligent.class));
        setCdcTimeout(init(getCdcTimeout(), 60 * 1000L));
        setCheckNoPkTable(init(getCheckNoPkTable(), false));
        setTimeCheckMode(init(getTimeCheckMode(), TimeCheckModeEnum.NORMAL));
        setTableFilter(init(getTableFilter(), depth, TableFilter.class));

        // 设置队列容量，默认为：3 倍 CDC 采样数量
        Integer queueCapacity = getQueueCapacity();
        if (null == queueCapacity) {
            if (TaskInspectMode.CUSTOM == getMode()) {
                queueCapacity = Optional.of(getCustom())
                    .map(Custom::getCdc)
                    .map(CustomCdc::getSample)
                    .map(CustomCdcSample::getLimit)
                    .map(capacity -> capacity * 3)
                    .orElse(null);
            }
        }
        setQueueCapacity(init(queueCapacity, 1000));
        return this;
    }

    public Update toUpdateUnsetWithNullValue() {
        Update update = new Update();
        update = set(update, FIELD_ENABLE, getEnable());
        update = set(update, FIELD_MODE, getMode());
        update = set(update, FIELD_INTELLIGENT, getIntelligent());
        update = set(update, FIELD_CUSTOM, getCustom());
        update = set(update, FIELD_QUEUE_CAPACITY, getQueueCapacity());
        update = set(update, FIELD_CDC_TIMEOUT, getCdcTimeout());
        update = set(update, FIELD_CHECK_NO_PK_TABLE, getCheckNoPkTable());
        update = set(update, FIELD_TIME_CHECK_MODE, getTimeCheckMode());
        update = set(update, FIELD_TABLE_FILTER, getTableFilter());
        return update;
    }

    private Update set(Update update, String field, Object value) {
        if (null == value) {
            return update.unset(field);
        }
        return update.set(field, value);
    }

    public static TaskInspectConfig createClose() {
        return new TaskInspectConfig().init(-1);
    }

    public static void main(String[] args) {
        // 打印配置
        TaskInspectConfig config = new TaskInspectConfig();
        config.init(-1);
        System.out.println(JSON.toJSONString(config, SerializerFeature.PrettyFormat));
        System.out.println(config.toUpdateUnsetWithNullValue());
    }
}
