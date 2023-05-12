package com.tapdata.tm.commons.task.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.commons.base.convert.DagDeserialize;
import com.tapdata.tm.commons.base.convert.DagSerialize;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleVO;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingVO;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Task
 */
@Data
public class TaskDto extends ParentTaskDto {
    /** migrate迁移  logColleshareCachector 挖掘任务*/
    public static final String SYNC_TYPE_SYNC = "sync";
    public static final String SYNC_TYPE_MIGRATE = "migrate";
    public static final String SYNC_TYPE_LOG_COLLECTOR = "logCollector";
    public static final String SYNC_TYPE_CONN_HEARTBEAT = "connHeartbeat";
    /**
     * 试运行
     */
    public static final String SYNC_TYPE_TEST_RUN = "testRun";
    /**
     * 模型推演
     */
    public static final String SYNC_TYPE_DEDUCE_SCHEMA = "deduceSchema";

    public static final String LASTTASKRECORDID = "taskRecordId";

    public static final String PING_TIME_FIELD = "pingTime";

    public static final String LDP_TYPE_FDM = "fdm";
    public static final String LDP_TYPE_MDM = "mdm";

    public static final String ATTRS_USED_SHARE_CACHE = "usedShareCache";
    public static final String ATTRS_SKIP_ERROR_EVENT = "skipErrorEvent";

    /** 任务图*/
    @JsonSerialize( using = DagSerialize.class)
    @JsonDeserialize( using = DagDeserialize.class)
    private DAG dag;

    //@JsonSerialize( using = DagSerialize.class)
    //@JsonDeserialize( using = DagDeserialize.class)
    //private DAG temp;
    /**
     * cacheName 缓存名称
     * cacheKeys  字符串 ，用逗号隔开
     * maxRows 最大记录数
     * ttl 过期时间 秒为单位
     */
    private Boolean shareCache=false;

    // 需要根据数据源是否支持 数据校验功能来判断
    private Boolean canOpenInspect;
    //是否开启数据校验
    private Boolean isAutoInspect;
    public boolean isAutoInspect() {
        return Boolean.TRUE.equals(isAutoInspect);
    }

    private SkipErrorEvent skipErrorEvent;

    private String creator;

    private boolean showInspectTips;

    private String inspectId;

    private Map<String, Object> logSetting;

    /** 编辑中 */
    public static final String STATUS_EDIT = "edit";
    /** 调度中 */
    public static final String STATUS_SCHEDULING = "scheduling";
    /** 调度失败 */
    public static final String STATUS_SCHEDULE_FAILED = "schedule_failed";
    /** 待运行 */
    public static final String STATUS_WAIT_RUN = "wait_run";
    /** 运行中 */
    public static final String STATUS_RUNNING = "running";
    /** 停止中 */
    public static final String STATUS_STOPPING = "stopping";
    /** 错误 */
    public static final String STATUS_ERROR = "error";
    /** 完成 */
    public static final String STATUS_COMPLETE = "complete";
    /** 已停止 */
    public static final String STATUS_STOP = "stop";

    /** 待启动 */
    public static final String STATUS_WAIT_START = "wait_start";

    /** 重置中 */
    public static final String STATUS_RENEWING = "renewing";
    /** 删除中*/
    public static final String STATUS_DELETING = "deleting";
    /** 重置失败 */
    public static final String STATUS_RENEW_FAILED = "renew_failed";
    /** 删除失败 */
    public static final String STATUS_DELETE_FAILED = "delete_failed";


    @JsonSerialize( using = DagSerialize.class)
    @JsonDeserialize( using = DagDeserialize.class)
    private DAG tempDag;

    //用户对接pdk重置删除的标记
    private Boolean resetFlag;
    private Boolean deleteFlag;
    private Long version;

    private String taskRecordId;

    private List<AlarmSettingVO> alarmSettings;
    private List<AlarmRuleVO> alarmRules;

    private Integer resetTimes;

    private Long currentEventTimestamp;
    private Long snapshotDoneAt;

    private Long scheduleDate;
    private Long stopedDate;

    /**
     * js 试运行id
     */
    private String testTaskId;
    /**
     * js模型推演id
     */
    private String transformTaskId;

    private int stopRetryTimes;

    private boolean isSnapShotInterrupt;

    /** ldp 类型， fdm, mdm   为空或者其他为其他任务*/
    private String ldpType;

    /** ldp需要新增的表名列表 */
    private List<String> ldpNewTables;

    public DAG getDag() {
        if (dag != null) {
            dag.setTaskId(getId());
            dag.setOwnerId(getUserId());
            dag.setSyncType(getSyncType());
        }
        return dag;
    }

    private transient Map<String, Object> taskInfo;

    public Object taskInfo(String key){
        if (null == taskInfo) return null;
        return taskInfo.get(key);
    }

    public TaskDto taskInfo(String key, Object value){
        if (null == key) return this;
        if (null == taskInfo) taskInfo = new ConcurrentHashMap<>();
        taskInfo.put(key, value);
        return this;
    }

    public boolean isTestTask() {
        return StringUtils.equalsAnyIgnoreCase(getSyncType(), SYNC_TYPE_TEST_RUN, SYNC_TYPE_DEDUCE_SCHEMA);
    }

    public boolean isNormalTask() {
			return !isTestTask();
		}

    @Data
    public static class SyncPoint implements Serializable {
        @EqField
        private String nodeId;
        private String nodeName;
        /** 数据源id */
        private String connectionId;
        /** 数据源名称 */
        private String connectionName;
        /** 时间点类型  当前：current  浏览器时区 */
        @EqField
        private String pointType;
        /** 时区 */
        @EqField
        private String timeZone;

        /** 时间 */
        @EqField
        private Long dateTime;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o instanceof TaskDto) {
                Class className = TaskDto.class;
                for (; className != Object.class; className = className.getSuperclass()) {
                    java.lang.reflect.Field[] declaredFields = className.getDeclaredFields();
                    for (java.lang.reflect.Field declaredField : declaredFields) {
                        EqField annotation = declaredField.getAnnotation(EqField.class);
                        if (annotation != null) {
                            try {
                                Object f2 = declaredField.get(o);
                                Object f1 = declaredField.get(this);
                                boolean b = Node.fieldEq(f1, f2);
                                if (!b) {
                                    return false;
                                }
                            } catch (IllegalAccessException e) {
                            }
                        }
                    }
                }
                return true;
            }
            return false;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof TaskDto) {
            Class className = TaskDto.class;
            for (; className != Object.class; className = className.getSuperclass()) {
                java.lang.reflect.Field[] declaredFields = className.getDeclaredFields();
                for (java.lang.reflect.Field declaredField : declaredFields) {
                    EqField annotation = declaredField.getAnnotation(EqField.class);
                    if (annotation != null) {
                        try {
                            Object f2 = declaredField.get(o);
                            Object f1 = declaredField.get(this);
                            boolean b = Node.fieldEq(f1, f2);
                            if (!b) {
                                return false;
                            }
                        } catch (IllegalAccessException e) {
                        }
                    }
                }
            }
            return true;
        }
        return false;
    }

    /**
     * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
     * @version v1.0 2023/4/14 15:25 Create
     */
    public static class SkipErrorEvent implements Serializable {
        public enum ErrorMode {
            Disable,
            SkipTable,
            SkipData,
            ;
        }

        public enum LimitMode {
            Disable,
            SkipByRate,
            SkipByLimit,
            ;
        }

        private String errorMode;
        private String limitMode;
        private Long limit;
        private Integer rate;

        public SkipErrorEvent() {
        }

        public String getErrorMode() {
            return errorMode;
        }

        public ErrorMode getErrorModeEnum() {
            try {
                if (null == this.errorMode) {
                    return null;
                }
                return ErrorMode.valueOf(this.errorMode);
            } catch (IllegalArgumentException e) {
                return ErrorMode.Disable;
            }
        }

        public String getLimitMode() {
            return limitMode;
        }

        public LimitMode getLimitModeEnum() {
            try {
                if (null == this.limitMode) {
                    return null;
                }
                return LimitMode.valueOf(this.limitMode);
            } catch (IllegalArgumentException e) {
                return LimitMode.Disable;
            }
        }

        public void setErrorMode(String errorMode) {
            this.errorMode = errorMode;
        }

        public void setErrorMode(ErrorMode mode) {
            if (null == mode) {
                this.errorMode = null;
            } else {
                this.errorMode = mode.name();
            }
        }

        public void setLimitMode(String mode) {
            this.limitMode = mode;
        }

        public void setLimitMode(LimitMode mode) {
            if (null == mode) {
                this.limitMode = null;
            } else {
                this.limitMode = mode.name();
            }
        }

        public Long getLimit() {
            return limit;
        }

        public void setLimit(Long limit) {
            this.limit = limit;
        }

        public Integer getRate() {
            return rate;
        }

        public void setRate(Integer rate) {
            this.rate = rate;
        }
    }
}
