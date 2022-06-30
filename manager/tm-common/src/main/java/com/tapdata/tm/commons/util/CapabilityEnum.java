package com.tapdata.tm.commons.util;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum CapabilityEnum {
    MASTER_SLAVE_MERGE("master-slave-merge", "具备主从合并能力"),
    RESUME_STREAM_BY_TIMESTAMP("resume-stream-by-timestamp", "能根据指定时间恢复增量读取的能力,例如PG没有这个能力supportedDDL的events"),
    ALTER_FIELD_NAME_EVENT("alter-field-name-event", "能输出修改字段名事件"),
    ALTER_FIELD_DATATYPE_EVENT("alter-field-datatype-event", "能输出修改字段类型事件"),
    ALTER_FIELD_CHECK_EVENT("alter-field-check-event", "能输出修改字段检查事件"),
    CREATE_TABLE_EVENT("create-table-event", "能输出建表事件"),
    DROP_TABLE_EVENT ("drop-table-event", "能输出删表事件"),
    ALTER_PRIMARY_KEY_EVENT("alter-primary-key-event", "能输出修改主键事件"),
    ALTER_FIELD_CONSTRAINT_EVENT("alter-field-constraint-event", "能输出修改constraint事件"),
    DROP_FIELD_EVENT("drop-field-event", "能输出删除字段事件"),
    ALTER_FIELD_NOT_NULL_EVENT("alter-field-not-null-event", "能输出修改not null事件"),
    ALTER_FIELD_COMMENT_EVENT("alter-field-comment-event", "能输出修改注释事件"),
    NEW_FIELD_EVENT("new-field-event", "能输出新增字段事件"),
    ALTER_TABLE_CHARSET_EVENT("alter-table-charset-event", "能输出修改数据库字符集事件"),
    ALTER_FIELD_DEFAULT_EVENT("alter-field-default-event", "能输出修改默认值事件"),
    ALTER_DATABASE_TIMEZONE_EVENT("alter-database-timezone-event", "能输出修改数据库时区事件"),

    RELEASE_EXTERNAL_FUNCTION("release-external-function", "支持释放外部资源"),
    BATCH_READ_FUNCTION("batch-read-function", "支持全量"),
    STREAM_READ_FUNCTION("stream-read-function", "支持增量"),
    BATCH_COUNT_FUNCTION("batch-count-function", "支持获取全量数据总量"),
    TIMESTAMP_TO_STREAM_OFFSET_FUNCTION("timestamp-to-stream-offset-function", "支持通过事件恢复增量断点能力"),
    WRITE_RECORD_FUNCTION("write-record-function", "支持写入数据"),
    QUERY_BY_FILTER_FUNCTION("query-by-filter-function", "支持简单查询"),
    QUERY_BY_ADVANCE_FILTER_FUNCTION("query-by-advance-filter-function", "支持高级查询"),
    CREATE_TABLE_FUNCTION("create-table-function", "支持建表 "),
    CLEAR_TABLE_FUNCTION("clear-table-function", "支持清理数据"),
    DROP_TABLE_FUNCTION("drop-table-function", "支持删表");

    private final String event;

    private final String description;
}
