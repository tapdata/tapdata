package com.tapdata.tm.commons.util;

import io.tapdata.pdk.apis.entity.Capability;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@AllArgsConstructor
@Getter
public enum CapabilityEnum {
    MASTER_SLAVE_MERGE("master_slave_merge", "具备主从合并能力"),
    RESUME_STREAM_BY_TIMESTAMP("resume_stream_by_timestamp", "能根据指定时间恢复增量读取的能力,例如PG没有这个能力supportedDDL的events"),
    ALTER_FIELD_NAME_EVENT("alter_field_name_event", "能输出修改字段名事件"),
    ALTER_FIELD_DATATYPE_EVENT("alter_field_datatype_event", "能输出修改字段类型事件"),
    ALTER_FIELD_CHECK_EVENT("alter_field_check_event", "能输出修改字段检查事件"),
    CREATE_TABLE_EVENT("create_table_event", "能输出建表事件"),
    DROP_TABLE_EVENT("drop_table_event", "能输出删表事件"),
    ALTER_PRIMARY_KEY_EVENT("alter_primary_key_event", "能输出修改主键事件"),
    ALTER_FIELD_CONSTRAINT_EVENT("alter_field_constraint_event", "能输出修改constraint事件"),
    DROP_FIELD_EVENT("drop_field_event", "能输出删除字段事件"),
    ALTER_FIELD_NOT_NULL_EVENT("alter_field_not_null_event", "能输出修改not null事件"),
    ALTER_FIELD_COMMENT_EVENT("alter_field_comment_event", "能输出修改注释事件"),
    NEW_FIELD_EVENT("new_field_event", "能输出新增字段事件"),
    ALTER_TABLE_CHARSET_EVENT("alter_table_charset_event", "能输出修改数据库字符集事件"),
    ALTER_FIELD_DEFAULT_EVENT("alter_field_default_event", "能输出修改默认值事件"),
    ALTER_DATABASE_TIMEZONE_EVENT("alter_database_timezone_event", "能输出修改数据库时区事件"),

    RELEASE_EXTERNAL_FUNCTION("release_external_function", "支持释放外部资源"),
    BATCH_READ_FUNCTION("batch_read_function", "支持全量"),
    STREAM_READ_FUNCTION("stream_read_function", "支持增量"),
    BATCH_COUNT_FUNCTION("batch_count_function", "支持获取全量数据总量"),
    TIMESTAMP_TO_STREAM_OFFSET_FUNCTION("timestamp_to_stream_offset_function", "支持通过事件恢复增量断点能力"),
    WRITE_RECORD_FUNCTION("write_record_function", "支持写入数据"),
    QUERY_BY_FILTER_FUNCTION("query_by_filter_function", "支持简单查询"),
    QUERY_BY_ADVANCE_FILTER_FUNCTION("query_by_advance_filter_function", "支持高级查询"),
    CREATE_TABLE_FUNCTION("create_table_function", "支持建表 "),
    CREATE_TABLE_V2_FUNCTION("create_table_v2_function", "支持建表2"),
    CLEAR_TABLE_FUNCTION("clear_table_function", "支持清理数据"),
    DROP_TABLE_FUNCTION("drop_table_function", "支持删表");

    private final String id;

    private final String description;

    public boolean isCapability(Capability capability) {
        return getId().equals(capability.getId());
    }

    public boolean isCapability(List<Capability> capabilities) {
        if (null != capabilities) {
            for (Capability capability : capabilities) {
                if (isCapability(capability)) return true;
            }
        }
        return false;
    }
}
