package com.tapdata.tm.group.constant;

import java.util.Arrays;
import java.util.List;

/**
 * 分组管理模块统一常量
 *
 */
public final class GroupConstants {

    private GroupConstants() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    /**
     * 元数据实例集合名称
     */
    public static final String COLLECTION_METADATA_INSTANCES = "MetadataInstances";

    /**
     * 任务集合名称
     */
    public static final String COLLECTION_TASK = "Task";

    /**
     * 连接集合名称
     */
    public static final String COLLECTION_CONNECTION = "Connections";

    /**
     * 连接Excel文件名称
     */
    public static final String COLLECTION_CONNECTION_EXCEL = "Connections.xlsx";

    /**
     * 模块集合名称
     */
    public static final String COLLECTION_MODULES = "Modules";

    /**
     * 分组信息集合名称
     */
    public static final String COLLECTION_GROUP_INFO = "GroupInfo";

    /**
     * 校验任务集合名称
     */
    public static final String COLLECTION_INSPECT = "Inspect";

    /**
     * 标签集合名称
     */
    public static final String METADATA_DEFINITION = "MetadataDefinition";

    // ==================== 敏感字段列表 ====================

    /**
     * 需要脱敏的连接配置字段
     */
    public static final List<String> MASK_PROPERTIES = Arrays.asList(
            "host", "uri", "database", "schema", "sid",
            "masterSlaveAddress", "sentinelAddress",
            "mqQueueString", "mqTopicString", "brokerURL",
            "mqUsername", "mqPassword", "nameSrvAddr",
            "ftpHost", "ftpUsername", "ftpPassword",
            "rawLogServerHost", "databaseName",
            "username", "user", "password", "sslPass");

    // ==================== 文件扩展名常量 ====================

    /**
     * 导出文件扩展名
     */
    public static final String EXPORT_FILE_EXTENSION = ".tar";

    /**
     * 分组批量导出文件名前缀
     */
    public static final String BATCH_EXPORT_FILE_PREFIX = "group_batch";


    /**
     * 导入导出记录类型 - 导出
     */
    public static final String RECORD_TYPE_EXPORT = "export";

    /**
     * 导入导出记录类型 - 导入
     */
    public static final String RECORD_TYPE_IMPORT = "import";

    /**
     * 记录状态 - 导出中
     */
    public static final String RECORD_STATUS_EXPORTING = "exporting";

    /**
     * 记录状态 - 导入中
     */
    public static final String RECORD_STATUS_IMPORTING = "importing";

    /**
     * 记录状态 - 已完成
     */
    public static final String RECORD_STATUS_COMPLETED = "completed";

    /**
     * 记录状态 - 失败
     */
    public static final String RECORD_STATUS_FAILED = "failed";

    /**
     * 重复资源标记
     */
    public static final String DUPLICATE_MARKER = "duplicate";

    /**
     * 备份名称分隔符
     */
    public static final String BACKUP_NAME_SEPARATOR = "_backup_";
}
