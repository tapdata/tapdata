//
//package com.tapdata.tm.task.bean;
//
//import java.util.List;
//import java.util.Map;
//import lombok.Data;
//
//@Data
//public class Node {
//
//    /** 数据 */
//    public static String TYPE_DATABASE = "database";
//    /** 字段处理器 */
//    public static String TYPE_FIELD_PROCESSOR = "field_processor";
//    /** JS处理节点 */
//    public static String TYPE_JS_PROCESSOR = "js_processor";
//    /** 行过滤器 */
//    public static String TYPE_ROW_FILTER_PROCESSOR = "row_filter_processor";
//    /** 聚合 */
//    public static String TYPE_AGGREGATION_PROCESSOR = "aggregation_processor";
//    /** 连接处理节点 */
//    public static String TYPE_JOIN = "join";
//    /** 关联缓存处理节点 */
//    public static String TYPE_CACHE_LOOKUP_PROCESSOR = "cache_lookup_processor";
//
//
//    private String action;
//    private String name;
//
//    private List<Aggregation> aggregations;
//
//    private Map<String, Object> attrs;
//
//    private String cacheId;
//
//    private String connectionId;
//
//    private String databaseType;
//
//    /**将关联表内嵌到主表 */
//    private Boolean embeddedMode;
//    private EmbeddedSetting embeddedSetting;
//
//    private String existDataProcessMode;
//
//    private String expression;
//
//    private String id;
//
//    private String increasePoll;
//
//    private Long increaseReadSize;
//
//    private Long increaseSyncInterval;
//
//    private String joinExpression;
//
//    private String joinKey;
//
//    private List<JoinSetting> joinSettings;
//
//    private String joinType;
//
//    private Long maxTransactionDuration;
//
//    private List<Object> operations;
//
//    private String primaryKeys;
//
//    private Long processorThreadNum;
//
//    private String schema;
//
//    private String script;
//
//    private List<Object> scripts;
//
//    private String sql;
//
//    private String tableName;
//
//    private String totalReadMethod;
//
//    private String type;
//
//    private String whereExpression;
//
//    private String writeStrategy;
//}
