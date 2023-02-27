package com.tapdata.tm.task.constant;

public class DagOutputTemplate {
    /**
     * Agent可用性检测：
     * 【检测级别】+检测时间+【任务名】+【检测项+：检测项名称{agent名，支持检测多个}检测详情内容+检测结果
     * Agent可用：
     * 【INFO】 2022-05-24 21:05:22 【任务A】【Agent可用性检测】：检查到当前有3个可用Agent，当前任务将在Agent：{sharecdc-tapdaas-tapdaas-7c9b6ff48f}上运行
     * Agent不可用：
     * 【ERROR】 2022-05-24 21:05:22  【任务A】【Agent可用性检测】：当前无可用Agent，任务运行失败
     */
    public static String AGENT_CAN_USE_INFO = "$date【$taskName】【Agent可用性检测】：检查到当前有{1}个可用Agent：[{2}],当前任务将在Agent：{3}上运行";
    public static String AGENT_CAN_USE_ERROR = "$date【$taskName】【Agent可用性检测】：当前指定的Agent：{1}不可用，请尽快恢复，或者重新指定其它Agent。";

    /**
     * 任务设置检测
     * 【检测级别】+检测时间+【任务名】+【检测项】+：检测项名称{任务名}检测内容+检测结果
     * 任务检测通过：
     * 【INFO】 2022-05-24 21:05:22  【任务A】【任务设置检测】：任务{MySQL_test}检测通过
     * 任务检测未通过：
     * 【ERROR】 2022-05-24 21:05:22  【任务A】【任务设置检测】：任务{MySQL_test}检测未通过，异常项{任务名称}，异常原因：任务名称重复，请重新设置
     */
    public static String TASK_SETTING_INFO = "$date【$taskName】【任务设置检测】：任务'{'$taskName}检测通过";
    public static String TASK_SETTING_ERROR = "$date【$taskName】【任务设置检测】：任务'{'$taskName}检测未通过，异常项'{'任务名称}，异常原因：任务名称重复，请重新设置";

    /**
     * 源节点设置检测：
     * 【检测级别】+检测时间+【任务名】+【检测项】+：检测项名称{节点名}检测内容+检测结果
     * 源节点检测通过：
     * 【INFO】 2022-05-24 21:05:22  【任务A】【源节点设置检测】：节点{MySQL_01}检测通过
     * 源节点检测未通过：
     * 【ERROR】 2022-05-24 21:05:22  【任务A】【源节点设置检测】：节点{MySQL_01}检测未通过，异常项{节点名称}，异常原因：节点名称重复，请重新设置
     */
    public static String SOURCE_SETTING_INFO = "$date【$taskName】【源节点设置检测】：节点{0}检测通过";
    public static String SOURCE_SETTING_ERROR = "$date【$taskName】【源节点设置检测】：节点{0}检测未通过，异常项{节点名称}，异常原因：节点名称重复，请重新设置";
    public static String SOURCE_SETTING_ERROR_SCHEMA = "$date【$taskName】【源节点设置检测】：节点{0}检测未通过，源库没有表";
    public static String SOURCE_SETTING_ERROR_SCHEMA_LOAD = "$date【$taskName】【源节点设置检测】：节点{0}检测未通过，连接模型未加载完成";

    /**
     * JS节点设置检测：（没有可跳过检测）
     * 【检测级别】+检测时间+【任务名】+【检测项】+：检测项名称{节点名}检测内容+检测结果
     * JS节点检测通过：
     * 【INFO】 2022-05-24 21:05:22 【任务A】 【JS节点设置检测】：节点{JS节点001}检测通过
     * JS节点检测未通过：
     * 【ERROR】 2022-05-24 21:05:22 【任务A】 【JS节点设置检测】：节点{JS节点001}检测未通过，异常项属性：{脚本}，异常原因：设置有误，请重新设置
     */
    public static String JS_NODE_INFO = "$date【$taskName】【JS节点设置检测】：节点{1}检测通过";
    public static String JS_NODE_ERROR = "$date【$taskName】【JS节点设置检测】：节点{1}检测未通过，异常项属性：{脚本}，异常原因：设置有误，请重新设置";

    /**
     * 表编辑节点设置检测：（没有可跳过检测）
     * 【检测级别】+检测时间+【任务名】+【检测项】+：检测项名称{节点名}检测内容+检测结果
     * 表编辑节点检测通过：
     * 【INFO】 2022-05-24 21:05:22  【任务A】【表编辑节点设置检测】：节点{表编辑节点001}检测通过
     * 表编辑节点检测未通过：
     * 【ERROR】 2022-05-24 21:05:22 【任务A】 【表编辑节点设置检测】：节点{表编辑节点001}检测未通过，异常项属性：{前缀}，异常原因：设置有误，请重新设置
     */
    public static String TABLE_EDIT_NODE_INFO = "$date【$taskName】【表编辑节点设置检测】：节点{1}检测通过";
    public static String TABLE_EDIT_NODE_ERROR = "$date【$taskName】【表编辑节点设置检测】：节点{1}检测未通过，异常项属性：{前缀}，异常原因：设置有误，请重新设置";

    /**
     * 字段编辑节点设置检测：（没有可跳过检测）
     * 【检测级别】+检测时间+【任务名】+【检测项】+：检测项名称{节点名}检测内容+检测结果
     * 字段编辑节点检测通过：
     * 【INFO】 2022-05-24 21:05:22  【任务A】【字段编辑节点设置检测】：节点{字段编辑节点001}检测通过
     * 字段编辑节点检测未通过：
     * 【ERROR】 2022-05-24 21:05:22  【任务A】【字段编辑节点设置检测】：节点{字段编辑节点001}检测未通过，异常项属性：{后缀}，异常原因：设置有误，请重新设置
     */
    public static String FIELD_EDIT_NODE_INFO = "$date【$taskName】【字段编辑节点设置检测】：节点{1}检测通过";
    public static String FIELD_EDIT_NODE_ERROR = "$date【$taskName】【字段编辑节点设置检测】：节点{1}检测未通过，异常项属性：{后缀}，异常原因：设置有误，请重新设置";

    /**
     * 目标节点设置检测：
     * 【检测级别】+检测时间+【任务名】+【检测项】+：检测项名称{节点名}检测内容+检测结果
     * 目标节点检测通过：
     * 【INFO】 2022-05-24 21:05:22 【任务A】 【目标节点设置检测】：节点{MySQL_01}检测通过
     * 目标节点检测未通过：
     * 【ERROR】 2022-05-24 21:05:22  【任务A】【目标节点设置检测】：节点{MySQL_01}检测未通过，异常项属性：{节点名称}，异常原因：节点名称重复，请重新设置
     */
    public static String TARGET_NODE_INFO = "$date【$taskName】【目标节点设置检测】：节点{1}检测通过";
    public static String TARGET_NODE_ERROR = "$date【$taskName】【目标节点设置检测】：节点{1}检测未通过，异常项属性：{节点名称}，异常原因：节点名称重复，请重新设置";

    /**
     * 源连接检测：
     * 【检测级别】+检测时间+【任务名】+【检测项】+：检测项名称{连接名}检测内容+检测结果
     * 源连接检测通过：
     * 【INFO】 2022-05-24 21:05:22  【任务A】【源连接检测】：连接{"connectorId":
     * "626bc1bcf3ed334254305087","connectorType":"mysql","connectorName":"MySQL001"dbName":"students","schema":"students","schemaLoadStatus":"100%","connectDelay":"10ms","status":"ready","messages":"“MySQL_source”}检测通过
     * 源连接检测未通过：
     * 【ERROR】 2022-05-24 21:05:22  【任务A】【源连接检测】：连接{"connectorId":
     * "626bc1bcf3ed334254305087","connectorType":"mysql","connectorName":"MySQL001","dbName":"students","schema":"students","schemaLoadStatus":"100%","connectDelay":"10ms","status":"not ready","messages":"CDC权限校验不通过，无法进行增量同步"}，
     */
    public static String SOURCE_CONNECT_INFO = "$date【$taskName】【源连接检测】：连接{1}检测通过";
    public static String SOURCE_CONNECT_ERROR = "$date【$taskName】【源连接检测】：连接{1}检测未通过";

    /**
     * 目标连接检测
     */
    public static String TARGET_CONNECT_INFO = "$date【$taskName】【目标连接检测】：连接{1}检测通过";
    public static String TARGET_CONNECT_ERROR = "$date【$taskName】【目标连接检测】：连接{1}检测未通过";

    /**
     * 字符编码检测：
     * 【检测级别】+检测时间+【任务名】+【检测项】+检测内容+检测结果
     * 字符编码检测通过：
     * 【INFO】 2022-05-24 21:05:22  【任务A】【字符编码检测】：源库和目标库字符编码一致，均为UTF-8，检测正常
     * 字符编码检测未通过：
     * 【WARN】 2022-05-24 21:05:22  【任务A】【字符编码检测】：检测异常，异常项{编码不一致}，异常原因：目标库为GBK，源和目标编码不一致，可能会出现乱码
     */
    public static String CHARACTER_ENCODING_INFO = "$date【$taskName】【字符编码检测】：源库和目标库字符编码一致，均为UTF-8，检测正常";
    public static String CHARACTER_ENCODING_WARN = "$date【$taskName】【字符编码检测】：检测异常，异常项{编码不一致}，异常原因：目标库为GBK，源和目标编码不一致，可能会出现乱码";

    /**
     * 表名大小写检测：
     * 【检测级别】+检测时间+【任务名】+【检测项】+：检测内容+检测结果
     * 表名大小写检测通过：
     * 【INFO】 2022-05-24 21:05:22  【任务A】【表名大小写检测】： 源库表名和目标库表名大小写设置一致，检测正常
     * 表名大小写检测未通过：
     * 【WARN】 2022-05-24 21:05:22  【任务A】【表名大小写检测】：检测异常，异常原因：源库表名为大写，目标库表名默认为小写，可能会影响到任务同步
     */
    public static String TABLE_NAME_CASE_INFO = "$date【$taskName】【表名大小写检测】： 源库表名和目标库表名大小写设置一致，检测正常";
    public static String TABLE_NAME_CASE_ERROR = "$date【$taskName】【表名大小写检测】：检测异常，异常原因：源库表名为大写，目标库表名默认为小写，可能会影响到任务同步";

    /**
     * 模型推演检测：（多表时每100张打印一条日志）
     * 【检测级别】+检测时间+【任务名】+【检测项】+检测内容+检测结果
     * 模型推演检测通过：
     * 【INFO】 2022-05-24 21:05:22  【任务A】【模型推演检测】：推演进度100/1000，检测正常
     * 模型推演检测未通过：
     * 【WARN】 2022-05-24 21:05:22  【任务A】【模型推演检测】：表名{表名A}检测异常，异常原因：字段推演异常
     */
    public static String MODEL_PROCESS_INFO = "$date【$taskName】【模型推演检测】：推演进度{1}/{2}，检测正常";
    public static String MODEL_PROCESS_INFO_PRELOG = "$date【$taskName】【模型推演检测】：目标表结构生成中, 需要生成的表数量: {0}, 大约需要 {1}s, 请等待";
    public static String MODEL_PROCESS_ERROR = "$date【$taskName】【模型推演检测】：检测异常，异常原因：{1}";

    /**
     * 数据校验检测：（若任务中有处理节点则不进行校验）
     * 【检测级别】+检测时间+【任务名】+【检测项】+：检测内容+检测结果
     * 数据校验检测结果：
     * 【INFO】 2022-05-24 21:05:22  【任务A】【节点名称】【数据校验检测】：支持数据校验，其中支持校验表数量为20张，不支持校验表数量为0张
     * 【INFO】 2022-05-24 21:05:22  【任务A】【数据校验检测】：该连接不支持数据校验
     */
    public static String DATA_INSPECT_INFO = "$date【$taskName】【数据校验检测】：支持数据校验，其中支持校验表数量为{2}张，不支持校验表数量为{3}张";
    public static String DATA_INSPECT_ERROR = "$date【$taskName】【数据校验检测】：{2}";
}
