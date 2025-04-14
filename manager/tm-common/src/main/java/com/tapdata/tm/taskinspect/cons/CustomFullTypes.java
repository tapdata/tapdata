package com.tapdata.tm.taskinspect.cons;

/**
 * 全量自定义模式-校验类型
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/1/17 15:24 Create
 */
public enum CustomFullTypes {
    HASH, // 全量哈希
    COUNT, // 全量统计
    FIELDS_ALL, // 全字段校验所有数据
    FIELDS_SAMPLE_PERCENT, // 全字段校验-百分比抽样
    FIELDS_SAMPLE_LIMIT, // 全字段校验-上限条数
    ;
}
