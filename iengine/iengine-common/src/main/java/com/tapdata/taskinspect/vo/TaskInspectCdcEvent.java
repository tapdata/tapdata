package com.tapdata.taskinspect.vo;

import com.tapdata.constant.MD5Util;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.LinkedHashMap;

/**
 * 任务校验-增量事件
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/9 20:58 Create
 */
@Getter
@Setter
public class TaskInspectCdcEvent implements Serializable {
    private Long referenceTime; // 接收时间
    private Long time; // 事件时间
    private String tableName; // 表名
    private String rowId; // 数据行编号
    private LinkedHashMap<String, Object> keys; // 主键值

    public TaskInspectCdcEvent putKey(String k, Object v) {
        if (null == keys) {
            setKeys(new LinkedHashMap<>());
        }
        keys.put(k, v);
        return this;
    }

    /**
     * 初始化行数据编号（可能存在多行数据共用一个ID，如有需要绝对唯一，则修改算法）
     * <p>
     * 本方法用于生成当前对象的唯一行ID该行ID是通过将表名与所有键值串联起来，
     * 并通过MD5加密生成的这样可以确保每行数据有一个全局唯一的标识符，
     * 便于在分布式系统中进行数据管理和识别
     *
     * @return 生成的行ID
     */
    public String initRowId() {
        // 创建StringBuilder以提高性能，适用于大量字符串拼接操作
        StringBuilder buf = new StringBuilder(getTableName());

        // 遍历所有键值，将它们拼接到StringBuilder中，用"|"分隔
        for (Object v : keys.values()) {
            buf.append("|").append(v);
        }

        // 使用MD5Util工具类对拼接后的字符串进行加密，生成唯一的行ID
        setRowId(MD5Util.crypt(buf.toString(), false));
        return getRowId();
    }

    /**
     * 创建一个TaskInspectCdcEvent实例，使用当前系统时间作为事件时间，并指定数据库表名
     * <p>
     * 此方法提供了一种简便的方式，通过指定时间戳和表名来创建事件对象
     *
     * @param time      事件发生的时间戳，用于记录事件发生的具体时间
     * @param tableName 数据库表名，标识事件相关的数据库表
     * @return TaskInspectCdcEvent实例，包含了事件的基本信息
     */
    public static TaskInspectCdcEvent create(Long time, String tableName) {
        return create(System.currentTimeMillis(), time, tableName, null);
    }

    /**
     * 创建一个TaskInspectCdcEvent对象的重载方法
     * <p>
     * 此方法提供了一个简化接口，当不需要指定额外参数时，可以调用此方法
     *
     * @param referenceTime 参考时间，用于事件的参考时间戳
     * @param time          事件时间，用于事件的实际时间戳
     * @param tableName     表名，标识事件相关的数据库表
     * @return TaskInspectCdcEvent 实例，表示一个特定的事件
     */
    public static TaskInspectCdcEvent create(Long referenceTime, Long time, String tableName) {
        return create(referenceTime, time, tableName, null);
    }

    /**
     * 创建一个TaskInspectCdcEvent实例
     * <p>
     * 该方法用于初始化一个带有参考时间、事件时间、表名和键值对的事件对象
     *
     * @param referenceTime 参考时间，通常用于表示某个特定的时间点
     * @param time          事件发生的时间
     * @param tableName     发生变更的数据库表名
     * @param keys          变更事件的键值对信息，用于标识事件的详细位置
     * @return 返回一个初始化后的TaskInspectCdcEvent对象
     */
    public static TaskInspectCdcEvent create(Long referenceTime, Long time, String tableName, LinkedHashMap<String, Object> keys) {
        TaskInspectCdcEvent ins = new TaskInspectCdcEvent();
        ins.setReferenceTime(referenceTime);
        ins.setTime(time);
        ins.setTableName(tableName);
        ins.setKeys(null == keys ? new LinkedHashMap<>() : keys);
        return ins;
    }

}
