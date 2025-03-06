package com.tapdata.huawei.drs.kafka;

/**
 * 华为 DRS Kafka - Mysql 为源消息类型
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/18 15:54 Create
 */
public enum MysqlOpType {
    DELETE(Stage.CDC),    // 增量-删除事件
    UPDATE(Stage.CDC),    // 增量-更新事件
    INSERT(Stage.CDC),    // 增量-插入事件
    DDL(Stage.CDC),       // 增量-DDL
    INIT(Stage.INIT),     // 全量-数据
    INIT_DDL(Stage.INIT), // 全量-DDL
    UNDEFINED(null),// 未定义
    ;

    private final Stage stage;

    MysqlOpType(Stage stage) {
        this.stage = stage;
    }

    public Stage getStage() {
        return stage;
    }

    public static MysqlOpType fromValue(String type) {
        for (MysqlOpType t : values()) {
            if (t.name().equals(type)) return t;
        }
        return UNDEFINED;
    }
}
