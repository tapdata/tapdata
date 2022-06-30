package com.tapdata.tm.dataflow;

import com.tapdata.manager.common.utils.StringUtils;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/10/30 下午5:42
 * @description
 */
public enum StageType {

    Database("database", "dataNode"),
    Table("table", "dataNode"),
    Collection("collection", "dataNode"),
    File("file", "dataNode"),
    Gridfs("gridfs", "dataNode"),
    DummyDB("dummy db", "dataNode"),
    RestAPI("rest api", "dataNode"),
    ElasticSearch("elasticsearch", "dataNode"),
    CustomConnection("custom_connection", "dataNode"),
    PublishApi("publishApi", "dataNode"),
    MemCache("mem_cache", "dataNode"),
    LogCollect("log_collect", "dataNode"),
    Redis("redis", "dataNode"),
    CSV("csv", "dataNode"),
    Excel("excel", "dataNode"),
    Json("json", "dataNode"),
    Xml("xml", "dataNode"),
    Kafka("kafka", "dataNode"),
    Hive("hive", "dataNode"),
    TcpUdp("tcp_udp", "dataNode"),
    MQ("mq", "dataNode"),
    HBase("hbase", "dataNode"),
    Kudu("kudu", "dataNode"),
    ClickHouse("clickhouse", "dataNode");

    private final String name;
    private final String type;

    private StageType(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public boolean isDataNode() {
        return "dataNode".equals(type);
    }
    public boolean isProcessNode() {
        return "processNode".equals(type);
    }

    public static StageType get(String type) {
        for (StageType stageType : StageType.values()) {
            if (stageType.name.equals(type))
                return stageType;
        }
        return null;
    }

    public static boolean contains(String nodeType) {
        if (StringUtils.isEmpty(nodeType))
            return false;
        for (StageType stageType : StageType.values()) {
            if (stageType.name.equals(nodeType))
                return true;
        }
        return false;
    }

}
