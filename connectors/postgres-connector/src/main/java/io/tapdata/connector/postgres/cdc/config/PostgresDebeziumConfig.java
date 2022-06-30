package io.tapdata.connector.postgres.cdc.config;

import io.debezium.config.Configuration;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.kit.EmptyKit;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * debezium config for postgres
 *
 * @author Jarad
 * @date 2022/5/10
 */
public class PostgresDebeziumConfig {

    private PostgresConfig postgresConfig;
    private List<String> observedTableList;
    private String slotName; //unique for each slot, so create it by postgres config and observed tables
    private String namespace;

    public PostgresDebeziumConfig() {

    }

    public PostgresDebeziumConfig use(PostgresConfig postgresConfig) {
        this.postgresConfig = postgresConfig;
        return this;
    }

    public PostgresDebeziumConfig watch(List<String> observedTableList) {
        this.observedTableList = observedTableList;
        //unique and can find it
        if (EmptyKit.isNull(slotName)) {
//            this.slotName = "tapdata_cdc_" + UUID.nameUUIDFromBytes((TapSimplify.toJson(postgresConfig) + (EmptyKit.isNotEmpty(observedTableList) ?
//                            TapSimplify.toJson(observedTableList.stream().sorted().collect(Collectors.toList())) : "null")).getBytes())
//                    .toString().replaceAll("-", "_");
            this.slotName = "tapdata_cdc_" + UUID.randomUUID().toString().replaceAll("-", "_");
        }
        this.namespace = slotName + "-postgres-connector";
        return this;
    }

    public List<String> getObservedTableList() {
        return observedTableList;
    }

    public String getSlotName() {
        return slotName;
    }

    public PostgresDebeziumConfig useSlot(String slotName) {
        this.slotName = slotName;
        return this;
    }

    public String getNamespace() {
        return namespace;
    }

    /**
     * create debezium config
     *
     * @return Configuration
     */
    public Configuration create() {
        Configuration.Builder builder = Configuration.create();
        builder.with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
//                .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage", "io.tapdata.connector.postgres.cdc.offset.PostgresOffsetBackingStore") //customize offset store, store in engine
                .with("snapshot.mode", "never")
                .with("slot.name", slotName)
//                .with("offset.storage.file.filename", "d:/cdc/offset/" + slotName + ".dat") //path must be changed with requirement
                .with("offset.flush.interval.ms", 60000)
                .with("name", slotName + "-postgres-connector")
                .with("database.server.name", postgresConfig.getDatabase())
                .with("database.hostname", postgresConfig.getHost())
                .with("database.port", postgresConfig.getPort())
                .with("database.user", postgresConfig.getUser())
                .with("database.password", postgresConfig.getPassword())
                .with("database.dbname", postgresConfig.getDatabase())
                .with("time.precision.mode", "connect")
                .with("transforms.tsFormat1.type", "org.apache.kafka.connect.transforms.TimestampConverter$Value")
                .with("transforms.tsFormat1.target.type", "string")
                .with("transforms.tsFormat1.field", "transaction_time")
                .with("transforms.tsFormat1.format", "yyyy-MM-dd HH:mm:ss")
                .with("plugin.name", postgresConfig.getLogPluginName());
        if (EmptyKit.isNotEmpty(observedTableList)) {
            //construct tableWhiteList with schema.table(,) as <public.Student,postgres.test>
            String tableWhiteList = observedTableList.stream().map(v -> postgresConfig.getSchema() + "." + v).collect(Collectors.joining(", "));
            builder.with("table.whitelist", tableWhiteList);
        }
        return builder.build();
    }

    enum LogDecorderPlugins {
        DECORDERBUFS("decoderbufs"),
        WAL2JSON("wal2json"),
        WAL2JSONRDS("wal2json_rds"),
        WAL2JSONSTREMING("wal2json_streaming"),
        WAL2JSONRDSSTREAMING("wal2json_rds_streaming"),
        PGOUTPUT("pgoutput"),
        ;

        private String pluginName;

        LogDecorderPlugins(String pluginName) {
            this.pluginName = pluginName;
        }

        public String getPluginName() {
            return pluginName;
        }

        private static HashMap<String, LogDecorderPlugins> map = new HashMap<>();

        static {
            for (LogDecorderPlugins value : LogDecorderPlugins.values()) {
                map.put(value.getPluginName(), value);
            }
        }
    }
}
