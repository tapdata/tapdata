package io.tapdata.connector.postgres.cdc.config;

import io.debezium.config.Configuration;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.kit.EmptyKit;

import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;
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
    private TimeZone timeZone;

    public PostgresDebeziumConfig() {

    }

    public PostgresDebeziumConfig use(PostgresConfig postgresConfig) {
        this.postgresConfig = postgresConfig;
        return this;
    }

    public PostgresDebeziumConfig use(TimeZone timeZone) {
        this.timeZone = timeZone;
        return this;
    }

    public PostgresDebeziumConfig watch(List<String> observedTableList) {
        this.observedTableList = observedTableList;
        //unique and can find it
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
                .with("interval.handling.mode", "string")
                .with("converters", "timestamp,timestampTZ,time,timeTZ,geometry,other")
                .with("timestamp.type", "io.tapdata.connector.postgres.converters.TimestampConverter")
                .with("timestamp.schema.name", "io.debezium.postgresql.type.Timestamp")
                .with("timestamp.timezone", timeZone.getRawOffset())
                .with("timestampTZ.type", "io.tapdata.connector.postgres.converters.TimestampTZConverter")
                .with("timestampTZ.schema.name", "io.debezium.postgresql.type.TimestampTZ")
                .with("time.type", "io.tapdata.connector.postgres.converters.TimeConverter")
                .with("time.schema.name", "io.debezium.postgresql.type.Time")
                .with("time.timezone", timeZone.getRawOffset())
                .with("timeTZ.type", "io.tapdata.connector.postgres.converters.TimeTZConverter")
                .with("timeTZ.schema.name", "io.debezium.postgresql.type.TimeTZ")
                .with("geometry.type", "io.tapdata.connector.postgres.converters.GeometryConverter")
                .with("geometry.schema.name", "io.debezium.postgresql.type.Geometry")
                .with("other.type", "io.tapdata.connector.postgres.converters.OtherConverter")
                .with("other.schema.name", "io.debezium.postgresql.type.Other")
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
