package io.tapdata.connector.dws.converters;

import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.time.Instant;
import java.util.Properties;

public class TimestampConverter extends BaseTapdataConverter {

    @Override
    SchemaBuilder initSchemaBuilder(Properties props) {
        return SchemaBuilder.int64().name(props.getProperty("schema.name"));
    }

    @Override
    Object initDefaultValue() {
        return 0L;
    }

    @Override
    boolean needConvert(RelationalColumn column) {
        return "timestamp".equals(column.typeName());
    }

    @Override
    Object convert(Object data) {
        Instant instant = (Instant) data;
        return (instant.getEpochSecond() * 1000000 + instant.getNano() / 1000) / (long) Math.pow(10, 6 - column.scale().orElse(6));
    }
}
