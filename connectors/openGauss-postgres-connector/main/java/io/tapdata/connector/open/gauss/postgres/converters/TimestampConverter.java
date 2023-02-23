package io.tapdata.connector.open.gauss.postgres.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.tapdata.kit.EmptyKit;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.time.Instant;
import java.util.Properties;

public class TimestampConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private SchemaBuilder timestampSchema;

    @Override
    public void configure(Properties props) {
        timestampSchema = SchemaBuilder.int64().name(props.getProperty("schema.name"));
    }

    @Override
    public void converterFor(RelationalColumn column,
                             ConverterRegistration<SchemaBuilder> registration) {

        if ("timestamp".equals(column.typeName())) {
            registration.register(timestampSchema, x -> {
                if (EmptyKit.isNull(x)) {
                    return null;
                }
                Instant instant = (Instant) x;
                return (instant.getEpochSecond() * 1000000 + instant.getNano() / 1000) / (long) Math.pow(10, 6 - column.scale().orElse(6));
            });
        }
    }
}
