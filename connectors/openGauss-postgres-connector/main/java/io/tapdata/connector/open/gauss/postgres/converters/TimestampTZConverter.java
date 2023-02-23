package io.tapdata.connector.open.gauss.postgres.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.tapdata.kit.EmptyKit;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Properties;

public class TimestampTZConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private SchemaBuilder timestampTZSchema;

    @Override
    public void configure(Properties props) {
        timestampTZSchema = SchemaBuilder.int64().name(props.getProperty("schema.name"));
    }

    @Override
    public void converterFor(RelationalColumn column,
                             ConverterRegistration<SchemaBuilder> registration) {

        if ("timestamptz".equals(column.typeName())) {
            registration.register(timestampTZSchema, x -> {
                if (EmptyKit.isNull(x)) {
                    return null;
                }
                Instant instant = ((OffsetDateTime) x).toInstant();
                return (instant.getEpochSecond() * 1000000 + instant.getNano() / 1000) / (long) Math.pow(10, 6 - column.scale().orElse(6));
            });
        }
    }
}
