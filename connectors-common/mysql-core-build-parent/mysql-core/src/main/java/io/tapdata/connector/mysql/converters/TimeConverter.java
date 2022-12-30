package io.tapdata.connector.mysql.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.tapdata.kit.EmptyKit;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.time.Duration;
import java.util.Properties;

public class TimeConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private SchemaBuilder timeSchema;

    @Override
    public void configure(Properties props) {
        timeSchema = SchemaBuilder.int64().name(props.getProperty("schema.name"));
    }

    @Override
    public void converterFor(RelationalColumn column,
                             ConverterRegistration<SchemaBuilder> registration) {

        if ("time".equalsIgnoreCase(column.typeName())) {
            registration.register(timeSchema, x -> {
                if (EmptyKit.isNull(x)) {
                    return null;
                }
                Duration duration = (Duration) x;
                long seconds = duration.getSeconds();
                long nanos = duration.getNano();
                if (seconds < 0) {
                    int second = (int) (seconds * (-1)) % 60;
                    int minute = (int) ((seconds * (-1)) % 3600) / 60;
                    return (seconds + 2 * 60 * minute + 2 * second - (nanos != 0 ? 2 : 0)) * 1000000 + nanos / 1000;
                } else {
                    return seconds * 1000000 + nanos / 1000;
                }
            });
        }
    }
}
