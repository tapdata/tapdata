package io.tapdata.connector.postgres.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.tapdata.kit.EmptyKit;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.time.OffsetTime;
import java.util.Properties;

public class TimeTZConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private SchemaBuilder timeTZSchema;

    @Override
    public void configure(Properties props) {
        timeTZSchema = SchemaBuilder.int64().name(props.getProperty("schema.name"));
    }

    @Override
    public void converterFor(RelationalColumn column,
                             ConverterRegistration<SchemaBuilder> registration) {

        if ("timetz".equals(column.typeName())) {
            registration.register(timeTZSchema, x -> {
                if (EmptyKit.isNull(x)) {
                    return null;
                }
                OffsetTime offsetTime = (OffsetTime) x;
                return offsetTime.toLocalTime().toSecondOfDay() * 1000000L + offsetTime.getNano() / 1000;
            });
        }
    }
}
