package io.tapdata.connector.open.gauss.postgres.converters;

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

        if ("time".equals(column.typeName())) {
            registration.register(timeSchema, x -> {
                if (EmptyKit.isNull(x)) {
                    return null;
                }
                //for pg<=9.4
                if (x instanceof String) {
                    long second = 0;
                    String[] hourToSecond = ((String) x).split(":");
                    for (int i = 0; i < hourToSecond.length; i++) {
                        second += Integer.parseInt(hourToSecond[i]) + second * 60;
                    }
                    return second * 1000000;
                }
                //for pg>=9.5
                Duration duration = (Duration) x;
                return duration.getSeconds() * 1000000 + duration.getNano() / 1000;
            });
        }
    }
}
