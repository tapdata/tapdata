package io.tapdata.connector.postgres.converters;

import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.time.Duration;
import java.util.Properties;

public class TimeConverter extends BaseTapdataConverter {

    @Override
    SchemaBuilder initSchemaBuilder(Properties props) {
        milliSecondOffset = Long.parseLong(props.getProperty("timezone"));
        return SchemaBuilder.int64().name(props.getProperty("schema.name"));
    }

    @Override
    Object initDefaultValue() {
        return 0L;
    }

    @Override
    boolean needConvert(RelationalColumn column) {
        return "time".equals(column.typeName());
    }

    @Override
    Object convert(Object data) {
        //for pg<=9.4
        if (data instanceof String) {
            double microsecond = 0;
            String[] hourToSecond = ((String) data).split(":");
            for (String s : hourToSecond) {
                microsecond = Double.parseDouble(s) + microsecond * 60;
            }
            return (long) (microsecond * 1000000) - milliSecondOffset * 1000;
        }
        //for pg>=9.5
        Duration duration = (Duration) data;
        return duration.getSeconds() * 1000000 - milliSecondOffset * 1000 + duration.getNano() / 1000;
    }
}
