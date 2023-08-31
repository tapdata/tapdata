package io.tapdata.connector.dws.converters;

import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.time.OffsetTime;
import java.util.Properties;

public class TimeTZConverter extends BaseTapdataConverter {

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
        return "timetz".equals(column.typeName());
    }

    @Override
    Object convert(Object data) {
        OffsetTime offsetTime = (OffsetTime) data;
        return offsetTime.toLocalTime().toSecondOfDay() * 1000000L + offsetTime.getNano() / 1000;
    }
}
