package io.tapdata.connector.postgres.converters;

import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class GeometryConverter extends BaseTapdataConverter {

    private final List<String> geometryTypes = Arrays.asList("point", "line", "lseg", "box", "path", "polygon", "circle");

    @Override
    SchemaBuilder initSchemaBuilder(Properties props) {
        return SchemaBuilder.string().name(props.getProperty("schema.name"));
    }

    @Override
    Object initDefaultValue() {
        return "";
    }

    @Override
    boolean needConvert(RelationalColumn column) {
        return geometryTypes.contains(column.typeName());
    }

    @Override
    Object convert(Object data) {
        return data.toString();
    }
}
