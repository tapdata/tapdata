package io.tapdata.connector.open.gauss.postgres.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.tapdata.kit.EmptyKit;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class GeometryConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private SchemaBuilder geometrySchema;
    private final List<String> geometryTypes = Arrays.asList("point", "line", "lseg", "box", "path", "polygon", "circle");

    @Override
    public void configure(Properties props) {
        geometrySchema = SchemaBuilder.string().name(props.getProperty("schema.name"));
    }

    @Override
    public void converterFor(RelationalColumn column,
                             ConverterRegistration<SchemaBuilder> registration) {

        if (geometryTypes.contains(column.typeName())) {
            registration.register(geometrySchema, x -> EmptyKit.isNull(x) ? null : x.toString());
        }
    }
}
