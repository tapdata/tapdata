package io.tapdata.connector.dws.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.tapdata.kit.EmptyKit;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Properties;
import java.util.function.Function;

/**
 * @author samuel
 * @Description
 * @create 2023-06-29 15:11
 **/
public abstract class BaseTapdataConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    protected SchemaBuilder schemaBuilder;
    protected Object defaultValue;
    protected RelationalColumn column;

    abstract SchemaBuilder initSchemaBuilder(Properties props);
    abstract Object initDefaultValue();

    abstract boolean needConvert(RelationalColumn column);

    abstract Object convert(Object data);

    @Override
    public final void configure(Properties props) {
        this.schemaBuilder = initSchemaBuilder(props);
        this.defaultValue = initDefaultValue();
    }

    @Override
    public final void converterFor(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        if (needConvert(field)) {
            this.column = field;
            registration.register(schemaBuilder, d -> convert(d, field, defaultValue, this::convert));
        }
    }

    private Object convert(Object data, RelationalColumn column, Object fallback, Function<Object, Object> converter) {
        if (EmptyKit.isNull(data)) {
            if (column.isOptional()) {
                return null;
            }
            Object defaultValue = column.defaultValue();
            return EmptyKit.isNotNull(defaultValue) ? defaultValue : fallback;
        }
        return converter.apply(data);
    }
}
