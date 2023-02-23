package io.tapdata.connector.open.gauss.postgres.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.tapdata.kit.EmptyKit;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class OtherConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private SchemaBuilder otherSchema;
    private final List<String> otherTypes = Arrays.asList("bit", "varbit", "tsvector", "tsquery", "regproc", "regprocedure", "regoper", "regoperator", "regclass", "regtype", "regconfig", "regdictionary", "pg_lsn");

    @Override
    public void configure(Properties props) {
        otherSchema = SchemaBuilder.string().name(props.getProperty("schema.name"));
    }

    @Override
    public void converterFor(RelationalColumn column,
                             ConverterRegistration<SchemaBuilder> registration) {

        if (otherTypes.contains(column.typeName())) {
            registration.register(otherSchema, x -> EmptyKit.isNull(x) ? null : x.toString());
        }
    }
}
