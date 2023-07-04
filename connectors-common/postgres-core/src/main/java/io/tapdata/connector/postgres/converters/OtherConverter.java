package io.tapdata.connector.postgres.converters;

import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class OtherConverter extends BaseTapdataConverter {

    private final List<String> otherTypes = Arrays.asList("bit", "varbit", "tsvector", "tsquery", "regproc", "regprocedure", "regoper", "regoperator", "regclass", "regtype", "regconfig", "regdictionary", "pg_lsn");

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
        return otherTypes.contains(column.typeName());
    }

    @Override
    Object convert(Object data) {
        return data.toString();
    }
}
