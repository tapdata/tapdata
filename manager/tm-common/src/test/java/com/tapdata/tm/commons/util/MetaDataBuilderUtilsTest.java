package com.tapdata.tm.commons.util;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class MetaDataBuilderUtilsTest {

    @Test
    void buildShouldAssignPositionToPrimaryKeyWithoutPosition() {
        DataSourceConnectionDto source = new DataSourceConnectionDto();
        source.setId(new ObjectId());
        source.setName("db2i");
        source.setLoadSchemaField(false);

        Field primaryKey = new Field();
        primaryKey.setFieldName("id");
        primaryKey.setPrimaryKey(true);
        MetadataInstancesDto oldModel = new MetadataInstancesDto();
        oldModel.setFields(Collections.singletonList(primaryKey));

        MetadataInstancesDto result = MetaDataBuilderUtils.build(
                "table", source, null, null, "orders", null, oldModel, null);

        Assertions.assertEquals(1, result.getFields().get(0).getPrimaryKeyPosition());
    }
}
