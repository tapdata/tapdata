package com.tapdata.tm.metadatainstance.service;

import com.tapdata.tm.commons.schema.DifferenceField;
import com.tapdata.tm.commons.schema.DifferenceTypeEnum;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesCompareDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadataInstancesCompare.repository.MetadataInstancesCompareRepository;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/** L2-D: real TM compare-apply orchestration, with the Mongo-backed persistence boundary isolated. */
@Timeout(10)
class MetadataInstancesComparePrimaryKeyIT {
    private static final String NODE_ID = "tm-it-node";
    private static final String TABLE_NAME = "tap13093_table";
    private static final String QUALIFIED_NAME = "tap13093_schema.tap13093_table";

    @ParameterizedTest
    @EnumSource(value = DifferenceTypeEnum.class, names = {"Different", "Precision"})
    @DisplayName("L2-D-02: compare apply keeps primary-key metadata through TM model update")
    void applyDifferencePreservesPrimaryKeyAndPdkSchema(DifferenceTypeEnum differenceType) {
        MetadataInstancesCompareRepository compareRepository = mock(MetadataInstancesCompareRepository.class);
        MetadataInstancesService metadataInstancesService = mock(MetadataInstancesService.class);
        MetadataInstancesCompareServiceImpl service = spy(new MetadataInstancesCompareServiceImpl(compareRepository));
        ReflectionTestUtils.setField(service, "metadataInstancesService", metadataInstancesService);

        Field sourceField = field("account_id", "INTEGER", true, 1);
        Field targetField = field("account_id", "INT", false, 4);
        DifferenceField difference = differenceType == DifferenceTypeEnum.Different
                ? DifferenceField.buildDifferentField("account_id", sourceField, targetField)
                : DifferenceField.buildPrecisionField("account_id", sourceField, targetField);
        MetadataInstancesCompareDto compareDto = MetadataInstancesCompareDto.createMetadataInstancesCompareDtoCompare(
                "task-id", NODE_ID, TABLE_NAME, QUALIFIED_NAME, List.of(difference));

        MetadataInstancesDto model = new MetadataInstancesDto();
        model.setName(TABLE_NAME);
        model.setOriginalName(TABLE_NAME);
        model.setQualifiedName(QUALIFIED_NAME);
        model.setSchemaVersion("schema-v1");
        model.setFields(new ArrayList<>(List.of(sourceField)));

        AtomicReference<List<MetadataInstancesDto>> persistedModels = new AtomicReference<>();
        doReturn(List.of(compareDto)).when(service).findAll(any(Query.class));
        doReturn(1L).when(service).deleteAll(any(Query.class));
        doReturn(List.of(compareDto)).when(service).save(anyList(), any(UserDetail.class));
        when(metadataInstancesService.findAll(any(Query.class))).thenReturn(List.of(model));
        when(metadataInstancesService.bulkUpsetByWhere(anyList(), any(UserDetail.class))).thenAnswer(invocation -> {
            persistedModels.set(invocation.getArgument(0));
            return null;
        });

        service.saveMetadataInstancesCompareApply(List.of(), mock(UserDetail.class), true, NODE_ID);

        assertNotNull(persistedModels.get());
        MetadataInstancesDto persisted = persistedModels.get().get(0);
        assertEquals("schema-v1", persisted.getSchemaVersion());
        Field appliedField = persisted.getFields().get(0);
        assertTrue(appliedField.getPrimaryKey());
        assertEquals(1, appliedField.getPrimaryKeyPosition());

        TapTable pdkTable = PdkSchemaConvert.toPdk(persisted);
        assertEquals(List.of("account_id"), new ArrayList<>(pdkTable.primaryKeys()));
    }

    private static Field field(String name, String dataType, Boolean primaryKey, Integer position) {
        Field field = new Field();
        field.setFieldName(name);
        field.setOriginalFieldName(name);
        field.setDataType(dataType);
        field.setPrimaryKey(primaryKey);
        field.setPrimaryKeyPosition(position);
        field.setIsNullable(!Boolean.TRUE.equals(primaryKey));
        field.setAutoincrement("NO");
        return field;
    }
}
