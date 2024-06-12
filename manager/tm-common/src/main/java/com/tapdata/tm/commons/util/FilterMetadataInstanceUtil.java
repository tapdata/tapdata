package com.tapdata.tm.commons.util;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.schema.TableIndexColumn;
import io.tapdata.entity.conversion.PossibleDataTypes;
import io.tapdata.entity.schema.type.TapType;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class FilterMetadataInstanceUtil {
    public static void filterMetadataInstancesFields(MetadataInstancesDto metadataInstancesDto) {
        List<Field> fields = metadataInstancesDto.getFields();
        if (CollectionUtils.isNotEmpty(fields)){

            Map<String, PossibleDataTypes> dataTypes = metadataInstancesDto.getFindPossibleDataTypes();
            if (Objects.nonNull(dataTypes)) {
                fields.forEach(field -> {
                    if (Objects.nonNull(dataTypes.get(field.getFieldName())) && org.apache.commons.collections4.CollectionUtils.isEmpty(dataTypes.get(field.getFieldName()).getDataTypes())) {
                        field.setDeleted(true);
                    }
                    TapType tapType = JSON.parseObject(field.getTapType(), TapType.class);
                    if (TapType.TYPE_RAW == tapType.getType()) {
                        field.setDeleted(true);
                    }
                });
            }

            List<String> deleteFieldNames = fields.stream().filter(Field::isDeleted).map(Field::getFieldName).collect(Collectors.toList());
            metadataInstancesDto.setFields(fields.stream().filter(f->!f.isDeleted()).collect(Collectors.toList()));
            List<TableIndex> indices = metadataInstancesDto.getIndices();
            List<TableIndex> newIndices = new ArrayList<>();

            if(indices != null) {
                for (TableIndex index : indices) {
                    List<TableIndexColumn> columns = index.getColumns();
                    List<TableIndexColumn> newIndexColums = new ArrayList<>();
                    for (TableIndexColumn column : columns) {
                        if (!deleteFieldNames.contains(column.getColumnName())) {
                            newIndexColums.add(column);
                        }
                    }
                    if (newIndexColums.size() > 0) {
                        index.setColumns(newIndexColums);
                        newIndices.add(index);
                    }
                }
            }

            metadataInstancesDto.setIndices(newIndices);
        }
    }
}
