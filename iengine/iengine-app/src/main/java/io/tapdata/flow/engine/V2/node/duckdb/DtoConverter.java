package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.tm.commons.dag.process.dto.TapFieldDto;
import com.tapdata.tm.commons.dag.process.dto.TapTableDto;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DTO 到实体类的转换器
 * 
 * <p>将 TapTableDto 和 TapFieldDto 转换为 TapTable 和 TapField</p>
 */
public class DtoConverter {

    private static final Logger logger = LoggerFactory.getLogger(DtoConverter.class);

    /**
     * 将 TapTableDto 转换为 TapTable
     */
    public static TapTable toTapTable(TapTableDto dto) {
        if (dto == null) {
            return null;
        }

        TapTable tapTable = new TapTable();
        tapTable.setId(dto.getId());
        tapTable.setName(dto.getName());

        // 转换字段
        List<TapField> tapFields = new ArrayList<>();
        LinkedHashMap<String, TapField> nameFieldMap = new LinkedHashMap<>();
        
        if (dto.getFields() != null) {
            for (TapFieldDto fieldDto : dto.getFields()) {
                TapField tapField = toTapField(fieldDto);
                if (tapField != null) {
                    tapFields.add(tapField);
                    nameFieldMap.put(tapField.getName(), tapField);
                }
            }
        }
        
        tapTable.setNameFieldMap(nameFieldMap);
        
        // 设置主键（暂时不设置，只更新字段的主键标记）
        if (dto.getPrimaryKeys() != null && !dto.getPrimaryKeys().isEmpty()) {
            // 更新字段的主键标记
            for (String pk : dto.getPrimaryKeys()) {
                TapField field = nameFieldMap.get(pk);
                if (field != null) {
                    field.setPrimaryKey(true);
                }
            }
        }

        logger.debug("Converted TapTableDto to TapTable: {}, fields: {}, pks: {}", 
                     dto.getName(), tapFields.size(), dto.getPrimaryKeys());
        
        return tapTable;
    }

    /**
     * 将 TapFieldDto 转换为 TapField
     */
    public static TapField toTapField(TapFieldDto dto) {
        if (dto == null) {
            return null;
        }

        TapField tapField = new TapField();
        tapField.setName(dto.getName());
        tapField.setOriginalFieldName(dto.getOriginalFieldName());
        tapField.setDataType(dto.getDataType());
        tapField.setPrimaryKey(dto.getIsPrimaryKey() != null && dto.getIsPrimaryKey());
        
        if (dto.getPrimaryKeyPos() != null && dto.getPrimaryKeyPos() > 0) {
            tapField.setPrimaryKeyPos(dto.getPrimaryKeyPos());
        }
        
        if (dto.getNullable() != null) {
            tapField.setNullable(dto.getNullable());
        } else {
            tapField.setNullable(true);
        }
        
        // 暂时不设置列位置（因为 API 不支持）
        // if (dto.getPos() != null && dto.getPos() > 0) {
        //     tapField.setColumnPosition(dto.getPos());
        // }

        // 暂时不转换 TapType（因为没有足够的信息）
        // 只保留类型名称和参数
        
        return tapField;
    }

    /**
     * 将 TapTableDto 列表转换为 NodeSchemaInfo 列表
     */
    public static Map<String, NodeSchemaInfo> toNodeSchemaInfoMap(List<TapTableDto> dtos) {
        Map<String, NodeSchemaInfo> map = new LinkedHashMap<>();
        
        if (dtos != null) {
            for (TapTableDto dto : dtos) {
                TapTable tapTable = toTapTable(dto);
                if (tapTable != null) {
                    List<String> primaryKeys = new ArrayList<>();
                    if (dto.getPrimaryKeys() != null) {
                        primaryKeys.addAll(dto.getPrimaryKeys());
                    }
                    
                    NodeSchemaInfo schemaInfo = new NodeSchemaInfo(
                        dto.getId(),
                        dto.getName(),
                        null,
                        primaryKeys,
                        tapTable.getNameFieldMap(),
                        tapTable
                    );
                    
                    if (dto.getId() != null) {
                        map.put(dto.getId(), schemaInfo);
                    }
                }
            }
        }
        
        return map;
    }
}
