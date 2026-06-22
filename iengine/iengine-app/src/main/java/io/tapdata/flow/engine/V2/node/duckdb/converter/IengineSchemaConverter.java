package io.tapdata.flow.engine.V2.node.duckdb.converter;

import com.tapdata.tm.commons.dag.process.dto.TapFieldDto;
import com.tapdata.tm.commons.dag.process.dto.TapTableDto;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.schema.TableIndexColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.node.duckdb.NodeSchemaInfo;
import io.tapdata.flow.engine.V2.node.duckdb.TypeConverter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * iengine 端 Schema 转换器
 * 将 TapTableDto 列表转换为 NodeSchemaInfo 映射
 */
public class IengineSchemaConverter extends AbstractSchemaConverter<List<TapTableDto>, Map<String, NodeSchemaInfo>> {
    
    private static final Logger logger = LoggerFactory.getLogger(IengineSchemaConverter.class);
    
    /**
     * 单例实例
     */
    private static volatile IengineSchemaConverter instance;
    
    /**
     * 获取单例
     */
    public static IengineSchemaConverter getInstance() {
        if (instance == null) {
            synchronized (IengineSchemaConverter.class) {
                if (instance == null) {
                    instance = new IengineSchemaConverter();
                }
            }
        }
        return instance;
    }
    
    private IengineSchemaConverter() {
        // 默认缓存过期时间：24 小时
        this.cacheExpireTime = java.util.concurrent.TimeUnit.HOURS.toMillis(24);
    }
    
    @Override
    protected String getSourceKey(List<TapTableDto> source) {
        if (source == null || source.isEmpty()) {
            return "empty";
        }
        // 使用所有表的 ID 拼接作为缓存键
        StringBuilder sb = new StringBuilder();
        for (TapTableDto dto : source) {
            if (dto.getId() != null) {
                sb.append(dto.getId()).append("_");
            }
        }
        return sb.toString();
    }
    
    @Override
    protected Map<String, NodeSchemaInfo> doConvert(List<TapTableDto> source) {
        if (source == null || source.isEmpty()) {
            return new LinkedHashMap<>();
        }
        
        Map<String, NodeSchemaInfo> result = new LinkedHashMap<>();
        
        for (TapTableDto dto : source) {
            if (dto.getId() == null) {
                continue;
            }
            
            NodeSchemaInfo schemaInfo = convertToNodeSchemaInfo(dto);
            if (schemaInfo != null) {
                result.put(dto.getId(), schemaInfo);
            }
        }
        
        logger.debug("Converted {} TapTableDto to {} NodeSchemaInfo", source.size(), result.size());
        
        return result;
    }
    
    /**
     * 将单个 TapTableDto 转换为 NodeSchemaInfo
     */
    public NodeSchemaInfo convertToNodeSchemaInfo(TapTableDto dto) {
        if (dto == null) {
            return null;
        }
        
        // 先转换为 TapTable
        TapTable tapTable = convertToTapTable(dto);
        
        // 构建 NodeSchemaInfo
        List<String> primaryKeys = dto.getPrimaryKeys() != null ? new ArrayList<>(dto.getPrimaryKeys()) : new ArrayList<>();
        if (primaryKeys.isEmpty()) {
            List<TapIndex> indexList = tapTable.getIndexList();
            if (indexList != null && !indexList.isEmpty()) {
                TapIndex tapIndex = indexList.get(0);
                tapIndex.getIndexFields().forEach(field -> primaryKeys.add(field.getName()));
            }
        }
        Map<String, TapField> fieldMap = new ConcurrentHashMap<>();
        if (tapTable.getNameFieldMap() != null) {
            fieldMap.putAll(tapTable.getNameFieldMap());
        }
        
        // 预计算Arrow Schema
        Schema arrowSchema = precomputeArrowSchema(dto);
        
        return new NodeSchemaInfo(
            dto.getId(),
            dto.getName(),
            dto.getName(), // qualifiedName 使用表名
            primaryKeys,
            fieldMap,
            tapTable,
            arrowSchema
        );
    }
    
    /**
     * 预计算Arrow Schema（使用TapTableDto的预计算类型）
     */
    private Schema precomputeArrowSchema(TapTableDto dto) {
        List<Field> fields = new ArrayList<>();
        
        if (dto.getFields() == null) {
            return new Schema(fields);
        }
        
        for (TapFieldDto fieldDto : dto.getFields()) {
            String fieldName = fieldDto.getName();
            org.apache.arrow.vector.types.pojo.ArrowType arrowType = TypeConverter.fromTapFieldDto(fieldDto);
            boolean nullable = fieldDto.getNullable() == null || fieldDto.getNullable();
            
            FieldType fieldType = new FieldType(nullable, arrowType, null);
            Field field = new Field(fieldName, fieldType, null);
            fields.add(field);
        }
        
        return new Schema(fields);
    }
    
    /**
     * 将 TapTableDto 转换为 TapTable
     */
    public TapTable convertToTapTable(TapTableDto dto) {
        if (dto == null) {
            return null;
        }
        
        TapTable tapTable = new TapTable();
        tapTable.setId(dto.getId());
        tapTable.setName(dto.getName());
        TableIndex indexes = dto.getIndexes();
        List<TapIndex> indexesList = new ArrayList<>();
        if (indexes != null) {
            TapIndex item = new TapIndex();
            List<TapIndexField> indexFields = new ArrayList<>();
            for (TableIndexColumn column : indexes.getColumns()) {
                TapIndexField tapIndexField = new TapIndexField();
                tapIndexField.setFieldAsc(column.getColumnIsAsc());
                tapIndexField.setName(column.getColumnName());
                tapIndexField.setSubPosition(column.getSubPosition());
                indexFields.add(tapIndexField);
            }
            item.setIndexFields(indexFields);
            item.setUnique(indexes.isUnique());
            item.setCoreUnique(indexes.isCoreUnique());
            item.setPrimary(StringUtils.isNotBlank(indexes.getPrimaryKey()));
            item.setName(indexes.getIndexName());
            item.setCluster(StringUtils.isNotBlank(indexes.getClustered()));
            indexesList.add(item);
        }
        tapTable.setIndexList(indexesList);
        
        // 转换字段
        LinkedHashMap<String, TapField> nameFieldMap = new LinkedHashMap<>();
        
        if (dto.getFields() != null) {
            for (TapFieldDto fieldDto : dto.getFields()) {
                TapField tapField = convertToTapField(fieldDto);
                if (tapField != null) {
                    nameFieldMap.put(tapField.getName(), tapField);
                }
            }
        }
        
        tapTable.setNameFieldMap(nameFieldMap);
        
        // 更新字段的主键标记
        if (dto.getPrimaryKeys() != null && !dto.getPrimaryKeys().isEmpty()) {
            for (String pk : dto.getPrimaryKeys()) {
                TapField field = nameFieldMap.get(pk);
                if (field != null) {
                    field.setPrimaryKey(true);
                }
            }
        }
        
        return tapTable;
    }
    
    /**
     * 将 TapFieldDto 转换为 TapField
     */
    public TapField convertToTapField(TapFieldDto dto) {
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
        
         if (dto.getPos() != null && dto.getPos() > 0) {
             tapField.setPos(dto.getPos());
         }

        // 注意：TapType 的完整信息不恢复，只保留类名方式
        // 在 NodeSchemaInfo 中通过 dataType 或 tapTypeName 进行类型转换
        
        return tapField;
    }
}
