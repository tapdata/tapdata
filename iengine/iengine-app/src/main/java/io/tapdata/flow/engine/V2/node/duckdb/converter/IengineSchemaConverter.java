package io.tapdata.flow.engine.V2.node.duckdb.converter;

import com.tapdata.tm.commons.dag.process.dto.TapFieldDto;
import com.tapdata.tm.commons.dag.process.dto.TapTableDto;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.schema.TableIndexColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapArray;
import io.tapdata.entity.schema.type.TapBinary;
import io.tapdata.entity.schema.type.TapBoolean;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapJson;
import io.tapdata.entity.schema.type.TapMap;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapRaw;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.entity.schema.type.TapType;
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
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * iengine 端 Schema 转换器
 * 将 TapTableDto 列表转换为 NodeSchemaInfo 映射
 */
public class IengineSchemaConverter extends AbstractSchemaConverter<List<TapTableDto>, Map<String, NodeSchemaInfo>> {
    
    private static final Logger logger = LoggerFactory.getLogger(IengineSchemaConverter.class);
    private static final Pattern TYPE_PARAMS_PATTERN = Pattern.compile(
            "^\\s*[A-Z0-9_ ]+\\s*\\(\\s*(\\d+)\\s*(?:,\\s*(\\d+)\\s*)?\\).*");

    private static final class InstanceHolder {
        /**
         * 单例实例
         */
        private static final IengineSchemaConverter instance = new IengineSchemaConverter();
    }

    /**
     * 获取单例
     */
    public static IengineSchemaConverter getInstance() {
        return InstanceHolder.instance;
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
        tapField.setTapType(resolveTapType(dto));
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

        return tapField;
    }

    private TapType resolveTapType(TapFieldDto dto) {
        TapType tapType = createTapTypeByName(dto.getTapTypeName());
        if (tapType == null) {
            tapType = createTapTypeByDataType(dto.getDataType());
        } else {
            enrichTapTypeFromDataType(tapType, dto.getDataType());
        }
        if (tapType == null) {
            tapType = new TapString();
        }
        applyTapTypeParams(tapType, dto.getTapTypeParams());
        return tapType;
    }

    private TapType createTapTypeByName(String tapTypeName) {
        if (StringUtils.isBlank(tapTypeName)) {
            return null;
        }
        String normalized = tapTypeName.trim();
        int packageIndex = normalized.lastIndexOf('.');
        if (packageIndex >= 0) {
            normalized = normalized.substring(packageIndex + 1);
        }
        switch (normalized) {
            case "TapNumber":
                return new TapNumber();
            case "TapBoolean":
                return new TapBoolean();
            case "TapString":
                return new TapString();
            case "TapBinary":
                return new TapBinary();
            case "TapDate":
                return new TapDate();
            case "TapDateTime":
                return new TapDateTime();
            case "TapTime":
                return new TapTime();
            case "TapRaw":
                return new TapRaw();
            case "TapArray":
                return new TapArray();
            case "TapMap":
                return new TapMap();
            case "TapJson":
                return new TapJson();
            default:
                logger.debug("Unknown tapTypeName '{}', falling back to dataType inference", tapTypeName);
                return null;
        }
    }

    private TapType createTapTypeByDataType(String dataType) {
        if (StringUtils.isBlank(dataType)) {
            return null;
        }

        String upperType = dataType.trim().toUpperCase(Locale.ROOT);
        int[] params = parseTypeParams(upperType);

        if (upperType.contains("TIMESTAMPTZ") || upperType.contains("TIMESTAMP WITH TIME ZONE")) {
            return new TapDateTime().withTimeZone(true);
        }
        if (upperType.contains("TIMESTAMP") || upperType.contains("DATETIME")) {
            return new TapDateTime();
        }
        if (upperType.contains("TIME")) {
            return new TapTime().withTimeZone(upperType.contains("TIMETZ") || upperType.contains("WITH TIME ZONE"));
        }
        if (upperType.contains("DATE")) {
            return new TapDate();
        }
        if (upperType.contains("BOOL")) {
            return new TapBoolean();
        }
        if (upperType.contains("BLOB") || upperType.contains("BINARY") || upperType.contains("BYTEA")) {
            TapBinary binary = new TapBinary();
            if (params[0] > 0) {
                binary.bytes((long) params[0]);
            }
            return binary;
        }
        if (upperType.contains("JSON")) {
            return new TapJson();
        }
        if (upperType.contains("CHAR") || upperType.contains("TEXT") || upperType.contains("STRING") || upperType.contains("UUID")) {
            TapString string = new TapString();
            if (params[0] > 0) {
                string.bytes((long) params[0]);
            }
            if (upperType.contains("CHAR") && !upperType.contains("VARCHAR")) {
                string.fixed(true);
            }
            return string;
        }
        if (upperType.contains("DECIMAL") || upperType.contains("NUMERIC") || upperType.contains("NUMBER")) {
            TapNumber number = new TapNumber().fixed(true);
            if (params[0] > 0) {
                number.precision(params[0]);
            }
            if (params[1] >= 0) {
                number.scale(params[1]);
            }
            return number;
        }
        if (upperType.contains("FLOAT") || upperType.contains("REAL")) {
            return new TapNumber().bit(32).fixed(false);
        }
        if (upperType.contains("DOUBLE")) {
            return new TapNumber().bit(64).fixed(false);
        }
        if (upperType.contains("TINYINT")) {
            return new TapNumber().bit(8);
        }
        if (upperType.contains("SMALLINT")) {
            return new TapNumber().bit(16);
        }
        if (upperType.contains("BIGINT")) {
            return new TapNumber().bit(64);
        }
        if (upperType.contains("INT")) {
            return new TapNumber().bit(32);
        }

        return new TapString();
    }

    private void enrichTapTypeFromDataType(TapType tapType, String dataType) {
        if (tapType == null || StringUtils.isBlank(dataType)) {
            return;
        }

        String upperType = dataType.trim().toUpperCase(Locale.ROOT);
        int[] params = parseTypeParams(upperType);

        if (tapType instanceof TapNumber number) {
            if (upperType.contains("DECIMAL") || upperType.contains("NUMERIC") || upperType.contains("NUMBER")) {
                if (number.getFixed() == null) {
                    number.fixed(true);
                }
                if (number.getPrecision() == null && params[0] > 0) {
                    number.precision(params[0]);
                }
                if (number.getScale() == null && params[1] >= 0) {
                    number.scale(params[1]);
                }
            } else if (upperType.contains("FLOAT") || upperType.contains("REAL")) {
                if (number.getBit() == null) {
                    number.bit(32);
                }
                if (number.getFixed() == null) {
                    number.fixed(false);
                }
            } else if (upperType.contains("DOUBLE")) {
                if (number.getBit() == null) {
                    number.bit(64);
                }
                if (number.getFixed() == null) {
                    number.fixed(false);
                }
            } else if (number.getBit() == null) {
                if (upperType.contains("TINYINT")) {
                    number.bit(8);
                } else if (upperType.contains("SMALLINT")) {
                    number.bit(16);
                } else if (upperType.contains("BIGINT")) {
                    number.bit(64);
                } else if (upperType.contains("INT")) {
                    number.bit(32);
                }
            }
            return;
        }

        if (tapType instanceof TapString string) {
            if (string.getBytes() == null && params[0] > 0) {
                string.bytes((long) params[0]);
            }
            if (string.getFixed() == null && upperType.contains("CHAR") && !upperType.contains("VARCHAR")) {
                string.fixed(true);
            }
            return;
        }

        if (tapType instanceof TapBinary binary) {
            if (binary.getBytes() == null && params[0] > 0) {
                binary.bytes((long) params[0]);
            }
            return;
        }

        if (tapType instanceof TapDateTime dateTime) {
            if (dateTime.getWithTimeZone() == null
                    && (upperType.contains("TIMESTAMPTZ") || upperType.contains("WITH TIME ZONE"))) {
                dateTime.withTimeZone(true);
            }
            return;
        }

        if (tapType instanceof TapTime time && time.getWithTimeZone() == null
                && (upperType.contains("TIMETZ") || upperType.contains("WITH TIME ZONE"))) {
            time.withTimeZone(true);
        }
    }

    private int[] parseTypeParams(String dataType) {
        int[] params = new int[]{-1, -1};
        Matcher matcher = TYPE_PARAMS_PATTERN.matcher(dataType);
        if (!matcher.matches()) {
            return params;
        }
        params[0] = parseInt(matcher.group(1), -1);
        params[1] = parseInt(matcher.group(2), -1);
        return params;
    }

    private void applyTapTypeParams(TapType tapType, Map<String, Object> params) {
        if (tapType == null || params == null || params.isEmpty()) {
            return;
        }
        if (tapType instanceof TapNumber number) {
            number.bit(getInteger(params, "bit", number.getBit()));
            number.precision(getInteger(params, "precision", number.getPrecision()));
            number.scale(getInteger(params, "scale", number.getScale()));
            number.fixed(getBoolean(params, "fixed", number.getFixed()));
            number.unsigned(getBoolean(params, "unsigned", number.getUnsigned()));
            number.zerofill(getBoolean(params, "zerofill", number.getZerofill()));
            return;
        }
        if (tapType instanceof TapString string) {
            string.bytes(getLong(params, "bytes", string.getBytes()));
            string.fixed(getBoolean(params, "fixed", string.getFixed()));
            string.byteRatio(getInteger(params, "byteRatio", string.getByteRatio()));
            return;
        }
        if (tapType instanceof TapBinary binary) {
            binary.bytes(getLong(params, "bytes", binary.getBytes()));
            binary.fixed(getBoolean(params, "fixed", binary.getFixed()));
            binary.byteRatio(getInteger(params, "byteRatio", binary.getByteRatio()));
            return;
        }
        if (tapType instanceof TapDateTime dateTime) {
            dateTime.withTimeZone(getBoolean(params, "withTimeZone", dateTime.getWithTimeZone()));
            dateTime.fraction(getInteger(params, "fraction", dateTime.getFraction()));
            dateTime.defaultFraction(getInteger(params, "defaultFraction", dateTime.getDefaultFraction()));
            return;
        }
        if (tapType instanceof TapTime time) {
            time.withTimeZone(getBoolean(params, "withTimeZone", time.getWithTimeZone()));
            time.fraction(getInteger(params, "fraction", time.getFraction()));
            return;
        }
        if (tapType instanceof TapDate date) {
            date.withTimeZone(getBoolean(params, "withTimeZone", date.getWithTimeZone()));
        }
    }

    private Integer getInteger(Map<String, Object> params, String key, Integer defaultValue) {
        Object value = params.get(key);
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof String string) {
            return parseInt(string, defaultValue);
        }
        return defaultValue;
    }

    private Long getLong(Map<String, Object> params, String key, Long defaultValue) {
        Object value = params.get(key);
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String string) {
            try {
                return Long.parseLong(string);
            } catch (NumberFormatException ignored) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    private Boolean getBoolean(Map<String, Object> params, String key, Boolean defaultValue) {
        Object value = params.get(key);
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value instanceof String string && !string.isBlank()) {
            return Boolean.parseBoolean(string);
        }
        return defaultValue;
    }

    private int parseInt(String value, int defaultValue) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }
}
