package com.tapdata.tm.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.manager.common.utils.ReflectionUtils;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.group.constant.GroupConstants;
import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.*;

public class ExcelUtil {

    /**
     * 优先列（排在最前面）
     */
    public static final List<String> PRIORITY_COLUMNS = Arrays.asList("id","name", "database_type");

    /**
     * 其他基础列（排在敏感config字段之后）
     */
    public static final List<String> OTHER_BASE_COLUMNS = Arrays.asList(
            "connection_type", "pdkHash",
            "definitionPdkId", "definitionVersion", "definitionGroup",
            "definitionScope", "definitionBuildNumber", "definitionPdkAPIVersion",
            "definitionTags","pdkType","pdkRealName",
            "accessNodeType","listtags","shareCdcEnable","accessNodeType","accessNodeProcessIdList","loadAllTables",
            "shareCDCExternalStorageId","heartbeatEnable","isInit","accessNodeProcessId","table_filter"
    );

    /**
     * config列前缀
     */
    public static final String CONFIG_COLUMN_PREFIX = "config.";

    /**
     * 导出Connections为Excel字节数组
     *
     * @param connections 连接列表
     * @return Excel文件字节数组
     */
    public static byte[] exportConnectionsToExcel(List<DataSourceConnectionDto> connections) throws IOException {
        if (CollectionUtils.isEmpty(connections)) {
            return new byte[0];
        }

        // 收集所有config的keys
        Set<String> allConfigKeys = new LinkedHashSet<>();
        for (DataSourceConnectionDto conn : connections) {
            Map<String, Object> config = conn.getConfig();
            if (org.apache.commons.collections4.MapUtils.isNotEmpty(config)) {
                collectConfigKeys(config, "", allConfigKeys);
            }
        }

        // 分离敏感字段和非敏感字段
        List<String> sensitiveConfigKeys = new ArrayList<>();
        List<String> nonSensitiveConfigKeys = new ArrayList<>();
        for (String key : allConfigKeys) {
            if (isSensitiveKey(key)) {
                sensitiveConfigKeys.add(key);
            } else {
                nonSensitiveConfigKeys.add(key);
            }
        }
        Collections.sort(sensitiveConfigKeys);
        Collections.sort(nonSensitiveConfigKeys);
        try (Workbook workbook = new XSSFWorkbook();
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            Sheet sheet = workbook.createSheet("Connections");

            // 创建表头样式
            CellStyle headerStyle = createHeaderStyle(workbook);

            // 创建表头行
            Row headerRow = sheet.createRow(0);
            int colIndex = 0;

            // 1. 优先列表头（name, database_type）
            for (String col : PRIORITY_COLUMNS) {
                Cell cell = headerRow.createCell(colIndex++);
                cell.setCellValue(col);
                cell.setCellStyle(headerStyle);
            }

            // 2. 敏感config列表头（紧随优先列之后）
            for (String configKey : sensitiveConfigKeys) {
                Cell cell = headerRow.createCell(colIndex++);
                cell.setCellValue(CONFIG_COLUMN_PREFIX + configKey);
                cell.setCellStyle(headerStyle);
            }

            // 3. 其他基础列表头
            for (String col : OTHER_BASE_COLUMNS) {
                Cell cell = headerRow.createCell(colIndex++);
                cell.setCellValue(col);
                cell.setCellStyle(headerStyle);
            }

            // 4. 非敏感config列表头
            for (String configKey : nonSensitiveConfigKeys) {
                Cell cell = headerRow.createCell(colIndex++);
                cell.setCellValue(CONFIG_COLUMN_PREFIX + configKey);
                cell.setCellStyle(headerStyle);
            }

            // 填充数据行
            int rowIndex = 1;
            for (DataSourceConnectionDto conn : connections) {
                Row dataRow = sheet.createRow(rowIndex++);
                colIndex = 0;

                // 1. 填充优先列
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getId().toHexString()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getName()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getDatabase_type()));

                // 2. 填充敏感config列（脱敏为空）
                Map<String, Object> config = conn.getConfig();
                for (String configKey : sensitiveConfigKeys) {
                    dataRow.createCell(colIndex++).setCellValue("");
                }

                // 3. 填充其他基础列
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getConnection_type()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getPdkHash()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getDefinitionPdkId()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getDefinitionVersion()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getDefinitionGroup()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getDefinitionScope()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getDefinitionBuildNumber()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getDefinitionPdkAPIVersion()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getDefinitionTags()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getPdkType()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getPdkRealName()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getAccessNodeType()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getListtags()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getShareCdcEnable()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getAccessNodeProcessIdList()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getLoadAllTables()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getShareCDCExternalStorageId()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getHeartbeatEnable()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getIsInit()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getAccessNodeProcessId()));
                dataRow.createCell(colIndex++).setCellValue(getStringValue(conn.getTable_filter()));

                // 4. 填充非敏感config列
                for (String configKey : nonSensitiveConfigKeys) {
                    Cell cell = dataRow.createCell(colIndex++);
                    if (org.apache.commons.collections4.MapUtils.isNotEmpty(config)) {
                        Object value = getNestedValue(config, configKey);
                        cell.setCellValue(convertToString(value));
                    }
                }
            }

            // 自动调整列宽
            for (int i = 0; i < colIndex; i++) {
                sheet.autoSizeColumn(i);
            }

            workbook.write(baos);
            return baos.toByteArray();
        }
    }

    /**
     * 从Excel导入Connections
     *
     * @param inputStream Excel文件输入流
     * @return 解析后的连接列表
     */
    public static List<DataSourceConnectionDto> importConnectionsFromExcel(InputStream inputStream) throws IOException {
        List<DataSourceConnectionDto> connections = new ArrayList<>();

        try (Workbook workbook = WorkbookFactory.create(inputStream)) {
            Sheet sheet = workbook.getSheetAt(0);
            if (sheet == null) {
                return connections;
            }

            // 读取表头
            Row headerRow = sheet.getRow(0);
            if (headerRow == null) {
                return connections;
            }

            Map<Integer, String> columnMap = new LinkedHashMap<>();
            for (int i = 0; i < headerRow.getLastCellNum(); i++) {
                Cell cell = headerRow.getCell(i);
                if (cell != null) {
                    columnMap.put(i, getCellStringValue(cell));
                }
            }

            // 读取数据行
            for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                Row dataRow = sheet.getRow(rowIndex);
                if (dataRow == null) {
                    continue;
                }

                DataSourceConnectionDto conn = parseConnectionFromRow(dataRow, columnMap);
                if (conn != null && StringUtils.isNotBlank(conn.getName())) {
                    connections.add(conn);
                }
            }
        }

        return connections;
    }

    /**
     * 从Excel行解析Connection
     */
    private static DataSourceConnectionDto parseConnectionFromRow(Row row, Map<Integer, String> columnMap) {
        DataSourceConnectionDto conn = new DataSourceConnectionDto();
        Map<String, Object> config = new LinkedHashMap<>();

        for (Map.Entry<Integer, String> entry : columnMap.entrySet()) {
            int colIndex = entry.getKey();
            String columnName = entry.getValue();
            Cell cell = row.getCell(colIndex);
            String value = getCellStringValue(cell);

            if (StringUtils.isBlank(columnName)) {
                continue;
            }

            if (columnName.startsWith(CONFIG_COLUMN_PREFIX)) {
                // config列
                String configKey = columnName.substring(CONFIG_COLUMN_PREFIX.length());
                if (StringUtils.isNotBlank(value)) {
                    setNestedValue(config, configKey, parseValue(value));
                }
            } else {
                // 基础列
                setBaseColumnValue(conn, columnName, value);
            }
        }

        if (MapUtils.isNotEmpty(config)) {
            conn.setConfig(config);
        }

        return conn;
    }

    /**
     * 设置基础列值
     */
    private static void setBaseColumnValue(DataSourceConnectionDto conn, String columnName, String value) {
        if (StringUtils.isBlank(value)) {
            return;
        }
        switch (columnName) {
            case "id":
                conn.setId(MongoUtils.toObjectId(value));
                break;
            case "name":
                conn.setName(value);
                break;
            case "database_type":
                conn.setDatabase_type(value);
                break;
            case "connection_type":
                conn.setConnection_type(value);
                break;
            case "pdkHash":
                conn.setPdkHash(value);
                break;
            case "definitionPdkId":
                conn.setDefinitionPdkId(value);
                break;
            case "definitionVersion":
                conn.setDefinitionVersion(value);
                break;
            case "definitionGroup":
                conn.setDefinitionGroup(value);
                break;
            case "definitionScope":
                conn.setDefinitionScope(value);
                break;
            case "definitionBuildNumber":
                conn.setDefinitionBuildNumber(value);
                break;
            case "definitionPdkAPIVersion":
                conn.setDefinitionPdkAPIVersion(value);
                break;
            case "definitionTags":
                conn.setDefinitionTags(JsonUtil.parseJsonUseJackson(value, new TypeReference<List<String>>(){}));
                break;
            case "pdkType":
                conn.setPdkType(value);
                break;
            case "pdkRealName":
                conn.setPdkRealName(value);
                break;
            case "accessNodeType":
                conn.setAccessNodeType(value);
                break;
            case "listtags":
                conn.setListtags(JsonUtil.parseJsonUseJackson(value, new TypeReference<List<Map<String, String>>>(){}));
                break;
            case "shareCdcEnable":
                conn.setShareCdcEnable(Boolean.parseBoolean(value));
                break;
            case "accessNodeProcessIdList":
                conn.setAccessNodeProcessIdList(JsonUtil.parseJsonUseJackson(value, new TypeReference<List<String>>(){}));
                break;
            case "loadAllTables":
                conn.setLoadAllTables(Boolean.parseBoolean(value));
                break;
            case "shareCDCExternalStorageId":
                conn.setShareCDCExternalStorageId(value);
                break;
            case "heartbeatEnable":
                conn.setHeartbeatEnable(Boolean.parseBoolean(value));
                break;
            case "isInit":
                conn.setIsInit(Boolean.parseBoolean(value));
                break;
            case "accessNodeProcessId":
                conn.setAccessNodeProcessId(value);
                break;
            case "table_filter":
                conn.setTable_filter(value);
                break;
            default:
                break;
        }
    }

    /**
     * 递归收集config中的所有keys（支持嵌套对象）
     */
    private static void collectConfigKeys(Map<String, Object> config, String prefix, Set<String> keys) {
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map) {
                collectConfigKeys((Map<String, Object>) value, key, keys);
            } else {
                keys.add(key);
            }
        }
    }

    /**
     * 获取嵌套对象的值
     */
    private static Object getNestedValue(Map<String, Object> map, String key) {
        String[] parts = key.split("\\.");
        Object current = map;
        for (String part : parts) {
            if (current instanceof Map) {
                current = ((Map<String, Object>) current).get(part);
            } else {
                return null;
            }
        }
        return current;
    }

    /**
     * 设置嵌套对象的值
     */
    private static void setNestedValue(Map<String, Object> map, String key, Object value) {
        String[] parts = key.split("\\.");
        Map<String, Object> current = map;
        for (int i = 0; i < parts.length - 1; i++) {
            current = (Map<String, Object>) current.computeIfAbsent(parts[i], k -> new LinkedHashMap<>());
        }
        current.put(parts[parts.length - 1], value);
    }

    /**
     * 判断是否为敏感字段
     */
    private static boolean isSensitiveKey(String key) {
        String lastPart = key.contains(".") ? key.substring(key.lastIndexOf(".") + 1) : key;
        return GroupConstants.MASK_PROPERTIES.contains(lastPart);
    }

    /**
     * 创建表头样式
     */
    private static CellStyle createHeaderStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        Font font = workbook.createFont();
        font.setBold(true);
        style.setFont(font);
        style.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setBorderBottom(BorderStyle.THIN);
        style.setBorderTop(BorderStyle.THIN);
        style.setBorderLeft(BorderStyle.THIN);
        style.setBorderRight(BorderStyle.THIN);
        return style;
    }

    /**
     * 获取字符串值，null返回空字符串
     */
    private static String getStringValue(Object value) {
        if (value == null) {
            return "";
        }
        if (value instanceof String) {
            return (String) value;
        }else {
            return JsonUtil.toJsonUseJackson(value);
        }
    }

    /**
     * 将对象转换为字符串
     */
    private static String convertToString(Object value) {
        if (value == null) {
            return "";
        }
        if (value instanceof String) {
            return (String) value;
        }
        if (value instanceof Collection || value instanceof Map) {
            return JsonUtil.toJsonUseJackson(value);
        }
        return String.valueOf(value);
    }

    /**
     * 获取单元格字符串值
     */
    private static String getCellStringValue(Cell cell) {
        if (cell == null) {
            return "";
        }
        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getDateCellValue().toString();
                }
                double numValue = cell.getNumericCellValue();
                if (numValue == Math.floor(numValue)) {
                    return String.valueOf((long) numValue);
                }
                return String.valueOf(numValue);
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            case FORMULA:
                try {
                    return cell.getStringCellValue();
                } catch (Exception e) {
                    return String.valueOf(cell.getNumericCellValue());
                }
            default:
                return "";
        }
    }

    /**
     * 解析字符串值为适当的类型
     */
    private static Object parseValue(String value) {
        if (StringUtils.isBlank(value)) {
            return null;
        }
        // 尝试解析为数字
        try {
            if (value.contains(".")) {
                return Double.parseDouble(value);
            }
            return Long.parseLong(value);
        } catch (NumberFormatException ignored) {
        }
        // 尝试解析为布尔值
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return Boolean.parseBoolean(value);
        }
        // 尝试解析为JSON数组或对象
        if ((value.startsWith("[") && value.endsWith("]")) || (value.startsWith("{") && value.endsWith("}"))) {
            try {
                return JsonUtil.parseJsonUseJackson(value, Object.class);
            } catch (Exception ignored) {
            }
        }
        return value;
    }
    /**
     * 从payload中提取所有连接并生成Excel
     */
    public static byte[] buildConnectionExcel(List<TaskUpAndLoadDto> connectionPayloads) throws IOException {
        if (CollectionUtils.isEmpty(connectionPayloads)) {
            return new byte[0];
        }

        Map<String, DataSourceConnectionDto> connectionDtoMap = new HashMap<>();
        for (TaskUpAndLoadDto dto : connectionPayloads) {
            if (GroupConstants.COLLECTION_CONNECTION.equals(dto.getCollectionName())
                    && StringUtils.isNotBlank(dto.getJson())) {
                DataSourceConnectionDto conn = JsonUtil.parseJsonUseJackson(dto.getJson(), DataSourceConnectionDto.class);
                if (conn != null) {
                    connectionDtoMap.putIfAbsent(conn.getId().toHexString(), conn);
                }
            }
        }

        return exportConnectionsToExcel(connectionDtoMap.values().stream().toList());
    }
}
