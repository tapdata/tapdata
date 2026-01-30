package com.tapdata.tm.utils;


import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.group.constant.GroupConstants;
import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddressList;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.bson.types.ObjectId;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.*;

public class ExcelUtil {

    /**
     * 优先列（排在最前面）
     */
    public static final List<String> PRIORITY_COLUMNS = Arrays.asList("name", "database_type");

    /**
     * 其他基础列（排在敏感config字段之后）
     */
    public static final List<String> OTHER_BASE_COLUMNS = Arrays.asList(
            "id","connection_type", "pdkHash",
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

    private static final String SHEET_PROTECT = "tapdata";

    private static final String SHEET_NAME_MONGO = "Mongo";
    private static final String SHEET_NAME_JDBC = "JDBC";
    private static final String MONGO_API_KEY_URI = "database_uri";
    private static final String DEPLOYMENT_MODE_KEY = "deploymentMode";
    private static final String MASTER_SLAVE_ADDRESS_KEY = "masterSlaveAddress";
    private static final String DEPLOYMENT_MODE_MASTER_SLAVE = "master-slave";
    /**
     * 需要填写的敏感数据
     */
    private static final List<String> PREFERRED_API_KEY_ORDER = Arrays.asList(
            DEPLOYMENT_MODE_KEY,
            MASTER_SLAVE_ADDRESS_KEY,
            MONGO_API_KEY_URI,
            "database_host",
            "database_port",
            "database_sid",
            "database_tnsName",
            "database_name",
            "database_owner",
            "database_username",
            "database_password"
    );

    private static final Map<String, Field> BASE_COLUMN_FIELDS = buildBaseColumnFields();

    /**
     * 导出Connections为Excel字节数组
     *
     * @param connections 连接列表
     * @return Excel文件字节数组
     */
    public static byte[] exportConnectionsToExcel(List<DataSourceConnectionDto> connections, UserDetail user) throws IOException {
        if (CollectionUtils.isEmpty(connections)) {
            return new byte[0];
        }

        List<DataSourceConnectionDto> mongoConnections = new ArrayList<>();
        List<DataSourceConnectionDto> jdbcConnections = new ArrayList<>();
        for (DataSourceConnectionDto conn : connections) {
            if (isMongoDataSource(conn.getDatabase_type())) {
                mongoConnections.add(conn);
            } else {
                jdbcConnections.add(conn);
            }
        }

        Map<String, DataSourceDefinitionDto> definitionMap = buildDefinitionMap(connections,user);
        try (Workbook workbook = new XSSFWorkbook();
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            if (!mongoConnections.isEmpty()) {
                fillConnectionSheet(workbook, SHEET_NAME_MONGO, mongoConnections, definitionMap, true);
            }
            if (!jdbcConnections.isEmpty()) {
                fillConnectionSheet(workbook, SHEET_NAME_JDBC, jdbcConnections, definitionMap, false);
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
    public static List<DataSourceConnectionDto> importConnectionsFromExcel(InputStream inputStream,UserDetail user) throws IOException {
        List<DataSourceConnectionDto> connections = new ArrayList<>();

        try (Workbook workbook = WorkbookFactory.create(inputStream)) {
            Map<String, DataSourceDefinitionDto> definitionMap = buildDefinitionMapFromSheets(workbook,user);
            int sheetCount = workbook.getNumberOfSheets();
            for (int sheetIndex = 0; sheetIndex < sheetCount; sheetIndex++) {
                Sheet sheet = workbook.getSheetAt(sheetIndex);
                if (sheet == null) {
                    continue;
                }

                Row headerRow = sheet.getRow(0);
                if (headerRow == null) {
                    continue;
                }

                Map<Integer, String> columnMap = new LinkedHashMap<>();
                for (int i = 0; i < headerRow.getLastCellNum(); i++) {
                    Cell cell = headerRow.getCell(i);
                    if (cell != null) {
                        columnMap.put(i, getCellStringValue(cell));
                    }
                }

                for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                    Row dataRow = sheet.getRow(rowIndex);
                    if (dataRow == null) {
                        continue;
                    }

                    DataSourceConnectionDto conn = parseConnectionFromRow(dataRow, columnMap, definitionMap);
                    if (conn != null && StringUtils.isNotBlank(conn.getName())) {
                        connections.add(conn);
                    }
                }
            }
        }

        return connections;
    }

    /**
     * 从Excel行解析Connection
     */
    private static DataSourceConnectionDto parseConnectionFromRow(Row row, Map<Integer, String> columnMap,
            Map<String, DataSourceDefinitionDto> definitionMap) {
        DataSourceConnectionDto conn = new DataSourceConnectionDto();
        Map<String, Object> config = new LinkedHashMap<>();
        Map<String, String> valueMap = new LinkedHashMap<>();

        for (Map.Entry<Integer, String> entry : columnMap.entrySet()) {
            int colIndex = entry.getKey();
            String columnName = entry.getValue();
            Cell cell = row.getCell(colIndex);
            String value = getCellStringValue(cell);
            if (StringUtils.isNotBlank(columnName)) {
                valueMap.put(normalizeApiKeyHeader(columnName), value);
            }
        }

        for (Map.Entry<String, String> entry : valueMap.entrySet()) {
            String columnName = entry.getKey();
            String value = entry.getValue();
            if (StringUtils.isBlank(columnName)) {
                continue;
            }
            if (columnName.startsWith(CONFIG_COLUMN_PREFIX)) {
                String configKey = columnName.substring(CONFIG_COLUMN_PREFIX.length());
                if (StringUtils.isNotBlank(value)) {
                    setNestedValue(config, configKey, parseValue(value));
                }
                continue;
            }
            if (BASE_COLUMN_FIELDS.containsKey(columnName)) {
                setBaseColumnValue(conn, columnName, value);
            }
        }

        Map<String, String> apiServerKeyMap = getApiServerKeyToConfigKeyMap(conn, definitionMap);
        String deploymentModeValue = valueMap.get(DEPLOYMENT_MODE_KEY);
        for (Map.Entry<String, String> entry : valueMap.entrySet()) {
            String columnName = entry.getKey();
            String value = entry.getValue();
            if (StringUtils.isBlank(columnName) || StringUtils.isBlank(value)) {
                continue;
            }
            if (columnName.startsWith(CONFIG_COLUMN_PREFIX)) {
                continue;
            }
            if (BASE_COLUMN_FIELDS.containsKey(columnName)) {
                continue;
            }
            String configKey = apiServerKeyMap.get(columnName);
            if (StringUtils.isNotBlank(configKey)) {
                if (MASTER_SLAVE_ADDRESS_KEY.equals(columnName)
                        && !DEPLOYMENT_MODE_MASTER_SLAVE.equalsIgnoreCase(
                                StringUtils.defaultString(deploymentModeValue))) {
                    continue;
                }
                if (MASTER_SLAVE_ADDRESS_KEY.equals(columnName)) {
                    Object parsed = parseMasterSlaveAddress(value);
                    if (parsed != null) {
                        setNestedValue(config, configKey, parsed);
                    }
                    continue;
                }
                setNestedValue(config, configKey, parseValue(value));
            }
        }

        if (MapUtils.isNotEmpty(config)) {
            conn.setConfig(config);
        }

        return conn;
    }

    private static void fillConnectionSheet(Workbook workbook, String sheetName,
            List<DataSourceConnectionDto> connections,
            Map<String, DataSourceDefinitionDto> definitionMap,
            boolean mongoSheet) {
        Sheet sheet = workbook.createSheet(sheetName);

        ColumnLayout layout = buildColumnLayout(connections, definitionMap, mongoSheet);

        CellStyle headerStyleLocked = createHeaderStyle(workbook);
        headerStyleLocked.setLocked(true);
        headerStyleLocked.setWrapText(true);
        CellStyle headerStyleUnlocked = workbook.createCellStyle();
        headerStyleUnlocked.cloneStyleFrom(headerStyleLocked);
        headerStyleUnlocked.setLocked(false);
        headerStyleUnlocked.setWrapText(true);

        CellStyle dataStyleLocked = workbook.createCellStyle();
        dataStyleLocked.setLocked(true);
        CellStyle dataStyleUnlocked = workbook.createCellStyle();
        dataStyleUnlocked.setLocked(false);

        Row headerRow = sheet.createRow(0);
        headerRow.setHeight((short) -1);
        CreationHelper creationHelper = workbook.getCreationHelper();
        int colIndex = 0;
        Map<String, Integer> apiKeyColumnIndex = new HashMap<>();

        for (String col : PRIORITY_COLUMNS) {
            Cell cell = headerRow.createCell(colIndex++);
            cell.setCellValue(col);
            cell.setCellStyle(headerStyleLocked);
        }

        for (String apiKey : layout.orderedApiKeys) {
            Cell cell = headerRow.createCell(colIndex++);
            cell.setCellValue(displayApiKey(apiKey));
            cell.setCellStyle(headerStyleUnlocked);
            apiKeyColumnIndex.put(apiKey, colIndex - 1);
            if (layout.hiddenApiKeys.contains(apiKey)) {
                sheet.setColumnHidden(colIndex - 1, true);
            }
        }

        for (String col : OTHER_BASE_COLUMNS) {
            Cell cell = headerRow.createCell(colIndex++);
            cell.setCellValue(col);
            cell.setCellStyle(headerStyleUnlocked);
            sheet.setColumnHidden(colIndex - 1, true);
        }

        int rowIndex = 1;
        for (DataSourceConnectionDto conn : connections) {
            Row dataRow = sheet.createRow(rowIndex++);
            colIndex = 0;

            for (String col : PRIORITY_COLUMNS) {
                Cell cell = dataRow.createCell(colIndex++);
                cell.setCellValue(getStringValue(getBaseColumnValue(conn, col)));
                cell.setCellStyle(dataStyleLocked);
            }

            Map<String, Object> config = conn.getConfig();
            Map<String, String> apiServerKeyMap = getApiServerKeyToConfigKeyMap(conn, definitionMap);

            for (String apiKey : layout.orderedApiKeys) {
                Cell cell = dataRow.createCell(colIndex++);
                if (!apiServerKeyMap.containsKey(apiKey)) {
                    cell.setCellValue("");
                    cell.setCellStyle(dataStyleLocked);
                    addNotApplicableComment(sheet, cell, creationHelper);
                    continue;
                }
                if (MASTER_SLAVE_ADDRESS_KEY.equals(apiKey)) {
                    cell.setCellValue("");
                    cell.setCellStyle(dataStyleUnlocked);
                    addRequiredMasterSlaveComment(sheet, cell, creationHelper);
                    continue;
                }
                if (layout.sensitiveApiKeys.contains(apiKey)) {
                    cell.setCellValue("");
                    cell.setCellStyle(layout.hiddenApiKeys.contains(apiKey) ? dataStyleLocked : dataStyleUnlocked);
                    if (layout.hiddenApiKeys.contains(apiKey)) {
                        addNotApplicableComment(sheet, cell, creationHelper);
                    }
                    continue;
                }
                String configKey = apiServerKeyMap.get(apiKey);
                if (StringUtils.isNotBlank(configKey) && MapUtils.isNotEmpty(config)) {
                    Object value = getNestedValue(config, configKey);
                    cell.setCellValue(convertToString(value));
                }
                if (layout.hiddenApiKeys.contains(apiKey)) {
                    cell.setCellStyle(dataStyleLocked);
                    addNotApplicableComment(sheet, cell, creationHelper);
                } else {
                    cell.setCellStyle(dataStyleUnlocked);
                }
            }

            for (String col : OTHER_BASE_COLUMNS) {
                Cell cell = dataRow.createCell(colIndex++);
                cell.setCellValue(getStringValue(getBaseColumnValue(conn, col)));
                cell.setCellStyle(dataStyleUnlocked);
            }
        }

        for (int i = 0; i < colIndex; i++) {
            sheet.autoSizeColumn(i);
        }
        adjustHeaderColumnWidth(sheet, headerRow, colIndex);
        applyDataValidations(sheet, apiKeyColumnIndex, layout.enumOptions, connections.size());
        sheet.protectSheet(SHEET_PROTECT);
    }

    private static ColumnLayout buildColumnLayout(List<DataSourceConnectionDto> connections,
            Map<String, DataSourceDefinitionDto> definitionMap,
            boolean mongoSheet) {
        Map<String, Boolean> sensitiveMap = new LinkedHashMap<>();
        Map<String, List<String>> enumOptions = new LinkedHashMap<>();
        for (DataSourceConnectionDto conn : connections) {
            Map<String, String> apiServerKeyMap = getApiServerKeyToConfigKeyMap(conn, definitionMap);
            Map<String, List<String>> apiEnumOptions = getApiServerKeyEnumOptions(conn, definitionMap);
            for (Map.Entry<String, String> entry : apiServerKeyMap.entrySet()) {
                String apiKey = entry.getKey();
                String configKey = entry.getValue();
                if (mongoSheet && !MONGO_API_KEY_URI.equals(apiKey)) {
                    continue;
                }
                boolean sensitive = isSensitiveKey(configKey);
                if (!sensitiveMap.containsKey(apiKey)) {
                    sensitiveMap.put(apiKey, sensitive);
                } else if (sensitive) {
                    sensitiveMap.put(apiKey, true);
                }
                List<String> options = apiEnumOptions.get(apiKey);
                if (options != null && !options.isEmpty()) {
                    enumOptions.put(apiKey, options);
                }
            }
        }
        if (mongoSheet && !sensitiveMap.containsKey(MONGO_API_KEY_URI)) {
            sensitiveMap.put(MONGO_API_KEY_URI, isSensitiveKey("uri"));
        }

        List<String> apiKeys = new ArrayList<>(sensitiveMap.keySet());
        apiKeys.sort(apiKeyComparator());

        Set<String> sensitiveApiKeys = new HashSet<>();
        for (String apiKey : apiKeys) {
            if (Boolean.TRUE.equals(sensitiveMap.get(apiKey))) {
                sensitiveApiKeys.add(apiKey);
            }
        }
        Set<String> hiddenApiKeys = new HashSet<>();
        Set<String> preferredSet = new HashSet<>(PREFERRED_API_KEY_ORDER);
        for (String apiKey : apiKeys) {
            if (!preferredSet.contains(apiKey)) {
                hiddenApiKeys.add(apiKey);
            }
        }
        return new ColumnLayout(apiKeys, sensitiveApiKeys, hiddenApiKeys, enumOptions);
    }

    private static Map<String, String> getApiServerKeyToConfigKeyMap(DataSourceConnectionDto conn,
            Map<String, DataSourceDefinitionDto> definitionMap) {
        Map<String, String> apiServerKeyMap = new LinkedHashMap<>();
        DataSourceDefinitionDto definition = getDefinitionForConnection(conn, definitionMap);
        if (definition != null) {
            apiServerKeyMap.putAll(collectApiServerKeyMapping(definition));
        }
        if (isMongoDataSource(conn.getDatabase_type()) && !apiServerKeyMap.containsKey(MONGO_API_KEY_URI)) {
            apiServerKeyMap.put(MONGO_API_KEY_URI, "uri");
        }
        return apiServerKeyMap;
    }

    private static Map<String, List<String>> getApiServerKeyEnumOptions(DataSourceConnectionDto conn,
            Map<String, DataSourceDefinitionDto> definitionMap) {
        DataSourceDefinitionDto definition = getDefinitionForConnection(conn, definitionMap);
        if (definition == null) {
            return Collections.emptyMap();
        }
        return collectApiServerKeyEnumOptions(definition);
    }

    private static DataSourceDefinitionDto getDefinitionForConnection(DataSourceConnectionDto conn,
            Map<String, DataSourceDefinitionDto> definitionMap) {
        if (conn == null || definitionMap == null || definitionMap.isEmpty()) {
            return null;
        }
        if (StringUtils.isNotBlank(conn.getPdkHash())) {
            DataSourceDefinitionDto definition = definitionMap.get(conn.getPdkHash());
            if (definition != null) {
                return definition;
            }
        }
        if (StringUtils.isNotBlank(conn.getDatabase_type())) {
            return definitionMap.get(conn.getDatabase_type());
        }
        return null;
    }

    private static Map<String, String> collectApiServerKeyMapping(DataSourceDefinitionDto definition) {
        if (definition == null) {
            return Collections.emptyMap();
        }
        LinkedHashMap<String, Object> properties = definition.getProperties();
        if (properties == null) {
            return Collections.emptyMap();
        }
        Object connection = properties.get("connection");
        if (!(connection instanceof Map)) {
            return Collections.emptyMap();
        }
        Object connectionProperties = ((Map<String, Object>) connection).get("properties");
        if (!(connectionProperties instanceof Map)) {
            return Collections.emptyMap();
        }
        Map<String, String> apiServerKeyMap = new LinkedHashMap<>();
        Deque<MapNode> queue = new ArrayDeque<>();
        queue.add(new MapNode((Map<String, Object>) connectionProperties, ""));
        while (!queue.isEmpty()) {
            MapNode node = queue.poll();
            Map<String, Object> props = node.properties;
            if (props == null) {
                continue;
            }
            for (Map.Entry<String, Object> entry : props.entrySet()) {
                String key = node.prefix.isEmpty() ? entry.getKey() : node.prefix + "." + entry.getKey();
                if (!(entry.getValue() instanceof Map)) {
                    continue;
                }
                Map<String, Object> meta = (Map<String, Object>) entry.getValue();
                String apiKey = extractApiServerKey(meta, key);
                if (StringUtils.isNotBlank(apiKey)) {
                    apiServerKeyMap.putIfAbsent(apiKey, key);
                }
                Object childProperties = meta.get("properties");
                if (childProperties instanceof Map) {
                    queue.add(new MapNode((Map<String, Object>) childProperties, key));
                }
            }
        }
        return apiServerKeyMap;
    }

    private static Map<String, List<String>> collectApiServerKeyEnumOptions(DataSourceDefinitionDto definition) {
        if (definition == null) {
            return Collections.emptyMap();
        }
        LinkedHashMap<String, Object> properties = definition.getProperties();
        if (properties == null) {
            return Collections.emptyMap();
        }
        Object connection = properties.get("connection");
        if (!(connection instanceof Map)) {
            return Collections.emptyMap();
        }
        Object connectionProperties = ((Map<String, Object>) connection).get("properties");
        if (!(connectionProperties instanceof Map)) {
            return Collections.emptyMap();
        }
        Map<String, List<String>> enumOptions = new LinkedHashMap<>();
        Deque<MapNode> queue = new ArrayDeque<>();
        queue.add(new MapNode((Map<String, Object>) connectionProperties, ""));
        while (!queue.isEmpty()) {
            MapNode node = queue.poll();
            Map<String, Object> props = node.properties;
            if (props == null) {
                continue;
            }
            for (Map.Entry<String, Object> entry : props.entrySet()) {
                String key = node.prefix.isEmpty() ? entry.getKey() : node.prefix + "." + entry.getKey();
                if (!(entry.getValue() instanceof Map)) {
                    continue;
                }
                Map<String, Object> meta = (Map<String, Object>) entry.getValue();
                String apiKey = extractApiServerKey(meta, key);
                if (StringUtils.isNotBlank(apiKey)) {
                    List<String> values = extractEnumValues(meta.get("enum"));
                    if (!values.isEmpty()) {
                        enumOptions.putIfAbsent(apiKey, values);
                    }
                }
                Object childProperties = meta.get("properties");
                if (childProperties instanceof Map) {
                    queue.add(new MapNode((Map<String, Object>) childProperties, key));
                }
            }
        }
        return enumOptions;
    }

    private static Map<String, DataSourceDefinitionDto> buildDefinitionMap(List<DataSourceConnectionDto> connections,UserDetail user) {
        Set<String> databaseTypes = new LinkedHashSet<>();
        for (DataSourceConnectionDto conn : connections) {
            if (conn != null && StringUtils.isNotBlank(conn.getDatabase_type())) {
                databaseTypes.add(conn.getDatabase_type());
            }
        }
        return loadDefinitions(databaseTypes,user);
    }

    private static Map<String, DataSourceDefinitionDto> buildDefinitionMapFromSheets(Workbook workbook,UserDetail user) {
        Set<String> databaseTypes = new LinkedHashSet<>();
        int sheetCount = workbook.getNumberOfSheets();
        for (int sheetIndex = 0; sheetIndex < sheetCount; sheetIndex++) {
            Sheet sheet = workbook.getSheetAt(sheetIndex);
            if (sheet == null) {
                continue;
            }
            Row headerRow = sheet.getRow(0);
            if (headerRow == null) {
                continue;
            }
            int databaseTypeIndex = -1;
            for (int i = 0; i < headerRow.getLastCellNum(); i++) {
                Cell cell = headerRow.getCell(i);
                if (cell != null && "database_type".equals(getCellStringValue(cell))) {
                    databaseTypeIndex = i;
                    break;
                }
            }
            if (databaseTypeIndex < 0) {
                continue;
            }
            for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                Row dataRow = sheet.getRow(rowIndex);
                if (dataRow == null) {
                    continue;
                }
                String databaseType = getCellStringValue(dataRow.getCell(databaseTypeIndex));
                if (StringUtils.isNotBlank(databaseType)) {
                    databaseTypes.add(databaseType);
                }
            }
        }
        return loadDefinitions(databaseTypes,user);
    }

    private static Map<String, DataSourceDefinitionDto> loadDefinitions(Set<String> databaseTypes,UserDetail user) {
        if (databaseTypes == null || databaseTypes.isEmpty()) {
            return Collections.emptyMap();
        }
        DataSourceDefinitionService service = SpringUtil.getBean(DataSourceDefinitionService.class);
        if (service == null) {
            return Collections.emptyMap();
        }
        List<DataSourceDefinitionDto> definitions = service.getByDataSourceType(databaseTypes.stream().toList(), user);
        if (definitions == null) {
            return Collections.emptyMap();
        }
        Map<String, DataSourceDefinitionDto> definitionMap = new HashMap<>();
        for (DataSourceDefinitionDto definition : definitions) {
            if (definition == null) {
                continue;
            }
            if (StringUtils.isNotBlank(definition.getPdkHash())) {
                definitionMap.putIfAbsent(definition.getPdkHash(), definition);
            }
        }
        return definitionMap;
    }

    private static boolean isMongoDataSource(String databaseType) {
        return StringUtils.isNotBlank(databaseType)
                && databaseType.toLowerCase(Locale.ROOT).contains("mongo");
    }

    private static void adjustHeaderColumnWidth(Sheet sheet, Row headerRow, int columnCount) {
        if (headerRow == null) {
            return;
        }
        for (int i = 0; i < columnCount; i++) {
            Cell cell = headerRow.getCell(i);
            if (cell == null) {
                continue;
            }
            String value = cell.getStringCellValue();
            if (StringUtils.isBlank(value)) {
                continue;
            }
            int minWidth = Math.min(255, value.length() + 2) * 256;
            int expandedWidth = (int) Math.min(255 * 256L, Math.round(sheet.getColumnWidth(i) * 1.5));
            int targetWidth = Math.max(minWidth, expandedWidth);
            if (sheet.getColumnWidth(i) < targetWidth) {
                sheet.setColumnWidth(i, targetWidth);
            }
        }
    }

    private static void addNotApplicableComment(Sheet sheet, Cell cell, CreationHelper creationHelper) {
        Drawing<?> drawing = sheet.createDrawingPatriarch();
        ClientAnchor anchor = creationHelper.createClientAnchor();
        anchor.setCol1(cell.getColumnIndex());
        anchor.setCol2(cell.getColumnIndex() + 2);
        anchor.setRow1(cell.getRowIndex());
        anchor.setRow2(cell.getRowIndex() + 2);
        Comment comment = drawing.createCellComment(anchor);
        comment.setString(creationHelper.createRichTextString("No need to fill in"));
        cell.setCellComment(comment);
    }

    private static void addRequiredMasterSlaveComment(Sheet sheet, Cell cell, CreationHelper creationHelper) {
        Drawing<?> drawing = sheet.createDrawingPatriarch();
        ClientAnchor anchor = creationHelper.createClientAnchor();
        anchor.setCol1(cell.getColumnIndex());
        anchor.setCol2(cell.getColumnIndex() + 3);
        anchor.setRow1(cell.getRowIndex());
        anchor.setRow2(cell.getRowIndex() + 3);
        Comment comment = drawing.createCellComment(anchor);
        comment.setString(creationHelper.createRichTextString(
                "Mandatory when deploymentMode is set to master-slave. Format: master_host:master_port,slave_host:slave_port"));
        cell.setCellComment(comment);
    }

    private static void applyDataValidations(Sheet sheet, Map<String, Integer> apiKeyColumnIndex,
            Map<String, List<String>> enumOptions, int rowCount) {
        if (apiKeyColumnIndex == null || enumOptions == null || enumOptions.isEmpty()) {
            return;
        }
        Integer columnIndex = apiKeyColumnIndex.get(DEPLOYMENT_MODE_KEY);
        List<String> options = enumOptions.get(DEPLOYMENT_MODE_KEY);
        if (columnIndex == null || options == null || options.isEmpty()) {
            return;
        }
        DataValidationHelper helper = sheet.getDataValidationHelper();
        DataValidationConstraint constraint = helper.createExplicitListConstraint(options.toArray(new String[0]));
        int lastRow = Math.max(1, rowCount);
        CellRangeAddressList addressList = new CellRangeAddressList(1, lastRow, columnIndex, columnIndex);
        DataValidation validation = helper.createValidation(constraint, addressList);
        validation.setShowErrorBox(true);
        sheet.addValidationData(validation);
    }

    private static String displayApiKey(String apiKey) {
        if (apiKey == null) {
            return "";
        }
        String normalized = apiKey.trim();
        if ("database_owner".equals(normalized)) {
            return "database_schema";
        }
        return normalized;
    }

    private static Comparator<String> apiKeyComparator() {
        Map<String, Integer> orderIndex = new HashMap<>();
        for (int i = 0; i < PREFERRED_API_KEY_ORDER.size(); i++) {
            orderIndex.put(PREFERRED_API_KEY_ORDER.get(i), i);
        }
        return (a, b) -> {
            Integer ai = orderIndex.get(a);
            Integer bi = orderIndex.get(b);
            if (ai != null && bi != null) {
                return Integer.compare(ai, bi);
            }
            if (ai != null) {
                return -1;
            }
            if (bi != null) {
                return 1;
            }
            return a.compareTo(b);
        };
    }

    private static String extractApiServerKey(Map<String, Object> meta, String key) {
        Object apiServerKey = meta.get("apiServerKey");
        if (apiServerKey instanceof String) {
            String apiKey = ((String) apiServerKey).trim();
            if (StringUtils.isNotBlank(apiKey)) {
                return apiKey;
            }
        }
        return key;
    }

    private static List<String> extractEnumValues(Object enumObj) {
        if (!(enumObj instanceof List)) {
            return Collections.emptyList();
        }
        List<String> values = new ArrayList<>();
        for (Object item : (List<?>) enumObj) {
            if (item instanceof Map) {
                Object value = ((Map<?, ?>) item).get("value");
                if (value != null) {
                    values.add(String.valueOf(value));
                }
            } else if (item != null) {
                values.add(String.valueOf(item));
            }
        }
        return values;
    }

    private static Object parseMasterSlaveAddress(String value) {
        if (StringUtils.isBlank(value)) {
            return null;
        }
        String[] parts = value.split(",");
        List<Map<String, Object>> results = new ArrayList<>();
        for (String part : parts) {
            String trimmed = part.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String[] hostPort = trimmed.split(":");
            if (hostPort.length < 2) {
                continue;
            }
            String host = hostPort[0].trim();
            String portStr = hostPort[1].trim();
            if (StringUtils.isBlank(host) || StringUtils.isBlank(portStr)) {
                continue;
            }
            Integer port;
            try {
                port = Integer.parseInt(portStr);
            } catch (NumberFormatException ignored) {
                continue;
            }
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("host", host);
            entry.put("port", port);
            results.add(entry);
        }
        return results.isEmpty() ? null : results;
    }

    private static String normalizeApiKeyHeader(String columnName) {
        if (columnName == null) {
            return "";
        }
        String normalized = columnName.trim();
        if ("database_schema".equals(normalized)) {
            return "database_owner";
        }
        return normalized;
    }

    private static String getConfigString(Map<String, Object> config, String key) {
        if (StringUtils.isBlank(key) || MapUtils.isEmpty(config)) {
            return null;
        }
        Object value = getNestedValue(config, key);
        return value == null ? null : String.valueOf(value);
    }

    private static class ColumnLayout {
        private final List<String> orderedApiKeys;
        private final Set<String> sensitiveApiKeys;
        private final Set<String> hiddenApiKeys;
        private final Map<String, List<String>> enumOptions;

        private ColumnLayout(List<String> orderedApiKeys, Set<String> sensitiveApiKeys,
                Set<String> hiddenApiKeys, Map<String, List<String>> enumOptions) {
            this.orderedApiKeys = orderedApiKeys;
            this.sensitiveApiKeys = sensitiveApiKeys;
            this.hiddenApiKeys = hiddenApiKeys;
            this.enumOptions = enumOptions;
        }
    }

    private static class MapNode {
        private final Map<String, Object> properties;
        private final String prefix;

        private MapNode(Map<String, Object> properties, String prefix) {
            this.properties = properties;
            this.prefix = prefix;
        }
    }

    /**
     * 设置基础列值
     */
    private static void setBaseColumnValue(DataSourceConnectionDto conn, String columnName, String value) {
        if (StringUtils.isBlank(value)) {
            return;
        }
        Field field = BASE_COLUMN_FIELDS.get(columnName);
        if (field == null) {
            return;
        }
        Object converted = convertStringToFieldType(value, field.getType());
        if (converted == null && field.getType().isPrimitive()) {
            return;
        }
        try {
            field.set(conn, converted);
        } catch (IllegalAccessException ignored) {
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
        if (value instanceof ObjectId) {
            return ((ObjectId) value).toHexString();
        }
        if (value instanceof String) {
            return (String) value;
        } else {
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

    private static Object getBaseColumnValue(DataSourceConnectionDto conn, String columnName) {
        Field field = BASE_COLUMN_FIELDS.get(columnName);
        if (field == null) {
            return null;
        }
        try {
            return field.get(conn);
        } catch (IllegalAccessException ignored) {
            return null;
        }
    }

    private static Map<String, Field> buildBaseColumnFields() {
        List<String> columns = new ArrayList<>(PRIORITY_COLUMNS.size() + OTHER_BASE_COLUMNS.size());
        columns.addAll(PRIORITY_COLUMNS);
        columns.addAll(OTHER_BASE_COLUMNS);
        Map<String, Field> fields = new LinkedHashMap<>();
        for (String column : columns) {
            Field field = findField(DataSourceConnectionDto.class, column);
            if (field != null) {
                field.setAccessible(true);
            }
            fields.put(column, field);
        }
        return fields;
    }

    private static Field findField(Class<?> type, String name) {
        Class<?> current = type;
        while (current != null) {
            try {
                return current.getDeclaredField(name);
            } catch (NoSuchFieldException ignored) {
                current = current.getSuperclass();
            }
        }
        return null;
    }

    private static Object convertStringToFieldType(String value, Class<?> fieldType) {
        if (String.class.equals(fieldType)) {
            return value;
        }
        if (ObjectId.class.equals(fieldType)) {
            return MongoUtils.toObjectId(value);
        }
        if (fieldType.isEnum()) {
            try {
                return Enum.valueOf((Class<Enum>) fieldType, value);
            } catch (IllegalArgumentException ignored) {
                return null;
            }
        }
        if (Boolean.class.equals(fieldType) || boolean.class.equals(fieldType)) {
            return Boolean.parseBoolean(value);
        }
        if (Integer.class.equals(fieldType) || int.class.equals(fieldType)) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        if (Long.class.equals(fieldType) || long.class.equals(fieldType)) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        if (Double.class.equals(fieldType) || double.class.equals(fieldType)) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        if (Float.class.equals(fieldType) || float.class.equals(fieldType)) {
            try {
                return Float.parseFloat(value);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        if (Short.class.equals(fieldType) || short.class.equals(fieldType)) {
            try {
                return Short.parseShort(value);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        if (Byte.class.equals(fieldType) || byte.class.equals(fieldType)) {
            try {
                return Byte.parseByte(value);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        if (Date.class.equals(fieldType)) {
            try {
                return new Date(Long.parseLong(value));
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        if (Collection.class.isAssignableFrom(fieldType) || Map.class.isAssignableFrom(fieldType)) {
            try {
                return JsonUtil.parseJsonUseJackson(value, Object.class);
            } catch (Exception ignored) {
                return null;
            }
        }
        try {
            return JsonUtil.parseJsonUseJackson(value, fieldType);
        } catch (Exception ignored) {
            return null;
        }
    }
}
