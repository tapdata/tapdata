package io.tapdata.pdk.cli.commands;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.conversion.TargetTypesGenerator;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.TypeExprResult;
import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.mapping.type.TapStringMapping;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVMapFactory;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.cli.CommonCli;
import io.tapdata.pdk.cli.support.DataTypesHandler;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.connector.TapConnector;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.apache.maven.shared.invoker.*;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFCreationHelper;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import picocli.CommandLine;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.text.MessageFormat;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static io.tapdata.entity.simplify.TapSimplify.field;
import static io.tapdata.entity.simplify.TapSimplify.table;

@CommandLine.Command(
        description = "Model prediction method",
        subcommands = MainCli.class
)
public class ModelPredictionCli extends CommonCli {
    private static final String TAG = ModelPredictionCli.class.getSimpleName();

    @CommandLine.Parameters(paramLabel = "FILE", description = "One ore more pdk jar files")
    File[] files;

    @CommandLine.Option(names = {"-o", "--output"}, required = true, description = "Specify the folder where model deduction report files will be generated")
    private String output;
    @CommandLine.Option(names = {"-m", "--mavenHome"}, required = false, description = "Specify the maven home")
    private String mavenHome;
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "TapData cli help")
    private boolean helpRequested = false;

    private static final String FIELD_EXACT = "exact_";

    DataTypesHandler handler;

    public Integer execute() throws Exception {
        CommonUtils.setProperty("refresh_local_jars", "true");
        File outputFile = new File(output);
        if (outputFile.isFile())
            throw new IllegalArgumentException("");
        if (!outputFile.exists())
            FileUtils.forceMkdir(outputFile);
        if (!output.endsWith(File.separator)) {
            output = output + File.separator;
        }
        List<File> jarFiles = new ArrayList<>();

        handler = DataTypesHandler.create();

        for (File file : files) {
            String jarFile = null;
            if (file.isFile()) {
                jarFile = file.getAbsolutePath();
            } else if (file.isDirectory()) {
                if (!file.getAbsolutePath().contains("connectors")) {
                    throw new IllegalArgumentException("Connector project is under connectors directory, are you passing the correct connector project directory? " + file.getAbsolutePath());
                }
                System.setProperty("maven.home", getMavenHome(this.mavenHome));
                String pomFile = file.getAbsolutePath();
                if (!pomFile.endsWith("pom.xml")) {
                    pomFile = pomFile + File.separator + "pom.xml";
                }
//                    int state = mavenCli.doMain(new String[]{"install", "-f", pomFile}, "./", System.out, System.out);
                System.out.println(file.getName() + " is packaging...");
                InvocationRequest request = new DefaultInvocationRequest();
                request.setPomFile(new File(pomFile));
                request.setGoals(Collections.singletonList("package"));

                Invoker invoker = new DefaultInvoker();
                InvocationResult result = invoker.execute(request);

                if (result.getExitCode() != 0) {
                    if (result.getExecutionException() != null)
                        System.out.println(result.getExecutionException().getMessage());
                    System.out.println("------------- Project " + pomFile + " package Failed --------------");
                    System.exit(0);
                } else {
                    System.out.println("------------- Project " + pomFile + " package successfully -------------");
                    MavenXpp3Reader reader = new MavenXpp3Reader();
                    Model model = reader.read(new FileReader(FilenameUtils.concat(file.getAbsolutePath(), "pom.xml")));
                    jarFile = FilenameUtils.concat("./connectors", "./dist/" + model.getArtifactId() + "-v" + model.getVersion() + ".jar");
                }
            } else {
                throw new IllegalArgumentException("File " + file.getAbsolutePath() + " is not exist");
            }
            File theJarFile = new File(jarFile);
            if (!theJarFile.exists()) {
                throw new IllegalArgumentException("Packaged jar file " + jarFile + " not exists");
            }
            jarFiles.add(theJarFile);
//            CommonUtils.setProperty("pdk_test_jar_file", jarFile);
//            CommonUtils.setProperty("pdk_test_config_file", testConfig);
        }
        TapConnectorManager.getInstance().start(jarFiles);
        List<ConnectorNode> connectorNodes = new ArrayList<>();
        for (File file : jarFiles) {
            TapConnector testConnector = TapConnectorManager.getInstance().getTapConnectorByJarName(file.getName());
            Collection<TapNodeInfo> tapNodeInfoCollection = testConnector.getTapNodeClassFactory().getConnectorTapNodeInfos();
            for (TapNodeInfo nodeInfo : tapNodeInfoCollection) {
                TapNodeSpecification specification = nodeInfo.getTapNodeSpecification();
                String dagId = UUID.randomUUID().toString();
                KVMap<TapTable> kvMap = InstanceFactory.instance(KVMapFactory.class).getCacheMap(dagId, TapTable.class);
                KVMap<Object> stateMap = new KVMap<Object>() {
                    @Override
                    public void init(String mapKey, Class<Object> valueClass) {

                    }

                    @Override
                    public void put(String key, Object o) {

                    }

                    @Override
                    public Object putIfAbsent(String key, Object o) {
                        return null;
                    }

                    @Override
                    public Object remove(String key) {
                        return null;
                    }

                    @Override
                    public void clear() {

                    }

                    @Override
                    public void reset() {

                    }

                    @Override
                    public Object get(String key) {
                        return null;
                    }
                };//InstanceFactory.instance(KVMapFactory.class).getCacheMap(dagId, Object.class);
                ConnectorNode node = PDKIntegration.createConnectorBuilder()
                        .withDagId(dagId)
                        .withAssociateId("test")
                        .withGroup(specification.getGroup())
                        .withPdkId(specification.getId())
                        .withVersion(specification.getVersion())
                        .withTableMap(kvMap)
                        .withStateMap(stateMap)
                        .withGlobalStateMap(stateMap)
                        .build();
                PDKInvocationMonitor.getInstance().invokePDKMethod(node, PDKMethod.REGISTER_CAPABILITIES,
                        node::registerCapabilities,
                        MessageFormat.format("call registerCapabilities functions {0} associateId {1}", specification.idAndGroup(), "test"), TAG);
//                node.registerCapabilities();
                connectorNodes.add(node);
            }
        }

        for (ConnectorNode sourceNode : connectorNodes) {
            TapTable generatedTable = generateAllTypesTable(sourceNode);
            if (generatedTable == null)
                throw new NullPointerException("Generate all types for source " + sourceNode + " failed");

            for (ConnectorNode targetNode : connectorNodes) {
                if (!sourceNode.equals(targetNode)) {
                    TapResult<LinkedHashMap<String, TapField>> result = InstanceFactory.instance(TargetTypesGenerator.class).convert(generatedTable.getNameFieldMap(), targetNode.getConnectorContext().getSpecification().getDataTypesMap(), targetNode.getCodecsFilterManager());
                    if (result != null && result.getData() != null) {
                        outputCSVReport(sourceNode, generatedTable.getNameFieldMap(), targetNode, result.getData());
                    }
                }
            }
        }
        String file = writeWorkbook();
        TapLogger.info(TAG, "Excel file has been written in {}", file);
        System.exit(1);
        return 0;
    }

    private void specialDataTypesToTest(ConnectorNode sourceNode, TapTable generatedTable) {
        if (sourceNode.getConnectorContext().getSpecification().getId().equals("mysql")) {
            generatedTable.add(field("special_int(11)", "int(11)"));
            generatedTable.add(field("special_enum()", "enum('03TapMongoDB','04TapDB2','05TapPostgres','06TapSqlserver')"));
            generatedTable.add(field("special_mediumint(9)", "mediumint(9)"));
            generatedTable.add(field("special_mediumint(8)_unsigned", "mediumint(8) unsigned"));
            generatedTable.add(field("special_tinyint(4)", "tinyint(4)"));
            generatedTable.add(field("special_tinyint(3)_unsigned", "tinyint(3) unsigned"));
            generatedTable.add(field("special_tinyint(1)", "tinyint(1)"));
            generatedTable.add(field("special_bit(1)", "bit(1)"));
            generatedTable.add(field("special_bit(32)", "bit(32)"));
            generatedTable.add(field("special_bit(64)", "bit(64)"));
            generatedTable.add(field("special_smallint(6)", "smallint(6)"));
            generatedTable.add(field("special_smallint(5)_unsigned", "smallint(5) unsigned"));
            generatedTable.add(field("special_mediumint(9)", "mediumint(9)"));
            generatedTable.add(field("special_mediumint(8)_unsigned", "mediumint(8) unsigned"));
            generatedTable.add(field("special_int(10)_unsigned", "int(10) unsigned"));
            generatedTable.add(field("special_bigint(20)", "bigint(20)"));
            generatedTable.add(field("special_bigint(20)_unsigned", "bigint(20) unsigned"));
            generatedTable.add(field("special_varbinary(10)", "varbinary(10)"));
            generatedTable.add(field("special_varbinary(255)", "varbinary(255)"));
            generatedTable.add(field("special_set()", "set('03TapMongoDB','04TapDB2','05TapPostgres')"));
            generatedTable.add(field("special_char(10)", "char(10)"));
            generatedTable.add(field("special_char(255)", "char(255)"));
            generatedTable.add(field("special_varchar(10)", "varchar(10)"));
            generatedTable.add(field("special_varchar(255)", "varchar(255)"));
            generatedTable.add(field("special_decimal(30,5)", "decimal(30,5)"));
        }

        if (sourceNode.getConnectorContext().getSpecification().getId().equals("mongodb")) {
            generatedTable.add(field("special_string_id_primary", "STRING").primaryKeyPos(generatedTable.getNameFieldMap().size()));
        }

        if (sourceNode.getConnectorContext().getSpecification().getId().equals("oracle")) {
            generatedTable.add(field("special_NUMBER(38,-3)", "NUMBER(38,-3)"));
            generatedTable.add(field("special_NUMBER(38,-80)", "NUMBER(38,-80)"));
        }
    }

    private String writeWorkbook() {
        if (workbook == null)
            return null;
        String file = output + "TypesMappingReport_" + dateTime() + ".xlsx";
        try {
            //Write the workbook in file system
            FileOutputStream out = FileUtils.openOutputStream(new File(file));
            workbook.write(out);
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return file;
    }

    private String dateTime() {
        return DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss_SSS").withZone(ZoneId.systemDefault()).format(new Date().toInstant());
    }

    XSSFWorkbook workbook;

    private void outputCSVReport(ConnectorNode sourceNode, LinkedHashMap<String, TapField> sourceNameFieldMap, ConnectorNode targetNode, LinkedHashMap<String, TapField> targetNameFieldMap) {
        //Blank workbook
        if (workbook == null)
            workbook = new XSSFWorkbook();
        CreationHelper creationHelper = (XSSFCreationHelper) workbook.getCreationHelper();

        //Create a blank sheet
        XSSFSheet sheet = workbook.createSheet(sourceNode.getConnectorContext().getSpecification().getName() + " -> " + targetNode.getConnectorContext().getSpecification().getName());
        Drawing drawing = sheet.createDrawingPatriarch();
        //This data needs to be written (Object[])
        Map<String, Object[]> data = new LinkedHashMap<>();
        String sourceName = sourceNode.getConnectorContext().getSpecification().getName();
        String targetName = targetNode.getConnectorContext().getSpecification().getName();
        data.put("1", new Object[]{"No.", "Field name", sourceName + " type", sourceName + " tap type", sourceName + " params", sourceName + " expression", sourceName + " model", " to ", targetName + " model", targetName + " type", targetName + " tap type", targetName + " params", targetName + " expression"});
        Set<Map.Entry<String, TapField>> entries = sourceNameFieldMap.entrySet();
        int index = 0;
        for (Map.Entry<String, TapField> entry : entries) {
            TapField targetField = targetNameFieldMap.get(entry.getKey());
            data.put(String.valueOf(index + 2), new Object[]{index + 1, entry.getKey(),
                    getDataTypeName(sourceNode, entry.getValue()), getTapType(sourceNode, entry.getValue()), getParams(sourceNode, entry.getValue()), getExpression(sourceNode, entry.getValue()), entry.getValue().getDataType(),
                    "-->",
                    targetField.getDataType(), getDataTypeName(targetNode, targetField), getTapType(targetNode, targetField), getParams(targetNode, targetField), getExpression(targetNode, targetField),
            });
            index++;
        }
        int sourceCommendIndex = 6;
        int targetCommendIndex = 8;

        int sourceParamsIndex = 4;
        int targetParamsIndex = 11;

        //https://www.baeldung.com/apache-poi-change-cell-font
        //Iterate over data and write to sheet
        Set<String> keyset = data.keySet();
        int rownum = 0;
        for (String key : keyset) {
            Row row = sheet.createRow(rownum++);
            Object[] objArr = data.get(key);
            int cellnum = 0;
            for (Object obj : objArr) {
                CellStyle cellStyle = workbook.createCellStyle();

                Cell cell = row.createCell(cellnum++);
//                sheet.setDefaultRowHeight((short) 2000);
                if (cellnum - 1 == sourceParamsIndex || cellnum - 1 == targetParamsIndex) {
                    sheet.setColumnWidth(cellnum - 1, 4000);
//                    cellStyle.setAlignment(HorizontalAlignment.LEFT);
                } else {
                    sheet.autoSizeColumn(cellnum - 1);
                }
                if (sourceCommendIndex == cellnum - 1) {
                    TypeExprResult<DataMap> result = addCellComment(creationHelper, drawing, cell, sourceNode, (String) obj);
                }
                if (targetCommendIndex == cellnum - 1) {
                    TypeExprResult<DataMap> result = addCellComment(creationHelper, drawing, cell, targetNode, (String) obj);
                }

                if (sourceCommendIndex == cellnum - 1 || targetCommendIndex == cellnum - 1) {
                    cellStyle.setFillForegroundColor(IndexedColors.LIGHT_YELLOW.getIndex());
                    cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
                }
                if (rownum == 1) {
                    cellStyle.setFillForegroundColor(IndexedColors.LIGHT_GREEN.getIndex());
                    cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
                    cellStyle.setLocked(true);
                }

                cell.setCellStyle(cellStyle);
                if (obj instanceof String)
                    cell.setCellValue((String) obj);
                else if (obj instanceof Integer)
                    cell.setCellValue((Integer) obj);
            }
        }

    }

    private Object getExpression(ConnectorNode connectorNode, TapField field) {
        TypeExprResult<DataMap> result = connectorNode.getConnectorContext().getSpecification().getDataTypesMap().get(field.getDataType());
        if (result == null)
            return null;
        return result.getExpression();
    }

    private Object getParams(ConnectorNode connectorNode, TapField field) {
        TypeExprResult<DataMap> result = connectorNode.getConnectorContext().getSpecification().getDataTypesMap().get(field.getDataType());
        if (result == null)
            return null;
        Object value = result.getValue().get(TapMapping.FIELD_TYPE_MAPPING);
        if (value instanceof TapMapping) {
            TapMapping map = (TapMapping) value;
            return JSON.toJSONString(map, true);
        }
        return null;
    }

    private Object getTapType(ConnectorNode connectorNode, TapField field) {
        TypeExprResult<DataMap> result = connectorNode.getConnectorContext().getSpecification().getDataTypesMap().get(field.getDataType());
        if (result == null)
            return null;
        Object value = result.getValue().get(TapMapping.FIELD_TYPE_MAPPING);
        if (value instanceof TapMapping) {
            TapMapping map = (TapMapping) value;
            return map.getTo();
        }
        return null;
    }

    private String getDataTypeName(ConnectorNode connectorNode, TapField field) {
        String dataType = field.getDataType();
        if (dataType == null)
            return "";
        DefaultExpressionMatchingMap expressionMatchingMap = connectorNode.getConnectorContext().getSpecification().getDataTypesMap();
        TypeExprResult<DataMap> result = expressionMatchingMap.get(dataType);
        if (result != null) {
            TapMapping tapMapping = (TapMapping) result.getValue().get(TapMapping.FIELD_TYPE_MAPPING);
            return handler.dataNameClassHandlers().handle(tapMapping, result.getExpression());
        }
        return "";
    }

    private TypeExprResult<DataMap> addCellComment(CreationHelper creationHelper, Drawing drawing, Cell cell, ConnectorNode connectorNode, String dataType) {
        TypeExprResult<DataMap> result = connectorNode.getConnectorContext().getSpecification().getDataTypesMap().get(dataType);
        if (result == null)
            return null;

        ClientAnchor clientAnchor = drawing.createAnchor(0, 0, 0, 0, 0, 2, 7, 12);
        Comment comment = drawing.createCellComment(clientAnchor);
        RichTextString richTextString = creationHelper.createRichTextString("Expression: " + result.getExpression() + "\r\n" +
                "TapMapping: " + JSON.toJSONString(result.getValue().get(TapMapping.FIELD_TYPE_MAPPING), true));
        comment.setString(richTextString);
        comment.setAuthor("Aplomb");
        cell.setCellComment(comment);
        return result;
    }

    private TapTable generateAllTypesTable(ConnectorNode node) {
        final TapTable tapTable = table(node.getAssociateId());
        DefaultExpressionMatchingMap expressionMatchingMap = node.getConnectorContext().getSpecification().getDataTypesMap();
        expressionMatchingMap.iterate(expressionDataMapEntry -> {
            String expression = expressionDataMapEntry.getKey();
            tapTable.add(field(FIELD_EXACT + expression, expression));
            return false;
        }, DefaultExpressionMatchingMap.ITERATE_TYPE_EXACTLY_ONLY);
        expressionMatchingMap.iterate(expressionDataMapEntry -> {
            String expression = expressionDataMapEntry.getKey();
            DataMap dataMap = expressionDataMapEntry.getValue();
            TapMapping tapMapping = (TapMapping) dataMap.get(TapMapping.FIELD_TYPE_MAPPING);
            handler.fillTestFields(tapTable, expression, tapMapping);
            return false;
        }, DefaultExpressionMatchingMap.ITERATE_TYPE_PREFIX_ONLY);
        specialDataTypesToTest(node, tapTable);
        InstanceFactory.instance(TableFieldTypesGenerator.class).autoFill(tapTable.getNameFieldMap(), expressionMatchingMap);
        return tapTable;
    }
}
