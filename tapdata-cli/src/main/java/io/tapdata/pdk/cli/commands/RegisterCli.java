package io.tapdata.pdk.cli.commands;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.cli.CommonCli;
import io.tapdata.pdk.cli.services.UploadFileService;
import io.tapdata.pdk.core.connector.TapConnector;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.pdk.core.constants.DataSourceQCType;
import io.tapdata.pdk.core.tapnode.TapNodeContainer;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.IOUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

@CommandLine.Command(
        description = "Push PDK jar file into Tapdata",
        subcommands = MainCli.class
)
public class RegisterCli extends CommonCli {
    private static final String TAG = RegisterCli.class.getSimpleName();
    @CommandLine.Parameters(paramLabel = "FILE", description = "One or more pdk jar files")
    File[] files;

    @CommandLine.Option(names = {"-l", "--latest"}, required = false, defaultValue = "true", description = "whether replace the latest version")
    private boolean latest;

    @CommandLine.Option(names = {"-a", "--auth"}, required = false, description = "Provide auth token to register")
    private String authToken;

    @CommandLine.Option(names = {"-ak", "--accessKey"}, required = false, description = "Provide auth accessKey")
    private String ak;

    @CommandLine.Option(names = {"-sk", "--secretKey"}, required = false, description = "Provide auth secretKey")
    private String sk;

    @CommandLine.Option(names = {"-t", "--tm"}, required = true, description = "Tapdata TM url")
    private String tmUrl;

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "TapData cli help")
    private boolean helpRequested = false;

    @CommandLine.Option(names = {"-r", "--replace"}, required = false, description = "Replace Config file name")
    private String replaceName;

    //    private SummaryGeneratingListener listener = new SummaryGeneratingListener();
//    public void runOne() {
//        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
//                .selectors(selectClass("io.tapdata.pdk.tdd.tests.basic.ConnectionTestTest"))
//                .build();
//        Launcher launcher = LauncherFactory.create();
//        TestPlan testPlan = launcher.discover(request);
//        launcher.registerTestExecutionListeners(listener);
//        launcher.execute(request);
//
//        TestExecutionSummary summary = listener.getSummary();
//        summary.printTo(new PrintWriter(System.out));
//    }
//
//    public void runAll() {
//        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
//                .selectors(selectPackage("io.tapdata.pdk.tdd.tests"))
//                .filters(includeClassNamePatterns(".*Test"))
//                .build();
//        Launcher launcher = LauncherFactory.create();
//        TestPlan testPlan = launcher.discover(request);
//        launcher.registerTestExecutionListeners(listener);
//        launcher.execute(request);
//
//        TestExecutionSummary summary = listener.getSummary();
//        summary.printTo(new PrintWriter(System.out));
//    }
    public Integer execute() throws Exception {
//        runOne();
        try {
            CommonUtils.setProperty("refresh_local_jars", "true");
            TapConnectorManager.getInstance().start(Arrays.asList(files));

            for (File file : files) {
                List<String> jsons = new ArrayList<>();
                TapConnector connector = TapConnectorManager.getInstance().getTapConnectorByJarName(file.getName());
                Collection<TapNodeInfo> tapNodeInfoCollection = connector.getTapNodeClassFactory().getConnectorTapNodeInfos();
                Map<String, InputStream> inputStreamMap = new HashMap<>();
                for (TapNodeInfo nodeInfo : tapNodeInfoCollection) {
                    TapNodeSpecification specification = nodeInfo.getTapNodeSpecification();
                    String iconPath = specification.getIcon();
                    if (StringUtils.isNotBlank(iconPath)) {
                        InputStream is = nodeInfo.readResource(iconPath);
                        if (is != null) {
                            inputStreamMap.put(iconPath, is);
                        }
                    }

                    JSONObject o = (JSONObject) JSON.toJSON(specification);
                    DataSourceQCType qcType = DataSourceQCType.parse(specification.getManifest().get("Authentication"));
                    qcType = (null == qcType) ? DataSourceQCType.Alpha : qcType;
                    o.put("qcType", qcType);

                    String pdkAPIVersion = specification.getManifest().get("PDK-API-Version");
                    int pdkAPIBuildNumber = CommonUtils.getPdkBuildNumer(pdkAPIVersion);
                    o.put("pdkAPIVersion", pdkAPIVersion);
                    o.put("pdkAPIBuildNumber", pdkAPIBuildNumber);

                    o.put("beta", "beta".equals(specification.getManifest().get("Authentication")));
                    String nodeType = null;
                    switch (nodeInfo.getNodeType()) {
                        case TapNodeInfo.NODE_TYPE_SOURCE:
                            nodeType = "source";
                            break;
                        case TapNodeInfo.NODE_TYPE_SOURCE_TARGET:
                            nodeType = "source_and_target";
                            break;
                        case TapNodeInfo.NODE_TYPE_TARGET:
                            nodeType = "target";
                            break;
                        case TapNodeInfo.NODE_TYPE_PROCESSOR:
                            nodeType = "processor";
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown node type " + nodeInfo.getNodeType());
                    }
                    o.put("type", nodeType);
                    // get the version info and group info from jar
                    o.put("version", nodeInfo.getNodeClass().getPackage().getImplementationVersion());
                    o.put("group", nodeInfo.getNodeClass().getPackage().getImplementationVendor());

                    TapNodeContainer nodeContainer = JSON.parseObject(IOUtils.toString(nodeInfo.readResource(nodeInfo.getNodeClass().getAnnotation(TapConnectorClass.class).value())), TapNodeContainer.class);
                    if(nodeContainer.getDataTypes() == null) {
                        try (InputStream dataTypeInputStream = this.getClass().getClassLoader().getResourceAsStream("default-data-types.json")) {
                            if (dataTypeInputStream != null) {
                                String dataTypesJson = org.apache.commons.io.IOUtils.toString(dataTypeInputStream, StandardCharsets.UTF_8);
                                if (StringUtils.isNotBlank(dataTypesJson)) {
                                    TapNodeContainer container = InstanceFactory.instance(JsonParser.class).fromJson(dataTypesJson, TapNodeContainer.class);
                                    if (container != null && container.getDataTypes() != null)
                                        nodeContainer.setDataTypes(container.getDataTypes());
                                }
                            }
                        }
                    }
                    Map<String, Object> messsages = nodeContainer.getMessages();
                    String replacePath = null;//"replace_default";
                    Map<String, Object> replaceConfig = needReplaceKeyWords(nodeInfo, replacePath);
                    if(messsages != null) {
                        Set<String> keys = messsages.keySet();
                        for(String key : keys) {
                            if(!key.equalsIgnoreCase("default")) {
                                Map<String, Object> messagesForLan = (Map<String, Object>) messsages.get(key);
                                if(messagesForLan != null) {
                                    Object docPath = messagesForLan.get("doc");
                                    if(docPath instanceof String) {
                                        String docPathStr = (String) docPath;
                                        if(!inputStreamMap.containsKey(docPathStr)) {
                                            Optional.ofNullable(nodeInfo.readResource(docPathStr)).ifPresent(stream -> {
                                                InputStream inputStream = stream;
                                                if (null != replaceConfig){
                                                    Scanner scanner = null;
                                                    try {
                                                        scanner = new Scanner(stream, "UTF-8");
                                                        StringBuilder docTxt = new StringBuilder();
                                                        while(scanner.hasNextLine()) {
                                                            docTxt.append(scanner.nextLine()).append("\n");
                                                        }
                                                        String finalTxt = docTxt.toString();
                                                        for (Map.Entry<String, Object> entry : replaceConfig.entrySet()) {
                                                            finalTxt = finalTxt.replaceAll(entry.getKey(), String.valueOf(entry.getValue()));
                                                        }
                                                        inputStream = new ByteArrayInputStream(finalTxt.getBytes(StandardCharsets.UTF_8));
                                                    } catch (Exception e) {} finally {
                                                        try {
                                                            if (null != scanner) scanner.close();
                                                        }catch (Exception ignore){}
                                                    }
                                                }
                                                inputStreamMap.put(docPathStr, inputStream);
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                    o.put("expression", JSON.toJSONString(nodeContainer.getDataTypes()));
                    if (nodeContainer.getMessages() != null) {
                        o.put("messages", nodeContainer.getMessages());
                    }

                    io.tapdata.pdk.apis.TapConnector connector1 = (io.tapdata.pdk.apis.TapConnector) nodeInfo.getNodeClass().getConstructor().newInstance();
                    ConnectorFunctions connectorFunctions = new ConnectorFunctions();
                    TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
                    connector1.registerCapabilities(connectorFunctions, codecRegistry);

                    List<Capability> capabilities = connectorFunctions.getCapabilities();
                    Boolean disableDDLSync = null;
                    DataMap dataMap = nodeContainer.getConfigOptions();
                    if(dataMap != null) {
                        List<Map<String, Object>> capabilityList = (List<Map<String, Object>>) dataMap.get("capabilities");

                        if(CollectionUtils.isNotEmpty(capabilityList)) {
                            for(Map<String, Object> capabilityFromSpec : capabilityList) {
                                String capabilityId = (String) capabilityFromSpec.get("id");
                                if(capabilityId != null) {
                                    List<String> alternatives = (List<String>) capabilityFromSpec.get("alternatives");
                                    capabilities.add(Capability.create(capabilityId).alternatives(alternatives).type(Capability.TYPE_OTHER));
                                }
                            }
                        }

                        Map<String, Object> supportDDL = (Map<String, Object>) dataMap.get("supportDDL");
                        if(supportDDL != null) {
//                            disableDDLSync = (Boolean) supportDDL.get("disableDDLSync");
//                            if(disableDDLSync == null) {
//                                disableDDLSync = false;
//                            }
//                            if(!disableDDLSync) {
//                            }
                            List<String> ddlEvents = (List<String>) supportDDL.get("events");
                            if(ddlEvents != null) {
                                for(String ddlEvent : ddlEvents) {
                                    capabilities.add(Capability.create(ddlEvent).type(Capability.TYPE_DDL));
                                }
                            }
                        }

                    }
                    if (CollectionUtils.isNotEmpty(capabilities)) {
                        o.put("capabilities", capabilities);
//                        o.put("disableDDLSync", disableDDLSync);
                        dataMap.remove("capabilities");
                    }

                    Map<Class<?>, String> tapTypeDataTypeMap = codecRegistry.getTapTypeDataTypeMap();
                    o.put("tapTypeDataTypeMap", JSON.toJSONString(tapTypeDataTypeMap));
                    String jsonString = o.toJSONString();
                    jsons.add(jsonString);
                }
                if(file.isFile()) {
                    System.out.println(file.getName() + " uploading... to url " + tmUrl);
                    UploadFileService.upload(inputStreamMap, file, jsons, latest, tmUrl, authToken, ak, sk);
                    System.out.println(file.getName() + " registered successfully");
                }
                else {
                    System.out.println("File " + file + " doesn't exists");
                    System.out.println(file.getName() + " registered failed");
                }
            }
            System.exit(0);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            CommonUtils.logError(TAG, "Start failed", throwable);
            System.exit(-1);
        }
        return 0;
    }

    private static final String path = "tapdata-cli/src/main/resources/replace/";
    private Map<String, Object> needReplaceKeyWords(TapNodeInfo nodeInfo, String replacePath){
        if (null != this.replaceName && !"".equals(replaceName.trim())){
            replacePath = replaceName;
        }
        if (null == replacePath || "".equals(replacePath.trim())) return null;
        try {
            InputStream as = FileUtils.openInputStream(new File(path + replacePath + ".json"));//nodeInfo.readResource((String) replacePath);
            return JSON.parseObject(as, StandardCharsets.UTF_8, LinkedHashMap.class);
        }catch (IOException e){}
        return null;
    }
}
