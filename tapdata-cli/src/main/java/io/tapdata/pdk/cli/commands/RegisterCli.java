package io.tapdata.pdk.cli.commands;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
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
import io.tapdata.pdk.cli.utils.MultiThreadFactory;
import io.tapdata.pdk.cli.utils.PrintUtil;
import io.tapdata.pdk.cli.utils.split.SplitByFileSizeImpl;
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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;

@CommandLine.Command(
        description = "Push PDK jar file into TM",
        subcommands = MainCli.class
)
public class RegisterCli extends CommonCli {
    private static final String TAG = RegisterCli.class.getSimpleName();
    private PrintUtil printUtil;
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

    @CommandLine.Option(names = {"-t", "--tm"}, required = true, description = "TM server url")
    private String tmUrl;

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "Register cli help")
    private boolean helpRequested = false;

    @CommandLine.Option(names = {"-r", "--replace"}, required = false, description = "Replace Config file name")
    private String replaceName;

    @CommandLine.Option(names = {"-f", "--filter"}, required = false, description = "The list which are the Authentication types should not be skipped, if value is empty will register all connector. if it contains multiple, please separate them with commas")
    private String needRegisterConnectionTypes;

    @CommandLine.Option(names = {"-X", "--X"}, required = false, description = "Output detailed logs, true or false")
    private boolean showAllMessage = false;

    @CommandLine.Option(names = {"-T", "--threadCount"}, required = false, description = "Multi thread registration parameter, enable thread count, maximum value of 20, default value of 1")
    private int maxThreadCount = 1;

    protected void fixThreadCount() {
        if (maxThreadCount < 1) {
            maxThreadCount = 1;
        }
        if (maxThreadCount > 20) {
            maxThreadCount = 20;
        }
    }

    public Integer execute() throws Exception {
        fixThreadCount();
        long start = System.currentTimeMillis();
        printUtil = new PrintUtil(showAllMessage);
        final List<String> filterTypes = generateSkipTypes();
        if (!filterTypes.isEmpty()) {
            boolean tooMany = filterTypes.size() > 1;
            printUtil.print(PrintUtil.TYPE.TIP, String.format("* Starting to register data sources, plan to skip data sources that are not within the registration scope\n" +
                            "* The types of data sources that need to be registered %s: %s",
                    tooMany ? "are" : "is", filterTypes));
        } else {
            printUtil.print(PrintUtil.TYPE.TIP, "* Start registering data sources and plan to register all submitted data sources connectors");
        }

        files = getAllJarFile(files);

        CommonUtils.setProperty("refresh_local_jars", "true");
        loadAllPDKJar(Arrays.asList(files));

        printUtil.print(PrintUtil.TYPE.INFO, "- Register connector to: " + tmUrl);
        final String tmToken = findTMToken();

        try {
            printUtil.print(PrintUtil.TYPE.DEBUG, "* Start registering all connectors");
            long startRegister = System.currentTimeMillis();
            if (maxThreadCount == 1) {
                printUtil.print(PrintUtil.TYPE.TIP, "* Start registering with single thread");
                registerOneBatch(files, filterTypes, tmToken);
            } else {
                printUtil.print(PrintUtil.TYPE.TIP, String.format("* Start registering with multi thread of %s workers", maxThreadCount));
                final int eachSize = (files.length / maxThreadCount) + (files.length % maxThreadCount > 0 ? 1 : 0);
                MultiThreadFactory<File> multiThreadFactory = new MultiThreadFactory<>(maxThreadCount, eachSize);
                multiThreadFactory.setSplitStage(new SplitByFileSizeImpl(maxThreadCount, printUtil));
                multiThreadFactory.handel(Lists.newArrayList(files), fileListAnBatch -> {
                    File[] fs = new File[fileListAnBatch.size()];
                    registerOneBatch(fileListAnBatch.toArray(fs), filterTypes, tmToken);
                });
            }
            printUtil.print(PrintUtil.TYPE.DEBUG, String.format("* Register all connectors completed cost time: %s", PrintUtil.formatDate(startRegister)));
        } catch (Exception e) {
            printUtil.print(PrintUtil.TYPE.WARN, String.format("* Register all connectors failed cost time: %s", PrintUtil.formatDate(start)));
            System.exit(-1);
        }
        printUtil.print(PrintUtil.TYPE.UN_OUTSHOOT, String.format("* Register command execute completed, cost time: %s", PrintUtil.formatDate(start)));
        System.exit(0);
        return 0;
    }

    protected String findTMToken() {
        printUtil.print(PrintUtil.TYPE.UN_OUTSHOOT, "* Start to get permission registration authentication");
        long startFindToken = System.currentTimeMillis();
        String token = null;
        if (!UploadFileService.isCloud(ak)) {
            printUtil.print(PrintUtil.TYPE.INFO, "* Will register to OP environment");
            token = UploadFileService.findOpToken(tmUrl, authToken, printUtil);
            if (null == token || token.isEmpty()) {
                printUtil.print(PrintUtil.TYPE.WARN, "* Register be canceled, failed to get permission registration authentication from " + tmUrl);
                System.exit(-2);
            }
        } else {
            printUtil.print(PrintUtil.TYPE.INFO, "* Will register to Cloud environment");
        }
        printUtil.print(PrintUtil.TYPE.UN_OUTSHOOT, "* Get permission registration authentication completed, cost time: " + PrintUtil.formatDate(startFindToken));
        return token;
    }

    protected void loadAllPDKJar(List<File> files) {
        printUtil.print(PrintUtil.TYPE.DEBUG, "* Start load all connector to connector manager");
        long startLoadAllPDKJar = System.currentTimeMillis();
        PrintStream out = System.out;
        try(PrintStream p = new PrintStream(new ByteArrayOutputStream() {
            @Override
            public synchronized void write(int b) {
                if (showAllMessage) {
                    super.write(b);
                }
            }
        })) {
            System.setOut(p);
            TapConnectorManager.getInstance().start(files);
            System.setOut(out);
            printUtil.print(PrintUtil.TYPE.DEBUG, String.format("* Load all connector to connector manager completed cost time: %s",
                    PrintUtil.formatDate(startLoadAllPDKJar))
            );
        } catch (Exception e) {
            System.setOut(out);
            printUtil.print(PrintUtil.TYPE.WARN, String.format("* Can not load connector jar, register failed, message: %s", e.getMessage()));
            System.exit(-1);
        }
    }

    protected void registerOneBatch(File[] files, List<String> filterTypes, String tmToken) {
        try {
            for (File file : files) {
                printUtil.print(PrintUtil.TYPE.NORMAL, String.format("* Register Connector: %s  Starting", file.getName()));
                List<String> jsons = new ArrayList<>();
                TapConnector connector = TapConnectorManager.getInstance().getTapConnectorByJarName(file.getName());
                Collection<TapNodeInfo> tapNodeInfoCollection = connector.getTapNodeClassFactory().getConnectorTapNodeInfos();
                Map<String, InputStream> inputStreamMap = new HashMap<>();
                boolean needUpload = true;
                String connectionType = "";
                try {
                    for (TapNodeInfo nodeInfo : tapNodeInfoCollection) {
                        TapNodeSpecification specification = nodeInfo.getTapNodeSpecification();
                        String authentication = specification.getManifest().get("Authentication");
                        connectionType = authentication;
                        if (needSkip(authentication, filterTypes)) {
                            needUpload = false;
                            printUtil.print(PrintUtil.TYPE.IGNORE, String.format(" Connector: %s, Skipped with (%s)", file.getName(), connectionType));
                            break;
                        }
                        needUpload = true;
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
                        o.put("beta", "beta".equals(authentication));
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
                        if (nodeContainer.getDataTypes() == null) {
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
                        Map<String, Object> message = nodeContainer.getMessages();
                        String replacePath = null;
                        Map<String, Object> replaceConfig = needReplaceKeyWords(nodeInfo, replacePath);
                        if (message != null) {
                            Set<String> keys = message.keySet();
                            for (String key : keys) {
                                if (!key.equalsIgnoreCase("default")) {
                                    Map<String, Object> messagesForLan = (Map<String, Object>) message.get(key);
                                    if (messagesForLan != null) {
                                        Object docPath = messagesForLan.get("doc");
                                        if (docPath instanceof String) {
                                            String docPathStr = (String) docPath;
                                            if (!inputStreamMap.containsKey(docPathStr)) {
                                                Optional.ofNullable(nodeInfo.readResource(docPathStr)).ifPresent(stream -> {
                                                    InputStream inputStream = stream;
                                                    if (null != replaceConfig) {
                                                        Scanner scanner = null;
                                                        try {
                                                            scanner = new Scanner(stream, "UTF-8");
                                                            StringBuilder docTxt = new StringBuilder();
                                                            while (scanner.hasNextLine()) {
                                                                docTxt.append(scanner.nextLine()).append("\n");
                                                            }
                                                            String finalTxt = docTxt.toString();
                                                            for (Map.Entry<String, Object> entry : replaceConfig.entrySet()) {
                                                                finalTxt = finalTxt.replaceAll(entry.getKey(), String.valueOf(entry.getValue()));
                                                            }
                                                            inputStream = new ByteArrayInputStream(finalTxt.getBytes(StandardCharsets.UTF_8));
                                                        } catch (Exception e) {
                                                            printUtil.print(PrintUtil.TYPE.DEBUG, e.getMessage());
                                                        } finally {
                                                            try {
                                                                if (null != scanner) scanner.close();
                                                            } catch (Exception e) {
                                                                printUtil.print(PrintUtil.TYPE.DEBUG, e.getMessage());
                                                            }
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
                        DataMap dataMap = nodeContainer.getConfigOptions();
                        if (dataMap != null) {
                            List<Map<String, Object>> capabilityList = (List<Map<String, Object>>) dataMap.get("capabilities");

                            if (CollectionUtils.isNotEmpty(capabilityList)) {
                                for (Map<String, Object> capabilityFromSpec : capabilityList) {
                                    String capabilityId = (String) capabilityFromSpec.get("id");
                                    if (capabilityId != null) {
                                        List<String> alternatives = (List<String>) capabilityFromSpec.get("alternatives");
                                        capabilities.add(Capability.create(capabilityId).alternatives(alternatives).type(Capability.TYPE_OTHER));
                                    }
                                }
                            }

                            Map<String, Object> supportDDL = (Map<String, Object>) dataMap.get("supportDDL");
                            if (supportDDL != null) {
                                List<String> ddlEvents = (List<String>) supportDDL.get("events");
                                if (ddlEvents != null) {
                                    for (String ddlEvent : ddlEvents) {
                                        capabilities.add(Capability.create(ddlEvent).type(Capability.TYPE_DDL));
                                    }
                                }
                            }

                        }
                        if (CollectionUtils.isNotEmpty(capabilities)) {
                            o.put("capabilities", capabilities);
                            if (null != dataMap) {
                                dataMap.remove("capabilities");
                            }
                        }

                        Map<Class<?>, String> tapTypeDataTypeMap = codecRegistry.getTapTypeDataTypeMap();
                        o.put("tapTypeDataTypeMap", JSON.toJSONString(tapTypeDataTypeMap));
                        String jsonString = o.toJSONString();
                        jsons.add(jsonString);
                    }
                    if (!needUpload) {
                        printUtil.print(PrintUtil.TYPE.DEBUG, String.format("\t- skipped %s's source types is [%s], need register type list: %s", file.getName(), connectionType, filterTypes));
                        continue;
                    }
                    if (file.isFile()) {
                        UploadFileService.Param param = new UploadFileService.Param();
                        param.setToken(tmToken);
                        param.setInputStreamMap(inputStreamMap);
                        param.setJsons(jsons);
                        param.setLatest(latest);
                        param.setAk(ak);
                        param.setFile(file);
                        param.setPrintUtil(printUtil);
                        param.setSk(sk);
                        param.setHostAndPort(tmUrl);
                        printUtil.print(PrintUtil.TYPE.TIP, String.format("=> Uploading connector %s", file.getName()));
                        UploadFileService.uploadConnector(param);
                        printUtil.print(PrintUtil.TYPE.INFO, String.format("* Register Connector: %s | (%s) Completed", file.getName(), connectionType));
                    } else {
                        printUtil.print(PrintUtil.TYPE.DEBUG, "* File " + file + " doesn't exists");
                        printUtil.print(PrintUtil.TYPE.DEBUG, "* " + file.getName() + " registered failed");
                    }
                } finally {
                    if (!inputStreamMap.isEmpty()) {
                        inputStreamMap.entrySet().forEach(ent -> {
                            String name = ent.getKey();
                            Optional.ofNullable(ent.getValue()).ifPresent(stream -> {
                                try {
                                    stream.close();
                                } catch (Exception e) {
                                    printUtil.print(PrintUtil.TYPE.DEBUG, String.format("Failed to close stream of %s, message: %s", name, e.getMessage()));
                                }
                            });
                        });
                    }
                }
            }
        } catch (Throwable throwable) {
            printUtil.print(PrintUtil.TYPE.ERROR, throwable.getMessage());
            throwable.printStackTrace(System.out);
            if (showAllMessage) {
                CommonUtils.logError(TAG, "Start failed", throwable);
            }
            throw new RuntimeException(throwable);
        }
    }

    public File[] getAllJarFile(File[] paths) {
        printUtil.print(PrintUtil.TYPE.DEBUG, "* Start scan connector Jar registration package");
        long startScan = System.currentTimeMillis();
        try {
            Collection<File> allJarFiles = getAllJarFiles(paths);
            File[] strings = new File[allJarFiles.size()];
            return allJarFiles.toArray(strings);
        } finally {
            printUtil.print(PrintUtil.TYPE.DEBUG, String.format("* Scan connector Jar registration package completed cost time: %s, %s connectors be scanned",
                    PrintUtil.formatDate(startScan), files.length)
            );
        }
    }

    protected Collection<File> getAllJarFiles(File[] paths) {
        Set<File> pathSet = new HashSet<>();
        for (File s : paths) {
            fileTypeDirector(s, pathSet);
        }
        return pathSet;
    }

    protected void fileTypeDirector(File f, Set<File> pathSet) {
        int i = fileType(f);
        if (i == 1) {
            File[] listFiles = f.listFiles();
            if (null != listFiles && listFiles.length > 0) {
                pathSet.addAll(getAllJarFiles(listFiles));
            }
            return;
        }
        if (i == 2) {
            pathSet.add(f);
        }
    }


    protected int fileType(File file) {
        if (null == file || !file.exists()) {
            return -1;
        }
        if (file.isDirectory()) {
            return 1;
        }
        if (file.isFile() && file.getAbsolutePath().endsWith(".jar")) {
            return 2;
        }
        return -1;
    }

    protected static final String PATH = "tapdata-cli/src/main/resources/replace/";
    private Map<String, Object> needReplaceKeyWords(TapNodeInfo nodeInfo, String replacePath){
        if (null != this.replaceName && !"".equals(replaceName.trim())){
            replacePath = replaceName;
        }
        if (null == replacePath || "".equals(replacePath.trim())) return null;
        try {
            InputStream as = FileUtils.openInputStream(new File(PATH + replacePath + ".json"));
            return JSON.parseObject(as, StandardCharsets.UTF_8, LinkedHashMap.class);
        }catch (IOException e){
            printUtil.print(PrintUtil.TYPE.WARN, e.getMessage());
        }
        return null;
    }

    protected List<String> generateSkipTypes() {
        List<String> needRegisterConnectionTypesArray = new ArrayList<>();
        if (!StringUtils.isBlank(needRegisterConnectionTypes)) {
            DataSourceQCType[] values = DataSourceQCType.values();
            String[] split = needRegisterConnectionTypes.toUpperCase().split(",");
            for (String tag : split) {
                for (DataSourceQCType value : values) {
                    String name = value.name();
                    if (name.equalsIgnoreCase(tag)) {
                        needRegisterConnectionTypesArray.add(name);
                        break;
                    }
                }
            }
        }
        return needRegisterConnectionTypesArray;
    }

    protected boolean needSkip(String authentication, List<String> skipList) {
        return StringUtils.isNotBlank(authentication) && !skipList.isEmpty() && !skipList.contains(String.valueOf(authentication).toUpperCase());
    }
}
