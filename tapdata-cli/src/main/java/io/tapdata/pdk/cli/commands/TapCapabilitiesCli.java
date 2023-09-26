package io.tapdata.pdk.cli.commands;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVMapFactory;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.functions.*;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.cli.CommonCli;
import io.tapdata.pdk.cli.utils.TapExcel;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.connector.TapConnector;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.tapnode.TapNodeContainer;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.IOUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.*;

@CommandLine.Command(
        description = "Export TapConnectors Capabilities as .xlsx",
        subcommands = MainCli.class
)
public class TapCapabilitiesCli extends CommonCli {
    private static final String TAG = TapCapabilitiesCli.class.getSimpleName();

    @CommandLine.Option(names = { "-o", "--output" }, required = true, description = "Specify the folder where model deduction report files will be generated")
    private String output;

    @CommandLine.Parameters(paramLabel = "FILE", description = "One ore more pdk jar files")
    File[] files;

    @CommandLine.Option(names = { "-m", "--mavenHome" }, required = false, description = "Specify the maven home")
    private String mavenHome;

    Set<String> fun = new TreeSet<>();
    Set<String> cab = new TreeSet<>();
    Set<String> baseFun = new TreeSet<>();
    Map<String,Map<String,String>> result = new HashMap<>();

    public static final String SUPPORT = "✔";
    public static final String NOT_SUPPORT = "✘";
    public static final String UNDEFINE = "-";

    @Override
    public Integer execute() throws Exception {
        CommonUtils.setProperty("refresh_local_jars", "true");
        File outputFile = new File(output);
        if(outputFile.isFile())
            throw new IllegalArgumentException("");
        if(!outputFile.exists())
            FileUtils.forceMkdir(outputFile);
        if(!output.endsWith(File.separator)) {
            output = output + File.separator;
        }
        initFun();
        initBaseFun();

        List<File> jarFiles = new ArrayList<>();
        for(File file : files) {
            String jarFile = null;
            if(file.isFile()) {
                jarFile = file.getAbsolutePath();
            } else if(file.isDirectory()) {
                jarFiles.addAll(Arrays.asList(file.listFiles()));
            } else {
                throw new IllegalArgumentException("File " + file.getAbsolutePath() + " is not exist");
            }
            if (null != jarFile){
                File theJarFile = new File(jarFile);
                if(!theJarFile.exists()) {
                    throw new IllegalArgumentException("Packaged jar file " + jarFile + " not exists");
                }
                jarFiles.add(theJarFile);
            }
        }

        TapConnectorManager.getInstance().start(jarFiles);
        List<ConnectorNode> connectorNodes = new ArrayList<>();
        for(File file : jarFiles) {
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
                        .withAssociateId(UUID.randomUUID().toString())
                        .build();
                PDKInvocationMonitor.getInstance().invokePDKMethod(node, PDKMethod.REGISTER_CAPABILITIES,
                        node::registerCapabilities,
                        MessageFormat.format("call registerCapabilities functions {0} associateId {1}", specification.idAndGroup(), "test"), TAG);
                connectorNodes.add(node);
                Set<String> capabilityIds = new HashSet<>();
                for (Capability capability : this.initCab(node.getTapNodeInfo())) {
                    capabilityIds.add(capability.getId());
                }
                this.cab.addAll(capabilityIds);
            }
        }
        connectorNodes.stream().filter(Objects::nonNull).forEach(node->{
            TapNodeInfo nodeInfo = node.getTapNodeInfo();
            Map<String,String> connectorResult = new HashMap<>();
            connectorResult.put("connector",nodeInfo.getTapNodeSpecification().getId());
            for (String base : this.baseFun) {
                connectorResult.put(base,SUPPORT);
            }

            Map<String,Capability> allCab = this.allCapability(node);
//            if (null != allCab && ! allCab.isEmpty()){
//                allCab.forEach((name,capability)->{
//                    try {
//                        Class<String> stringClass = String.class;
//                        Field value = stringClass.getDeclaredField("value");
//                        value.setAccessible(true);
//                        value.set(name,this.getCapability(name));
//                    }catch (Exception e){
//                    }
//                });
//            }
            this.fun.forEach(name->{
                connectorResult.put(name,null == allCab || null == allCab.get(name) ? NOT_SUPPORT : SUPPORT);
            });
            try {
                Map<String,Capability> initCab = this.initCab(node.getTapNodeInfo()).stream().filter(Objects::nonNull).collect(Collectors.toMap(c->c.getId(),c->c,(c1,c2)->c2));
                this.cab.forEach(name->{
                    Capability contains = initCab.get(name);
                    if (null != contains){
                        String result;
                        switch (name){
                            case "api_server_supported":
                            case "master_slave_merge":result = SUPPORT;break;
                            default: {
                                List<String> alternatives = contains.getAlternatives();
                                if (null != alternatives){
                                    StringJoiner joiner = new StringJoiner(";\n");
                                    for (String alternative : alternatives) {
                                        joiner.add(FunctionDesc.desc(alternative));
                                    }
                                    result = "支持\n"+joiner.toString();
                                }else {
                                    result = NOT_SUPPORT;
                                }
                            }
                        }
                        connectorResult.put(name,result );
                    }else {
                        connectorResult.put(name,NOT_SUPPORT );
                    }
                });
            } catch (IOException e) {

            }
            result.put(nodeInfo.getTapNodeSpecification().getId(),connectorResult);
        });


        TapExcel.create(this.basePath(output))
                .cells(this.buildColumn(),this.result)
                .export("PDK-capability-description-"+(new SimpleDateFormat("yyyyMMdd_HHmmss")).format(new Date())+".xlsx");

        System.exit(1);
        return 0;
    }

    public String basePath(String logPath) {
        try {
            Path path = Paths.get(logPath );
            String pathFinal = path.toFile().getCanonicalPath() + "/";
            File file = new File(pathFinal);
            if (!file.exists()){
                file.mkdir();
            }
            return pathFinal;
        } catch (Throwable throwable) {
            String pathFinal = FilenameUtils.concat(logPath, "../");
            File file = new File(pathFinal);
            if (!file.exists()){
                file.mkdir();
            }
            return pathFinal;
        }
    }

    List<Map<String,Object>> buildColumn(){
        List<Map<String,Object>> column = new ArrayList<>();
        column.add(map(entry("id","connector"),entry("label","数据源/能力")));
        if (null != this.baseFun && !this.baseFun.isEmpty()) {
            this.baseFun.stream().filter(Objects::nonNull).forEach(f->{
                column.add(map(entry("id",f),entry("label",FunctionDesc.desc(f))));
            });
        }
        if (null != this.fun && !this.fun.isEmpty()){
            this.fun.stream().filter(Objects::nonNull).forEach(f->{
                column.add(map(entry("id",f),entry("label",FunctionDesc.desc(f))));
            });
        }
        if (null != this.fun && !this.cab.isEmpty()) {
            this.cab.stream().filter(Objects::nonNull).forEach(c->{
                column.add(map(entry("id",c),entry("label",FunctionDesc.desc(c))));
            });
        }
        return column;
    }

    public String getCapability(String capabilityId){
        final String replaceChar = "_";
        char[] charArray = capabilityId.toCharArray();
        StringBuilder finalCapabilityId = new StringBuilder();
        for (int i = 0; i < charArray.length; i++) {
            char ch = charArray[i];
            if ( ch == '_' && charArray.length > i+1){
                char charAt = String.valueOf(charArray[++i]).toUpperCase().charAt(0);
                finalCapabilityId.append(charAt);
                continue;
            }
            if (ch != '_'){
                finalCapabilityId.append(ch);
            }
        }
        return finalCapabilityId.toString();
    }

    private Map<String,Capability> allCapability(ConnectorNode node){
        if (null == node) return null;
        List<Capability> connectorCapabilities = node.getConnectorFunctions().getCapabilities();
        return connectorCapabilities.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(
                        c->this.getCapability(c.getId()),
                        c->c,
                        (c1,c2)->c2)
                );
    }

    private Set<Capability> initCab(TapNodeInfo nodeInfo) throws IOException {
        Set<Capability> itemClb = new TreeSet<Capability>((o1,o2)->{
            String id1 = o1.getId();
            String id2 = o2.getId();
            return id1.equals(id2)?0:1;
        });
        TapNodeContainer nodeContainer = JSON.parseObject(IOUtils.toString(nodeInfo.readResource(nodeInfo.getNodeClass().getAnnotation(TapConnectorClass.class).value())), TapNodeContainer.class);
        DataMap dataMap = nodeContainer.getConfigOptions();
        if(dataMap != null) {
            List<Map<String, Object>> capabilityList = (List<Map<String, Object>>) dataMap.get("capabilities");
            if(CollectionUtils.isNotEmpty(capabilityList)) {
                for(Map<String, Object> capabilityFromSpec : capabilityList) {
                    String capabilityId = (String) capabilityFromSpec.get("id");
                    if(capabilityId != null) {
                        List<String> alternatives = (List<String>) capabilityFromSpec.get("alternatives");
                        itemClb.add(Capability.create(capabilityId).alternatives(alternatives).type(Capability.TYPE_OTHER));
                    }
                }
            }
            Map<String, Object> supportDDL = (Map<String, Object>) dataMap.get("supportDDL");
            if(supportDDL != null) {
                List<String> ddlEvents = (List<String>) supportDDL.get("events");
                if(ddlEvents != null) {
                    for (String ddlEvent : ddlEvents) {
                        itemClb.add(Capability.create(ddlEvent).type(Capability.TYPE_DDL));
                    }
                }
            }
        }
        return itemClb;
    }

    private void initBaseFun(){
        this.baseFun = new TreeSet<>();
        this.baseFun.add("discoverSchema");
        this.baseFun.add("connectionTest");
        this.baseFun.add("tableCount");
    }

    private void initFun(){
        this.fun = new TreeSet<>();
        this.set(ConnectorFunctions.class,ConnectionFunctions.class,CommonFunctions.class,ProcessorFunctions.class);
    }
    private void set(Class<? extends Functions> ... clas){
        if (null == clas || clas.length < 1 ) return;
        for (Class<? extends Functions> cla : clas) {
            if (null == cla) return;
            Field[] fields = cla.getDeclaredFields();
            if (null != fields) {
                for (Field field : fields) {
                    fun.add(field.getName());
                }
            }
        }
    }
    enum FunctionDesc{
        UNDEFINE("","*"),

        discoverSchema("discoverSchema","加载模型"),
        connectionTest("connectionTest","连接测试"),
        tableCount("tableCount","获取表数量"),

        releaseExternalFunction("releaseExternalFunction","任务重置释放外部资源"),
        batchReadFunction("batchReadFunction","全量读取数据"),
        streamReadFunction("streamReadFunction","增量读取数据"),
        batchCountFunction("batchCountFunction","获取数据记录数"),
        timestampToStreamOffsetFunction("timestampToStreamOffsetFunction","通过时间戳获得增量断点"),
        writeRecordFunction("writeRecordFunction","批量写入数据"),
        queryByFilterFunction("queryByFilterFunction","通过构建Filter查询记录"),
        queryByAdvanceFilterFunction("queryByAdvanceFilterFunction","根据预先条件筛选查询结果"),
        createTableFunction("createTableFunction","创建表"),
        createTableV2Function("createTableV2Function","创建表后返回创建结果"),
        clearTableFunction("clearTableFunction","清空表数据但不删除表"),
        dropTableFunction("dropTableFunction","删除表"),
        controlFunction("controlFunction","控制事件的接收方法"),
        createIndexFunction("createIndexFunction","数据源可以对字段创建索引"),
        deleteIndexFunction("deleteIndexFunction","删除索引"),
        queryIndexesFunction("queryIndexesFunction","查询索引列表"),
        alterDatabaseTimeZoneFunction("alterDatabaseTimeZoneFunction","基于DDL修改数据库时区方法"),
        alterFieldAttributesFunction("alterFieldAttributesFunction","修改表字段属性"),
        alterFieldNameFunction("alterFieldNameFunction","修改表字段名称"),
        alterTableCharsetFunction("alterTableCharsetFunction","修改表的字符集"),
        dropFieldFunction("dropFieldFunction","删除字段属性"),
        newFieldFunction("newFieldFunction","新增字段属性"),
        rawDataCallbackFilterFunction("rawDataCallbackFilterFunction","aas平台WebHook回调的方法，单个接收"),
        rawDataCallbackFilterFunctionV2("rawDataCallbackFilterFunctionV2","Saas平台WebHook回调的方法，批量接收"),

        getTableNamesFunction("getTableNamesFunction","获取表"),
        connectionCheckFunction("connectionCheckFunction","连接检测"),
        getCharsetsFunction("getCharsetsFunction","获取字符集"),
        commandCallbackFunction("commandCallbackFunction","在连接页面和节点页面由用户点击发送Command获取数据源相关数据"),
        errorHandleFunction("errorHandleFunction","出错重试的处理方法"),

        memoryFetcherFunction("memoryFetcherFunction","逻辑内存导出方法"),
        memoryFetcherFunctionV2("memoryFetcherFunctionV2","逻辑内存导出方法（v2版）"),

        processRecordFunction("processRecordFunction","处理器处理数据方法"),

        api_server_supported("api_server_supported","支持APIServer"),

        dml_insert_policy("dml_insert_policy","插入策略"),
        update_on_exists("update_on_exists","存在就修改"),
        ignore_on_exists("ignore_on_exists","存在就忽略"),

        dml_update_policy("dml_update_policy","更新策略"),
        ignore_on_nonexists("ignore_on_nonexists","不存在就忽略"),
        insert_on_nonexists("insert_on_nonexists","不存在时插入"),


        master_slave_merge("master_slave_merge","支持主从合并功能")
        ;
        String funName;
        String funDesc;
        FunctionDesc(String funName,String funDesc){
            this.funName = funName;
            this.funDesc = funDesc;
        }
        public static String desc(String funName){
            if (null == funName ) return UNDEFINE.funDesc;
            FunctionDesc[] values = values();
            for (FunctionDesc value : values) {
                if (funName.equals(value.getFunName())) return value.getFunDesc()+"("+funName+")";
            }
            return UNDEFINE.getFunDesc()+"("+funName+")";
        }
        public String getFunName() {
            return funName;
        }
        public void setFunName(String funName) {
            this.funName = funName;
        }
        public String getFunDesc() {
            return funDesc;
        }
        public void setFunDesc(String funDesc) {
            this.funDesc = funDesc;
        }
    }
}
