package io.tapdata.pdk.cli.commands;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVMapFactory;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.tdd.tests.support.CapabilitiesExecutionMsg;
import io.tapdata.pdk.tdd.tests.support.History;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TDDPrintf {
    List<TDDCli.TapSummary> summarys;
    private final String LANG_PATH = "i18n.lang";
    private Locale langType = Locale.SIMPLIFIED_CHINESE;
    private final String AREA_SPLIT = "------------------------------------------------------------------------------------\n";

    public TDDPrintf setLanType(Locale langType){
        this.langType = langType;
        return this;
    }

    public static TDDPrintf create(List<TDDCli.TapSummary> summarys){
        return new TDDPrintf(summarys);
    }

    private TDDPrintf(List<TDDCli.TapSummary> summarys){
        this.summarys = summarys;
    }
    public static TDDPrintf create(){
        return new TDDPrintf();
    }

    private TDDPrintf(){
    }

    public void defaultShow(){
        if (null == summarys) return;
        System.out.println("*****************************************************TDD Results*****************************************************");
        for(TDDCli.TapSummary testSummary : summarys) {
            StringBuilder builder = new StringBuilder();
            builder.append("-------------PDK id '").append(testSummary.tapNodeInfo.getTapNodeSpecification().getId()).append("' class '").append(testSummary.tapNodeInfo.getNodeClass().getName()).append("'-------------").append("\n");
            builder.append("\t\tNode class ").append(testSummary.tapNodeInfo.getNodeClass()).append(" run ");

            builder.append(testSummary.testClasses.size()).append(" test classes").append("\n");
            for(Class<?> testClass : testSummary.testClasses) {
                builder.append("\t\t").append(testClass.getName()).append("\n");
            }
            builder.append("\n");
            builder.append("\t\t" + "Test run finished after ").append(testSummary.summary.getTimeFinished() - testSummary.summary.getTimeStarted()).append("\n");
            builder.append("\t\t").append(testSummary.summary.getTestsFoundCount()).append(" test(s) found").append("\n");
            builder.append("\t\t").append(testSummary.summary.getTestsSkippedCount()).append(" test(s) skipped").append("\n");
            builder.append("\t\t").append(testSummary.summary.getTestsStartedCount()).append(" test(s) started").append("\n");
            builder.append("\t\t").append(testSummary.summary.getTestsSucceededCount()).append(" test(s) successful").append("\n");
            builder.append("\t\t").append(testSummary.summary.getTestsFailedCount()).append(" test(s) failed").append("\n");
            builder.append("-------------PDK id '").append(testSummary.tapNodeInfo.getTapNodeSpecification().getId()).append("' class '").append(testSummary.tapNodeInfo.getNodeClass().getName()).append("'-------------").append("\n");
            System.out.print(builder.toString());
        }
        System.out.println("*****************************************************TDD Results*****************************************************");
    }

    public void showTestResult(TDDCli.TapSummary summary,TapNodeInfo nodeInfo,String fileName){
        showLogo();
        TestExecutionSummary executionSummary = summary.summary;
        Map<Class, CapabilitiesExecutionMsg> result = summary.capabilitiesResult;
        StringBuilder builder = new StringBuilder(AREA_SPLIT);
        builder.append("------------PDK id '")
                .append(summary.tapNodeInfo.getTapNodeSpecification().getId())
                .append("' class '")
                .append(summary.tapNodeInfo.getNodeClass().getName())
                .append("'------------\n");
        showCapabilities(nodeInfo,builder);
        if (null != summary.doNotSupportFunTest && !summary.doNotSupportFunTest.isEmpty()){
            summary.doNotSupportFunTest.forEach(((aClass, s) -> {
                builder.append("☆ ")
                        .append(aClass.getSimpleName()).append("\n[%{TEST_RESULT_ERROR}%]\n")
                        .append("\t\t").append(s).append("\n");
            }));
        }
        int allTestResult = CapabilitiesExecutionMsg.SUCCEED;//default 0 === succeed
        for (Map.Entry<Class,CapabilitiesExecutionMsg> entry : result.entrySet()){
            Class cla = entry.getKey();
            CapabilitiesExecutionMsg res = entry.getValue();

            builder.append("☆ ")
                    .append(cla.getSimpleName()).append("\n[")
                    .append(res.executionResult() == CapabilitiesExecutionMsg.ERROR?"%{TEST_RESULT_ERROR}%":"%{TEST_RESULT_SUCCEED}%")
                    .append("(");
            List<History> history = res.history();
            Map<String, List<History>> collect = history.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(History::tag));
            if (null == collect || collect.isEmpty()) return;
            List<History> warnHistory = collect.get(History.WARN);
            List<History> errorHistory = collect.get(History.ERROR);
            int warnSize = null==warnHistory?0:warnHistory.size();
            int errorSize = null==errorHistory?0:errorHistory.size();
            builder.append("%{SUCCEED_COUNT_LABEL}%").append(res.executionTimes()-warnSize-errorSize)
                    .append(",%{WARN_COUNT_LABEL}%").append(warnSize)
                    .append(",%{ERROR_COUNT_LABEL}%").append(errorSize)
                    .append(")]");

            if (res.executionResult() != CapabilitiesExecutionMsg.ERROR && null != warnHistory && !warnHistory.isEmpty()){
                builder.append("%{HAS_WARN_COUNT}%")
                        .append(warnHistory.size())
                        .append(".");
            }
            builder.append(".\n");
            AtomicInteger index = new AtomicInteger(1);
            collect.forEach((tag,his)->{
                builder.append("\t\t")
                        .append(index.getAndIncrement()).append(") ")
                        .append("%{TEST_OF_").append(tag).append("}%")
                        .append("\n");

                String flag = History.SUCCEED.equals(tag)? "✓":
                        (History.ERROR.equals(tag)? "✗":
                                (History.WARN.equals(tag)? "！":"？"));
                for (int i = 0; i < his.size(); i++) {
                    builder.append("\t\t\t[")
                            .append(flag)
                            .append("].").append(his.get(i).message()).append("\n");
                }
            });

            //获取最终结果，如果出现Error，最终结果为Error.仅出现Error，最终结果为Error.
            int resResult = res.executionResult();
            if (allTestResult == CapabilitiesExecutionMsg.SUCCEED
                    && (resResult == CapabilitiesExecutionMsg.WARN || resResult == CapabilitiesExecutionMsg.ERROR)){
                allTestResult = resResult;
            }
            if (allTestResult == CapabilitiesExecutionMsg.WARN && resResult == CapabilitiesExecutionMsg.ERROR){
                allTestResult = resResult;
            }
        }

        if(CapabilitiesExecutionMsg.ERROR == allTestResult) {
            builder.append("\n(╳) Oops, PDK ")
                    .append(fileName)
                    .append(" %{TEST_ERROR_SUF}%\n\n");
        }else {
            builder.append("\n(✔)%{TEST_SUCCEED_PRE}%, PDK ")
                    .append(fileName)
                    .append(" %{TEST_SUCCEED_SUF}%");
            if (CapabilitiesExecutionMsg.WARN == allTestResult){
                builder.append("%{SUCCEED_WITH_WARN}%");
            }
        }
        builder.append("\n").append(AREA_SPLIT);

        String finalStr = this.replaceAsLang(builder.toString());

        System.out.print(finalStr);
        //System.out.print(langType.getLanguage().equals("en")?format(finalStr):finalStr);
        System.exit(0);
    }

    public void tddShow(){
        StringBuilder builder = new StringBuilder("TDD Result==========>");
        for(TDDCli.TapSummary testSummary : summarys) {

        }
        System.out.println(builder.toString());
    }

    public StringBuilder showCapabilities(TapNodeInfo nodeInfo,StringBuilder show){
        String dagId = UUID.randomUUID().toString();
        if (null == nodeInfo) return show;
        TapNodeSpecification spec = nodeInfo.getTapNodeSpecification();
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
        };
        KVMap<TapTable> kvMap = InstanceFactory.instance(KVMapFactory.class).getCacheMap(dagId, TapTable.class);
        ConnectorNode connectorNode = PDKIntegration.createConnectorBuilder()
                .withDagId(dagId)
                .withAssociateId(UUID.randomUUID().toString())
                .withGroup(spec.getGroup())
                .withVersion(spec.getVersion())
                .withTableMap(kvMap)
                .withPdkId(spec.getId())
                .withGlobalStateMap(stateMap)
                .withStateMap(stateMap)
                .withTable(UUID.randomUUID().toString())
                .build();

        TapNodeInfo tapNodeInfo = connectorNode.getTapNodeInfo();
        TapNodeSpecification tapNodeSpecification = tapNodeInfo.getTapNodeSpecification();
        show.append(AREA_SPLIT)
                .append(tapNodeSpecification.getName())
                .append("%{CONNECTOR_CAPABILITIES_START}%\n");
        ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
        Field[] declaredFields = connectorFunctions.getClass().getDeclaredFields();

        int index = 0;
        for (Field declaredField : declaredFields) {
            try {
                declaredField.setAccessible(Boolean.TRUE);
                Object o = declaredField.get(connectorFunctions);
                if (null != o){
                    String name = declaredField.getName();
                    show.append("\t\t").append(++index).append(". ").append(name).append(";\n");
                }
            } catch (IllegalAccessException e) {

            }
        }
        show.append("%{TOTAL_CAPABILITIES_OF}%")
                .append(tapNodeSpecification.getName())
                .append(" : ").append(index)
                .append(".\n")
                .append(AREA_SPLIT);
        return show;
//        System.out.print(show.toString());
    }

    private String replaceAsLang(String txt){
        ResourceBundle lang = ResourceBundle.getBundle(LANG_PATH, langType);
        int startAt = 0;
        int length = txt.length();
        while (startAt <= length) {
            int splitAt = txt.indexOf("%{",startAt);
            int splitEnd = txt.indexOf("}%",splitAt);
            if (splitAt < 0 || splitEnd < 0) break;
            String key = txt.substring(splitAt+2,splitEnd);
            try {
                String langValue = lang.getString(key);
                txt = txt.replaceAll("%\\{" + key + "}%", !"".equals(langValue)? langValue:"?");
                startAt = splitAt+langValue.length()-1;
            }catch (MissingResourceException e){
                txt = txt.replaceAll("%\\{" + key + "}%", "?");
                startAt = splitAt+1;
            }catch (Exception e){
                startAt = splitEnd+1;
            }

        }
        return txt;
//        return txt.replaceAll("\\n","\n|");
    }

    public void showLogo(){
        System.out.println("\n"+AREA_SPLIT+ "[.___________.    ___      .______    _______       ___   .___________.    ___     ]\n" +
                            "[|           |   /   \\     |   _  \\  |       \\     /   \\  |           |   /   \\    ]\n" +
                            "[`---|  |----`  /  ^  \\    |  |_)  | |  .--.  |   /  ^  \\ `---|  |----`  /  ^  \\   ]\n" +
                            "[    |  |      /  /_\\  \\   |   ___/  |  |  |  |  /  /_\\  \\    |  |      /  /_\\  \\  ]\n" +
                            "[    |  |     /  _____  \\  |  |      |  '--'  | /  _____  \\   |  |     /  _____  \\ ]\n" +
                            "[    |__|    /__/     \\__\\ | _|      |_______/ /__/     \\__\\  |__|    /__/     \\__\\]");
    }

    private String format(String txt){
        StringBuilder builder = new StringBuilder();
        int len = AREA_SPLIT.length();
        int start = 0;
        //System.out.println(txt);
        int end = txt.indexOf("\n");
        while (start <= txt.length()){
            if (end < 0){
                end = txt.length();
            }
            String sub = txt.substring(start,end);
            if (null == sub || "".equals(sub) ) break;
            if (sub.length()>len){
                builder.append(txt,start,start + len).append("\n");
                start += len;
                continue;
            }
            builder.append(txt, start, end);
            start = end;
            end = txt.indexOf("\n",start+1);
        }

        return builder.toString();


//        int len = AREA_SPLIT.length();
//        int start = 0;
//        int end = len;
//        while (start <= builder.length() && end<=builder.length()){
//            String sub = builder.substring(start,end);
//            if (null == sub) break;
//            if (!sub.contains("\\n") && sub.length()>len){
//                builder.insert(start+len,"\\n");
//                start = end;
//            }else {
//                int index = sub.indexOf("\\n");
//                if (index<0) break;
//                start += (index+1);
//            }
//            end = start + len;
//        }
//        return txt;
    }

    public static void main(String[] args) {
        String s  ="[.___________.    ___      .______    _______       ___   .___________.    ___     ]\n" +
                "[|           |   /   \\     |   _  \\  |       \\     /   \\  |           |   /   \\    ]\n" +
                "[`---|  |----`  /  ^  \\    |  |_)  | |  .--.  |   /  ^  \\ `---|  |----`  /  ^  \\   ]\n" +
                "[    |  |      /  /_\\  \\   |   ___/  |  |  |  |  /  /_\\  \\    |  |      /  /_\\  \\  ]\n" +
                "[    |  |     /  _____  \\  |  |      |  '--'  | /  _____  \\   |  |     /  _____  \\ ]\n" +
                "[    |__|    /__/     \\__\\ | _|      |_______/ /__/     \\__\\  |__|    /__/     \\__\\]\n" +
                "[INFO]  $$tag:: PDKTestBase [Found connector name MongoDB id mongodb group io.tapdata version 1.0-SNAPSHOT icon icons/mongodb.png]\n" +
                "------------------------------------------------------------------------------------\n" +
                "MongoDB%{CONNECTOR_CAPABILITIES_START}%\n" +
                "\t\t1. batchReadFunction;\n" +
                "\t\t2. streamReadFunction;\n" +
                "\t\t3. batchCountFunction;\n" +
                "\t\t4. timestampToStreamOffsetFunction;\n" +
                "\t\t5. writeRecordFunction;\n" +
                "\t\t6. queryByAdvanceFilterFunction;\n" +
                "\t\t7. dropTableFunction;\n" +
                "\t\t8. createIndexFunction;\n" +
                "%{TOTAL_CAPABILITIES_OF}%MongoDB : 8.\n" +
                "------------------------------------------------------------------------------------\n" +
                "-------------PDK id 'mongodb' class 'io.tapdata.mongodb.MongodbConnector'-------------\n" +
                "WriteRecordTest=======>测试失败\n" +
                "\t\t检测到数据源存在未实现的方法，终止了当前用例的测试流程，未实现的方法为：io.tapdata.pdk.apis.functions.connector.target.CreateTableFunction\n" +
                "\n" +
                "☆WriteRecordWithQueryTest=======>测试失败(通过用例：2,告警用例：1,错误用例：1).\n" +
                "\t\t1) 成功的记录\n" +
                "\t\t\t[✓]. 插入操作成功，1 个记录在MongoDB执行时发生。\n" +
                "\t\t\t[✓].Succeed query by advance when insert record,the filter Results not null.\n" +
                "\t\t\t[✓].Succeed query by advance when insert record,the filter Results not empty results.\n" +
                "\t\t2) 失败的记录\n" +
                "\t\t\t[✗].insert record not succeed. ==> expected: <true> but was: <false>\n" +
                "\t\t3) 告警的记录\n" +
                "\t\t\t[！].insert record not succeed. ==> expected: <true> but was: <false>\n" +
                "\n" +
                "\n" +
                "(╳) Oops, PDK mongodb-connector-v1.0-SNAPSHOT.jar 未通过所有测试，请解决以上问题，然后重试。\n" +
                "\n" +
                "Disconnected from the target VM, address: '127.0.0.1:55601', transport: 'socket'\n" +
                "\n" +
                "Process finished with exit code 0\n";

        System.out.println(create().format(s));

        System.out.println(Locale.ENGLISH.getLanguage());
    }
}
