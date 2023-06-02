package io.tapdata.pdk.cli.commands;

import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVMapFactory;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.tests.support.CapabilitiesExecutionMsg;
import io.tapdata.pdk.tdd.tests.support.Case;
import io.tapdata.pdk.tdd.tests.support.History;
import io.tapdata.pdk.tdd.tests.support.TapSummary;
import org.junit.jupiter.api.DisplayName;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @deprecated please use TapSummary
 * */
public class TDDPrintf {
    List<TapSummary> summarys;
    private static final String LANG_PATH = "i18n.lang";
    private Locale langType = Locale.SIMPLIFIED_CHINESE;
    private final String AREA_SPLIT = "------------------------------------------------------------------------------------";

    public TDDPrintf setLanType(Locale langType){
        this.langType = langType;
        return this;
    }

    public static TDDPrintf create(List<TapSummary> summarys){
        return new TDDPrintf(summarys);
    }

    private TDDPrintf(List<TapSummary> summarys){
        this.summarys = summarys;
    }
    public static TDDPrintf create(){
        return new TDDPrintf();
    }

    private TDDPrintf(){
    }

    /**
     * @deprecated
     * */
    public void defaultShow(){
        if (null == summarys) return;
        System.out.println("*****************************************************TDD Results*****************************************************");
        for(TapSummary testSummary : summarys) {
            StringBuilder builder = new StringBuilder();
            builder.append("-------------PDK id '").append(testSummary.tapNodeInfo().getTapNodeSpecification().getId()).append("' class '").append(testSummary.tapNodeInfo().getNodeClass().getName()).append("'-------------").append("\n");
            builder.append("\t\tNode class ").append(testSummary.tapNodeInfo().getNodeClass()).append(" run ");

            builder.append(testSummary.testClasses().size()).append(" test classes").append("\n");
            for(Class<?> testClass : testSummary.testClasses()) {
                builder.append("\t\t").append(testClass.getName()).append("\n");
            }
            builder.append("\n");
            builder.append("\t\t" + "Test run finished after ").append(testSummary.summary().getTimeFinished() - testSummary.summary().getTimeStarted()).append("\n");
            builder.append("\t\t").append(testSummary.summary().getTestsFoundCount()).append(" test(s) found").append("\n");
            builder.append("\t\t").append(testSummary.summary().getTestsSkippedCount()).append(" test(s) skipped").append("\n");
            builder.append("\t\t").append(testSummary.summary().getTestsStartedCount()).append(" test(s) started").append("\n");
            builder.append("\t\t").append(testSummary.summary().getTestsSucceededCount()).append(" test(s) successful").append("\n");
            builder.append("\t\t").append(testSummary.summary().getTestsFailedCount()).append(" test(s) failed").append("\n");
            builder.append("-------------PDK id '").append(testSummary.tapNodeInfo().getTapNodeSpecification().getId()).append("' class '").append(testSummary.tapNodeInfo().getNodeClass().getName()).append("'-------------").append("\n");
            System.out.print(builder.toString());
        }
        System.out.println("*****************************************************TDD Results*****************************************************");
    }

    public void showTestResultAll(TapNodeInfo nodeInfo,String fileName){
        if (null != summarys && !summarys.isEmpty()) {
            summarys.stream().filter(Objects::nonNull).forEach(summary -> showTest(summary));
        }
    }
    public void showTestResult(TapSummary summary){
        showTest(summary);
    }
    public void showCapabilities(TapSummary summary,TapNodeInfo nodeInfo){
        StringBuilder builder = new StringBuilder(AREA_SPLIT);
        builder.append("\n------------PDK id '")
                .append(summary.tapNodeInfo().getTapNodeSpecification().getId())
                .append("' class '")
                .append(summary.tapNodeInfo().getNodeClass().getName())
                .append("'------------\n");
        System.out.println(showCapabilities(nodeInfo,builder));
    }
    private void showTest(TapSummary summary){
        Map<Class<? extends PDKTestBase>,Integer> resultExecution = summary.resultExecution();
        Map<Class, CapabilitiesExecutionMsg> result = summary.capabilitiesResult;
        StringBuilder builder = new StringBuilder(AREA_SPLIT);
        builder.append("\n");
        if (null != summary.doNotSupportFunTest() && !summary.doNotSupportFunTest().isEmpty()){
            summary.doNotSupportFunTest().forEach(((aClass, s) -> {
                builder.append("☆ ")
                        .append(aClass.getSimpleName()).append("\n")
                        .append("\t").append(s).append("\n");
                builder.append("☆ ")
                        .append(TDDPrintf.format("ONCE_HISTORY",
                                aClass.getSimpleName(),
                                TDDPrintf.format("TEST_RESULT_ERROR"),
                                0,
                                0,
                                0,
                                1))
                        .append("\n");
            }));
        }
        int allTestResult = CapabilitiesExecutionMsg.SUCCEED;//default 0 === succeed
        for (Map.Entry<Class,CapabilitiesExecutionMsg> entry : result.entrySet()){
            Class cla = entry.getKey();
            CapabilitiesExecutionMsg res = entry.getValue();

            StringBuilder capabilityBuilder = new StringBuilder();
            Map<Method, Case> caseMap = res.testCases();
//            Map<String, List<Map<Method, Case>>> caseGroupMapByTag = res.testCaseGroupTag();
            int warnSize = 0;//null==warnHistory?0:warnHistory.size();
            int errorSize = 0;//null==errorHistory?0:errorHistory.size();
//            if (null!=caseGroupMapByTag && !caseGroupMapByTag.isEmpty()){
            Map<Method,Case> methodCaseMap = res.testCases();
            if (null != methodCaseMap && !methodCaseMap.isEmpty()) {
                int caseIndex = 0;
                for (Map.Entry<Method, Case> methodCaseEntry : methodCaseMap.entrySet()) {
                    Case testCase = methodCaseEntry.getValue();

                    switch (testCase.tag()) {
                        case Case.ERROR:
                            errorSize++;
                            break;
                        case Case.WARN:
                            warnSize++;
                            break;
                    }
                    capabilityBuilder.append("\t")
                            .append(((char) (65 + caseIndex)))
                            .append(".")
                            .append(TDDPrintf.format(testCase.message()))
                            .append("\n");
                    caseIndex++;

                    List<History> history = testCase.histories();

                    //Map<String, List<History>> collect = history.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(History::tag));
                    TreeMap<String, List<History>> collect = history.stream()
                            .filter(Objects::nonNull)
                            .collect(Collectors.groupingBy(History::tag, TreeMap::new, Collectors.toList()));
                    if (null == collect || collect.isEmpty()) return;
                    AtomicInteger index = new AtomicInteger(1);
                    collect.forEach((tag, his) -> {
                        capabilityBuilder.append("\t\t")
                                .append(index.getAndIncrement()).append(") ")
                                .append("%{TEST_OF_").append(tag).append("}%")
                                .append("\n");

                        String flag = History.SUCCEED.equals(tag) ? "✓" :
                                (History.ERROR.equals(tag) ? "✗" :
                                        (History.WARN.equals(tag) ? "！" : "？"));
                        for (int i = 0; i < his.size(); i++) {
                            capabilityBuilder.append("\t\t\t[")
                                    .append(flag)
                                    .append("].").append(his.get(i).message()).append("\n");
                        }
                    });
                }
            }
                /**
                for (Map.Entry<String, List<Map<Method, Case>>> caseGroup : caseGroupMapByTag.entrySet()) {
                    String key = caseGroup.getKey();
                    List<Map<Method,Case>> value = caseGroup.getValue();
                    switch (key){
                        case Case.ERROR:capabilityBuilder.append(TDDPrintf.format("ERROR.CASE"));break;
                        case Case.SUCCEED:capabilityBuilder.append(TDDPrintf.format("SUCCEED.CASE"));break;
                        case Case.WARN:capabilityBuilder.append(TDDPrintf.format("WARN.CASE"));break;
                    }
                    int caseIndex = 0 ;
                    for (Map<Method, Case> methodCaseMap : value) {
                        for (Map.Entry<Method, Case> methodCaseEntry : methodCaseMap.entrySet()) {
                            Case testCase = methodCaseEntry.getValue();

                            switch (testCase.tag()){
                                case Case.ERROR:errorSize++;break;
                                case Case.WARN:warnSize++;break;
                            }
                            capabilityBuilder.append("\t")
                                    .append(((char)(65+caseIndex)))
                                    .append(".")
                                    .append(TDDPrintf.format(testCase.message()))
                                    .append("\n");
                            caseIndex++;

                            List<History> history = testCase.histories();

                            Map<String, List<History>> collect = history.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(History::tag));
                            if (null == collect || collect.isEmpty()) return;
                            AtomicInteger index = new AtomicInteger(1);
                            collect.forEach((tag,his)->{
                                capabilityBuilder.append("\t\t")
                                        .append(index.getAndIncrement()).append(") ")
                                        .append("%{TEST_OF_").append(tag).append("}%")
                                        .append("\n");

                                String flag = History.SUCCEED.equals(tag)? "✓":
                                        (History.ERROR.equals(tag)? "✗":
                                                (History.WARN.equals(tag)? "！":"？"));
                                for (int i = 0; i < his.size(); i++) {
                                    capabilityBuilder.append("\t\t\t[")
                                            .append(flag)
                                            .append("].").append(his.get(i).message()).append("\n");
                                }
                            });
                        }
                    }
                }
                 */
//            }
            Annotation annotation = cla.getAnnotation(DisplayName.class);
            builder.append("☆ ")
                    .append(cla.getSimpleName());
            if (null!= annotation) {
                builder.append("(")
                        .append(TDDPrintf.format(((DisplayName)cla.getAnnotation(DisplayName.class)).value()))
                        .append(")");
            }
//            builder.append("\t[")
//                    .append(res.executionResult() == CapabilitiesExecutionMsg.ERROR?"%{TEST_RESULT_ERROR}%":"%{TEST_RESULT_SUCCEED}%")
//                    .append("(")
//                    .append("%{SUCCEED_COUNT_LABEL}%").append(caseMap.size()-warnSize-errorSize)
//                    .append(",%{WARN_COUNT_LABEL}%").append(warnSize)
//                    .append(",%{ERROR_COUNT_LABEL}%").append(errorSize)
//                    .append(")].");
            if (res.executionResult() != CapabilitiesExecutionMsg.ERROR && warnSize>0){
                builder.append(TDDPrintf.format("HAS_WARN_COUNT",warnSize));
            }
            builder.append("\n")
                    .append(capabilityBuilder)
                    .append("\n");
            builder.append("☆ ")
                    .append(TDDPrintf.format("ONCE_HISTORY",
                            cla.getSimpleName(),
                            res.executionResult() == CapabilitiesExecutionMsg.ERROR?TDDPrintf.format("TEST_RESULT_ERROR"):TDDPrintf.format("TEST_RESULT_SUCCEED"),
                            caseMap.size()-warnSize-errorSize,
                            warnSize,
                            errorSize,
                            0))
                    .append("\n");
//                    .append(cla.getSimpleName())
//                    .append("\t 结果：")
//                    .append(res.executionResult() == CapabilitiesExecutionMsg.ERROR?"%{TEST_RESULT_ERROR}%":"%{TEST_RESULT_SUCCEED}%")
//                    .append("%{SUCCEED_COUNT_LABEL}%").append(caseMap.size()-warnSize-errorSize)
//                    .append(",%{WARN_COUNT_LABEL}%").append(warnSize)
//                    .append(",%{ERROR_COUNT_LABEL}%").append(errorSize)
//                    .append(")].");
            //获取最终结果，如果出现Error，最终结果为Error.仅出现Warn，最终结果为Warn.
            int resResult = res.executionResult();
            if (allTestResult == CapabilitiesExecutionMsg.SUCCEED
                    && (resResult == CapabilitiesExecutionMsg.WARN || resResult == CapabilitiesExecutionMsg.ERROR)){
                allTestResult = resResult;
            }
            if (allTestResult == CapabilitiesExecutionMsg.WARN && resResult == CapabilitiesExecutionMsg.ERROR){
                allTestResult = resResult;
            }
            resultExecution.put(cla,allTestResult);
        }


        if(CapabilitiesExecutionMsg.ERROR == allTestResult && !Case.ERROR.equals(TapSummary.hasPass)) {
            //builder.append(TDDPrintf.format("TEST_ERROR_END","\n",fileName));
            TapSummary.hasPass = Case.ERROR;
        }else {
            //builder.append(TDDPrintf.format("TEST_SUCCEED_END","\n",fileName));
            if (CapabilitiesExecutionMsg.WARN == allTestResult && !Case.ERROR.equals(TapSummary.hasPass)){
                TapSummary.hasPass = Case.WARN;
                //builder.append(TDDPrintf.format("SUCCEED_WITH_WARN"));
            }
        }
        builder.append(AREA_SPLIT).append("\n");

        String finalStr = this.replaceAsLang(builder.toString());

        System.out.print(finalStr);
        //System.out.print(langType.getLanguage().equals("en")?format(finalStr):finalStr);
    }
//    private void showTest(TDDCli.TapSummary summary,TapNodeInfo nodeInfo,String fileName){
//        Map<Class, CapabilitiesExecutionMsg> result = summary.capabilitiesResult;
//        StringBuilder builder = new StringBuilder(AREA_SPLIT);
//        builder.append("------------PDK id '")
//                .append(summary.tapNodeInfo.getTapNodeSpecification().getId())
//                .append("' class '")
//                .append(summary.tapNodeInfo.getNodeClass().getName())
//                .append("'------------\n");
//        showCapabilities(nodeInfo,builder);
//        if (null != summary.doNotSupportFunTest && !summary.doNotSupportFunTest.isEmpty()){
//            summary.doNotSupportFunTest.forEach(((aClass, s) -> {
//                builder.append("☆ ")
//                        .append(aClass.getSimpleName()).append("\n[%{TEST_RESULT_ERROR}%]\n")
//                        .append("\t\t").append(s).append("\n");
//            }));
//        }
//        int allTestResult = CapabilitiesExecutionMsg.SUCCEED;//default 0 === succeed
//        for (Map.Entry<Class,CapabilitiesExecutionMsg> entry : result.entrySet()){
//            Class cla = entry.getKey();
//            CapabilitiesExecutionMsg res = entry.getValue();
//
//            builder.append("☆ ")
//                    .append(cla.getSimpleName()).append("\n[")
//                    .append(res.executionResult() == CapabilitiesExecutionMsg.ERROR?"%{TEST_RESULT_ERROR}%":"%{TEST_RESULT_SUCCEED}%")
//                    .append("(");
//            List<History> history = res.history();
//            Map<String, List<History>> collect = history.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(History::tag));
//            if (null == collect || collect.isEmpty()) return;
//            List<History> warnHistory = collect.get(History.WARN);
//            List<History> errorHistory = collect.get(History.ERROR);
//            int warnSize = null==warnHistory?0:warnHistory.size();
//            int errorSize = null==errorHistory?0:errorHistory.size();
//            builder.append("%{SUCCEED_COUNT_LABEL}%").append(res.executionTimes()-warnSize-errorSize)
//                    .append(",%{WARN_COUNT_LABEL}%").append(warnSize)
//                    .append(",%{ERROR_COUNT_LABEL}%").append(errorSize)
//                    .append(")]");
//
//            if (res.executionResult() != CapabilitiesExecutionMsg.ERROR && null != warnHistory && !warnHistory.isEmpty()){
//                builder.append("%{HAS_WARN_COUNT}%")
//                        .append(warnHistory.size())
//                        .append(".");
//            }
//            builder.append(".\n");
//            AtomicInteger index = new AtomicInteger(1);
//            collect.forEach((tag,his)->{
//                builder.append("\t\t")
//                        .append(index.getAndIncrement()).append(") ")
//                        .append("%{TEST_OF_").append(tag).append("}%")
//                        .append("\n");
//
//                String flag = History.SUCCEED.equals(tag)? "✓":
//                        (History.ERROR.equals(tag)? "✗":
//                                (History.WARN.equals(tag)? "！":"？"));
//                for (int i = 0; i < his.size(); i++) {
//                    builder.append("\t\t\t[")
//                            .append(flag)
//                            .append("].").append(his.get(i).message()).append("\n");
//                }
//            });
//
//            //获取最终结果，如果出现Error，最终结果为Error.仅出现Error，最终结果为Error.
//            int resResult = res.executionResult();
//            if (allTestResult == CapabilitiesExecutionMsg.SUCCEED
//                    && (resResult == CapabilitiesExecutionMsg.WARN || resResult == CapabilitiesExecutionMsg.ERROR)){
//                allTestResult = resResult;
//            }
//            if (allTestResult == CapabilitiesExecutionMsg.WARN && resResult == CapabilitiesExecutionMsg.ERROR){
//                allTestResult = resResult;
//            }
//        }
//
//        if(CapabilitiesExecutionMsg.ERROR == allTestResult) {
//            builder.append("\n(╳) Oops, PDK ")
//                    .append(fileName)
//                    .append(" %{TEST_ERROR_SUF}%\n\n");
//        }else {
//            builder.append("\n(✔)%{TEST_SUCCEED_PRE}%, PDK ")
//                    .append(fileName)
//                    .append(" %{TEST_SUCCEED_SUF}%");
//            if (CapabilitiesExecutionMsg.WARN == allTestResult){
//                builder.append("%{SUCCEED_WITH_WARN}%");
//            }
//        }
//        builder.append("\n").append(AREA_SPLIT);
//
//        String finalStr = this.replaceAsLang(builder.toString());
//
//        System.out.print(finalStr);
//        //System.out.print(langType.getLanguage().equals("en")?format(finalStr):finalStr);
//    }

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
                .withLog(new TapLog())
                .withTable(UUID.randomUUID().toString())
                .build();

        TapNodeInfo tapNodeInfo = connectorNode.getTapNodeInfo();
        TapNodeSpecification tapNodeSpecification = tapNodeInfo.getTapNodeSpecification();
        show.append(AREA_SPLIT).append("\n")
                .append(tapNodeSpecification.getName())
                .append(TDDPrintf.format("CONNECTOR_CAPABILITIES_START","\n"));
        ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
        List<Capability> capabilities = connectorFunctions.getCapabilities();
        AtomicInteger index = new AtomicInteger();
        capabilities.stream().filter(Objects::nonNull).forEach(capability -> show.append(TDDPrintf.format(capability.getId(),"\t",index.incrementAndGet(),"\n")));
        show.append(TDDPrintf.format("TOTAL_CAPABILITIES_OF",tapNodeSpecification.getName(),index.get(),"\n"));
        return show;
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
//        /**
//         * 引擎启动
//         */
//        public void start() {
//            TapLogger.info(TAG, ".___________.    ___      .______    _______       ___   .___________.    ___     ");
//            TapLogger.info(TAG, "|           |   /   \\     |   _  \\  |       \\     /   \\  |           |   /   \\    ");
//            TapLogger.info(TAG, "`---|  |----`  /  ^  \\    |  |_)  | |  .--.  |   /  ^  \\ `---|  |----`  /  ^  \\   ");
//            TapLogger.info(TAG, "    |  |      /  /_\\  \\   |   ___/  |  |  |  |  /  /_\\  \\    |  |      /  /_\\  \\  ");
//            TapLogger.info(TAG, "    |  |     /  _____  \\  |  |      |  '--'  | /  _____  \\   |  |     /  _____  \\ ");
//            TapLogger.info(TAG, "    |__|    /__/     \\__\\ | _|      |_______/ /__/     \\__\\  |__|    /__/     \\__\\");
//            TapLogger.info(TAG, "                                                                            v{}", version);
//            //http://www.network-science.de/ascii/
//            //starwars
//
//        }
        System.out.println("\n"+AREA_SPLIT+ "\n[.___________.    ___      .______    _______       ___   .___________.    ___     ]\n" +
                            "[|           |   /   \\     |   _  \\  |       \\     /   \\  |           |   /   \\    ]\n" +
                            "[`---|  |----`  /  ^  \\    |  |_)  | |  .--.  |   /  ^  \\ `---|  |----`  /  ^  \\   ]\n" +
                            "[    |  |      /  /_\\  \\   |   ___/  |  |  |  |  /  /_\\  \\    |  |      /  /_\\  \\  ]\n" +
                            "[    |  |     /  _____  \\  |  |      |  '--'  | /  _____  \\   |  |     /  _____  \\ ]\n" +
                            "[    |__|    /__/     \\__\\ | _|      |_______/ /__/     \\__\\  |__|    /__/     \\__\\]\n"
                +AREA_SPLIT);
    }

    private String formatStr(String txt){
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

    public static String format(String key, Object ... formatValue){
        String tap_lang = CommonUtils.getProperty("tap_lang");
        ResourceBundle lang = ResourceBundle.getBundle(LANG_PATH, new Locale(tap_lang));
        String value = lang.getString(key);
        if (null == value) return "?";
        if (null==formatValue||formatValue.length<1) return value;
        return String.format(value,formatValue);
    }
}
