package io.tapdata.pdk.cli.commands;
import javax.swing.JFileChooser;
import javax.swing.JPanel;
import javax.swing.filechooser.FileNameExtensionFilter;
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
import io.tapdata.pdk.tdd.tests.support.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

import java.io.BufferedWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
//    public static class TapSummary {
//        public TapSummary() {}
//        TapNodeInfo tapNodeInfo;
//        TestExecutionSummary summary;
//        List<Class<?>> testClasses = new ArrayList<>();
//
//        //存放所有测试类的所有测试用例的全部执行断言的结果
//        public static Map<Class, CapabilitiesExecutionMsg> capabilitiesResult = new HashMap<>();
//
//        //存放没有实现方法二不能进行测试的测试类
//        Map<Class,String> doNotSupportFunTest = new HashMap<>();
//
//        //存放所有测试类的最终测试结果
//        Map<Class<? extends PDKTestBase>,Integer> resultExecution = new HashMap<>();
//
//        //用来表示一次测试过程是否通过
//        public static String hasPass = "SUCCEED";
//        //每轮测试结束需要调用这个方法进行清除一些数据
//        public void clean(){
//            TapSummary.capabilitiesResult = new HashMap<>();
//            doNotSupportFunTest = new HashMap<>();
//            testClasses = new ArrayList<>();
//        }
//    }
public class TapSummary {
    TapNodeInfo tapNodeInfo;
    TestExecutionSummary summary;
    List<Class<?>> testClasses = new ArrayList<>();
    StringBuilder resultBuilder = new StringBuilder();

    //存放所有测试类的所有测试用例的全部执行断言的结果
    public static Map<Class, CapabilitiesExecutionMsg> capabilitiesResult = new HashMap<>();

    //存放没有实现方法二不能进行测试的测试类
    Map<Class,String> doNotSupportFunTest = new HashMap<>();

    //存放所有测试类的最终测试结果
    Map<Class<? extends PDKTestBase>,Integer> resultExecution = new HashMap<>();

    //用来表示一次测试过程是否通过
    public static String hasPass = "SUCCEED";
    //每轮测试结束需要调用这个方法进行清除一些数据
    public void clean(){
        TapSummary.capabilitiesResult = new HashMap<>();
        doNotSupportFunTest = new HashMap<>();
        testClasses = new ArrayList<>();
    }


    List<TapSummary> summarys;
    private static final String LANG_PATH = "i18n.lang";
    private Locale langType = Locale.SIMPLIFIED_CHINESE;
    private final String AREA_SPLIT = "------------------------------------------------------------------------------------";
    private final String AREA_SPLIT_SIMPLE = "————————————————————————————————————————————————————————————————————————————————————";
    public TapSummary setLanType(Locale langType){
        this.langType = langType;
        return this;
    }
    public static TapSummary create(List<TapSummary> summarys){
        return new TapSummary(summarys);
    }
    private TapSummary(List<TapSummary> summarys){
        this.summarys = summarys;
    }
    public static TapSummary create(){
        return new TapSummary();
    }
    private TapSummary(){
    }

    public void showTestResultAll(TapNodeInfo nodeInfo,String fileName){
        if (null != summarys && !summarys.isEmpty()) {
            summarys.stream().filter(Objects::nonNull).forEach(summary -> showTest(summary));
        }
    }
    public void showTestResult(TapSummary summary){
        showTest(summary);
        clean();
    }
    public void showCapabilities(TapNodeInfo nodeInfo){
        this.tapNodeInfo = nodeInfo;
        System.out.println(showCapabilitiesV2());
    }
    private String showCapabilitiesV2(){
        StringBuilder builder = new StringBuilder(AREA_SPLIT);
        builder.append("\n------------PDK id '")
                .append(tapNodeInfo.getTapNodeSpecification().getId())
                .append("' class '")
                .append(tapNodeInfo.getNodeClass().getName())
                .append("'------------\n");
        showCapabilities(tapNodeInfo,builder);
        return builder.toString();
    }

    private void showNotSupport(TapSummary summary,StringBuilder builder){
        if (null != summary.doNotSupportFunTest && !summary.doNotSupportFunTest.isEmpty()){
//            summary.doNotSupportFunTest.forEach(((aClass, s) -> {
//                builder.append("☆ ")
//                        .append(aClass.getSimpleName());
//                String annotation = this.getAnnotationName(aClass,DisplayName.class);
//                if (null!= annotation) {
//                    builder.append("(").append(annotation).append(")");
//                }
//                builder.append("\n")
//                        .append("\t").append(s).append("\n");
//                builder.append("☆ ")
//                        .append(TapSummary.format("ONCE_HISTORY",
//                                aClass.getSimpleName(),
//                                TapSummary.format("TEST_RESULT_ERROR"),
//                                0,
//                                0,
//                                0,
//                                1))
//                        .append("\n");
//            }));
            Iterator<Map.Entry<Class, String>> iterator = summary.doNotSupportFunTest.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<Class, String> next = iterator.next();
                Class aClass = next.getKey();
                String s = next.getValue();

                builder.append("☆ ")
                        .append(aClass.getSimpleName());
                String annotation = this.getAnnotationName(aClass,DisplayName.class);
                if (null!= annotation) {
                    builder.append("(").append(annotation).append(")");
                }
                builder.append("\n")
                        .append("\t").append(s).append("\n");

                int jumpCase = 0;
                try {
                    Object test = aClass.newInstance();
                    Method[] declaredMethods = aClass.getDeclaredMethods();
                    for (int i = 0; i < declaredMethods.length; i++) {
                        Test testAnn = declaredMethods[i].getAnnotation(Test.class);
                        if (null != testAnn) jumpCase++;
                    }
                } catch (InstantiationException | IllegalAccessException e) {
                }
                builder.append("★ ")
                        .append(TapSummary.format("ONCE_HISTORY",
                                aClass.getSimpleName(),
                                TapSummary.format("TEST_RESULT_ERROR"),
                                0,
                                0,
                                0,
                                jumpCase))
                        .append("\n");

                if (iterator.hasNext()){
                    builder.append(AREA_SPLIT_SIMPLE).append("\n");
                }
            }
        }
    }

    private void showTest(TapSummary summary){
        Map<Class<? extends PDKTestBase>,Integer> resultExecution = summary.resultExecution;
        Map<Class, CapabilitiesExecutionMsg> result = summary.capabilitiesResult;
        StringBuilder builder = new StringBuilder(AREA_SPLIT);
        builder.append("\n");
        this.showNotSupport(summary,builder);
        int allTestResult = CapabilitiesExecutionMsg.SUCCEED;//default 0 === succeed
        for (Map.Entry<Class,CapabilitiesExecutionMsg> entry : result.entrySet()){
            Class cla = entry.getKey();
            CapabilitiesExecutionMsg res = entry.getValue();

            StringBuilder capabilityBuilder = new StringBuilder();
            Map<Method, Case> methodCaseMap = res.testCases();
            int warnSize = 0;
            int errorSize = 0;
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
                            .append(TapSummary.format(testCase.message()))
                            .append("\n");
                    caseIndex++;

                    List<History> history = testCase.histories();
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
            builder.append("☆ ")
                    .append(cla.getSimpleName());
            String annotation = this.getAnnotationName(cla,DisplayName.class);
            if (null!= annotation) {
                builder.append("(").append(annotation).append(")");
            }
//            if (res.executionResult() != CapabilitiesExecutionMsg.ERROR && warnSize>0){
//                builder.append(TapSummary.format("HAS_WARN_COUNT",warnSize));
//            }
            builder.append("\n")
                    .append(capabilityBuilder)
                    .append("\n");
            builder.append("★ ")
                    .append(TapSummary.format("ONCE_HISTORY",
                            cla.getSimpleName(),
                            res.executionResult() == CapabilitiesExecutionMsg.ERROR?TapSummary.format("TEST_RESULT_ERROR"):TapSummary.format("TEST_RESULT_SUCCEED"),
                            methodCaseMap.size()-warnSize-errorSize,
                            warnSize,
                            errorSize,
                            0))
                    .append("\n");
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
            TapSummary.hasPass = Case.ERROR;
        }else {
            if (CapabilitiesExecutionMsg.WARN == allTestResult && !Case.ERROR.equals(TapSummary.hasPass)){
                TapSummary.hasPass = Case.WARN;
            }
        }
        builder.append(AREA_SPLIT).append("\n");

        String finalStr = this.replaceAsLang(builder.toString());

        System.out.print(finalStr);

        resultBuilder.append(finalStr);
    }

    private String fileChooser() {
        JFileChooser chooser = new JFileChooser();
        //打开选择器面板
        int returnVal = chooser.showSaveDialog(new JPanel());
        //保存文件从这里入手，输出的是文件名
        if(returnVal == JFileChooser.APPROVE_OPTION) {
            String path = chooser.getSelectedFile().getPath();
            return !path.endsWith(".txt")?path.replaceAll("\\.","_")+".txt":path;
        }
        //@TODO default save path
        return "D:\\GavinData\\deskTop\\newFile_"+System.nanoTime()+".txt";
    }
    public void asFile(String file){
        System.out.println("\n(？)Do you want to printout as a file?(y/n):");
        Scanner scanner = new Scanner(System.in);
        String cmd = scanner.nextLine();
        while (!"N".equals(cmd) && !"n".equals(cmd) && !"Y".equals(cmd) && !"y".equals(cmd)) {
            System.out.print("(！)Please enter Y or y or N or n to make a decision:");
            cmd = scanner.nextLine();
        }
        if (("Y").equals(cmd) || "y".equals(cmd)) {
            String fileName = fileChooser();
            Path path = Paths.get(fileName);
            try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
                resultBuilder.append("\n\n")
                        .append(AREA_SPLIT_SIMPLE)
                        .append("\n\nprint time:")
                        .append(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date()))
                        .append("\n");
                writer.write(
                        showLogoV2() +
                             showCapabilitiesV2()+
                        resultBuilder.toString()+
                             endingShowV2(file)
                );
            }catch (Exception e) {
            }
        }
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
        show.append(AREA_SPLIT).append("\n")
                .append(tapNodeSpecification.getName())
                .append(TapSummary.format("CONNECTOR_CAPABILITIES_START","\n"));
        ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
        List<Capability> capabilities = connectorFunctions.getCapabilities();
        AtomicInteger index = new AtomicInteger();
        capabilities.stream().filter(Objects::nonNull).forEach(capability -> show.append(TapSummary.format(capability.getId(),"\t",index.incrementAndGet(),"\n")));
        show.append(TapSummary.format("TOTAL_CAPABILITIES_OF",tapNodeSpecification.getName(),index.get(),"\n"));
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
        System.out.println(showLogoV2());
    }
    private String showLogoV2(){
        String logo = "\n"+AREA_SPLIT+ "\n"+
                "[.___________.    ___      .______    _______       ___   .___________.    ___     ]\n" +
                "[|           |   /   \\     |   _  \\  |       \\     /   \\  |           |   /   \\    ]\n" +
                "[`---|  |----`  /  ^  \\    |  |_)  | |  .--.  |   /  ^  \\ `---|  |----`  /  ^  \\   ]\n" +
                "[    |  |      /  /_\\  \\   |   ___/  |  |  |  |  /  /_\\  \\    |  |      /  /_\\  \\  ]\n" +
                "[    |  |     /  _____  \\  |  |      |  '--'  | /  _____  \\   |  |     /  _____  \\ ]\n" +
                "[    |__|    /__/     \\__\\ | _|      |_______/ /__/     \\__\\  |__|    /__/     \\__\\]\n"
                +AREA_SPLIT;
        return logo;
    }

    public static String format(String key, Object ... formatValue){
        String tap_lang = CommonUtils.getProperty("tap_lang");
        ResourceBundle lang = ResourceBundle.getBundle(LANG_PATH, new Locale(tap_lang));
        String value = "?";
        try {
            value = lang.getString(key);
        }catch (Exception e){
            return key;
        }
        if (null==formatValue||formatValue.length<1) return value;
        return String.format(value,formatValue);
    }

    private String getAnnotationName(Class objectCla,Class annotationCla){
        Annotation annotation = objectCla.getAnnotation(annotationCla);
        if (null == annotation) return "?";
        return TapSummary.format(((DisplayName)objectCla.getAnnotation(DisplayName.class)).value());
    }

    public void endingShow(String fileName){
        System.out.println(endingShowV2(fileName));
    }
    public String endingShowV2(String fileName){
        String msg = "";
        if(Case.ERROR.equals(TapSummary.hasPass)) {
            msg = TapSummary.format("TEST_ERROR_END","",fileName);
        }else {
            msg = TapSummary.format("TEST_SUCCEED_END","",fileName);
            if (Case.WARN.equals(TapSummary.hasPass)){
                msg += TapSummary.format("SUCCEED_WITH_WARN");
            }
        }
        final int len = msg.getBytes().length;
        String charStr = ".";
        StringBuilder splitStartAndEnd = new StringBuilder();
        for (int i = 0; i < len; i++) {
            splitStartAndEnd.append(charStr);
        }
        return splitStartAndEnd.toString()+"\n"+msg+"\n"+splitStartAndEnd.toString()+"\n";
    }

}