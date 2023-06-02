package io.tapdata.pdk.tdd.tests.support;

import javax.swing.JFileChooser;
import javax.swing.JPanel;

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
import io.tapdata.pdk.tdd.tests.support.printf.ChokeTag;
import io.tapdata.pdk.tdd.tests.support.printf.SummaryData;
import org.apache.commons.io.FilenameUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

import java.io.BufferedWriter;
import java.io.File;
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

    public void summary(TestExecutionSummary summary) {
        this.summary = summary;
    }

    public TestExecutionSummary summary() {
        return this.summary;
    }

    public List<Class<?>> testClasses() {
        return this.testClasses;
    }

    public Map<Class, String> doNotSupportFunTest() {
        return this.doNotSupportFunTest;
    }

    public void tapNodeInfo(TapNodeInfo tapNodeInfo) {
        this.tapNodeInfo = tapNodeInfo;
    }

    public TapNodeInfo tapNodeInfo() {
        return this.tapNodeInfo;
    }

    //存放所有测试类的所有测试用例的全部执行断言的结果
    public static Map<Class, CapabilitiesExecutionMsg> capabilitiesResult = new HashMap<>();

    //存放没有实现方法二不能进行测试的测试类
    Map<Class, String> doNotSupportFunTest = new HashMap<>();
    Map<Class<? extends PDKTestBase>, String> doSupportFunTest = new HashMap<>();

    public Map<Class<? extends PDKTestBase>, String> doSupportFunTest() {
        return this.doSupportFunTest;
    }

    //存放所有测试类的最终测试结果
    Map<Class<? extends PDKTestBase>, Integer> resultExecution = new HashMap<>();

    public Map<Class<? extends PDKTestBase>, Integer> resultExecution() {
        return this.resultExecution;
    }

    //用来表示一次测试过程是否通过
    public static String hasPass = "SUCCEED";

    SummaryData summaryData = new SummaryData();
    public SummaryData summaryData(){
        return this.summaryData;
    }

    //每轮测试结束需要调用这个方法进行清除一些数据
    public void clean() {
        TapSummary.capabilitiesResult = new HashMap<>();
        //doNotSupportFunTest = new HashMap<>();
        testClasses = new ArrayList<>();
    }


    List<TapSummary> summarys;

    private Locale langType = Locale.SIMPLIFIED_CHINESE;
    private final String AREA_SPLIT = "------------------------------------------------------------------------------------";
    private final String AREA_SPLIT_SIMPLE = "————————————————————————————————————————————————————————————————————————————————————";

    public TapSummary setLanType(Locale langType) {
        this.langType = langType;
        return this;
    }

    public static TapSummary create(List<TapSummary> summarys) {
        return new TapSummary(summarys);
    }

    private TapSummary(List<TapSummary> summarys) {
        this.summarys = summarys;
    }

    public static TapSummary create() {
        return new TapSummary();
    }

    private TapSummary() {
    }

    public void showTestResultAll(TapNodeInfo nodeInfo, String fileName) {
        if (null != summarys && !summarys.isEmpty()) {
            summarys.stream().filter(Objects::nonNull).forEach(summary -> {
                showTest(summary);
                this.showNotSupport(summary);
            });
        }
    }

    public ChokeTag showTestResult(TapSummary summary) {
        ChokeTag chokeTag = showTest(summary);
        clean();
        return chokeTag;
    }

    public void showCapabilities(TapNodeInfo nodeInfo) {
        this.tapNodeInfo = nodeInfo;
        System.out.println(showCapabilitiesV2());
    }

    private String showCapabilitiesV2() {
        StringBuilder builder = new StringBuilder(AREA_SPLIT);
        builder.append("\n------------PDK id '")
                .append(tapNodeInfo.getTapNodeSpecification().getId())
                .append("' class '")
                .append(tapNodeInfo.getNodeClass().getName())
                .append("'------------\n");
        showCapabilities(tapNodeInfo, builder);
        return builder.toString();
    }

    private StringBuilder showNotSupport(TapSummary summary) {
        StringBuilder builder = new StringBuilder(AREA_SPLIT + "\n");
        if (null != summary.doNotSupportFunTest && !summary.doNotSupportFunTest.isEmpty()) {
            Iterator<Map.Entry<Class, String>> iterator = summary.doNotSupportFunTest.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Class, String> next = iterator.next();
                Class aClass = next.getKey();
                String s = next.getValue();

                builder.append("☆ ")
                        .append(aClass.getSimpleName());
                String annotation = this.getAnnotationName(aClass, DisplayName.class);
                if (null != annotation) {
                    builder.append("(").append(annotation).append(")");
                }
                builder.append("\n")
                        .append("\t").append(s).append("\n");

                int jumpCase = 0;

                List<Method> declaredMethods = (new HashSet<>(Arrays.asList(aClass.getDeclaredMethods())))
                        .stream()
                        .filter(cls -> null != cls.getAnnotation(Test.class))
                        .sorted((m1, m2) -> {
                            TapTestCase testCase1 = m1.getDeclaredAnnotation(TapTestCase.class);
                            TapTestCase testCase2 = m2.getDeclaredAnnotation(TapTestCase.class);
                            return null == testCase1 || null == testCase2 || testCase1.sort() > testCase2.sort() ? 0 : -1;
                        }).collect(Collectors.toList());
                StringBuilder jumpCaseTest = new StringBuilder();
                int caseIndex = 0;
                for (Method method : declaredMethods) {
                    Test testAnn = method.getAnnotation(Test.class);
                    DisplayName testCase = method.getAnnotation(DisplayName.class);
                    if (null != testAnn) {
                        jumpCaseTest.append("\t\t")
                                .append(((char) (65 + caseIndex)))
                                .append(".")
                                .append(LangUtil.format(testCase.value()))
                                .append("\n");
                        jumpCase++;
                        caseIndex++;
                    }
                }
                if (caseIndex > 0) {
                    builder.append("\t◉ ")
                            .append(LangUtil.format("base.jumpCase.list"))
                            .append("\n")
                            .append(jumpCaseTest);
                }
                summaryData.summaryOnce(jumpCase, 0, 0, 0, 0, jumpCase);
                builder.append("★ ")
                        .append(LangUtil.format("ONCE_HISTORY",
                                aClass.getSimpleName(),
                                LangUtil.format("TEST_RESULT_WARN"),
                                0,
                                0,
                                0,
                                jumpCase))
                        .append("\n");

                if (iterator.hasNext()) {
                    builder.append(AREA_SPLIT_SIMPLE).append("\n");
                }
            }
        }
        builder.append(AREA_SPLIT).append("\n");
        return builder;
    }

    private ChokeTag showTest(TapSummary summary) {
        ChokeTag chokeTag = ChokeTag.tag();
        Map<Class<? extends PDKTestBase>, Integer> resultExecution = summary.resultExecution;
        Map<Class, CapabilitiesExecutionMsg> result = summary.capabilitiesResult;
        if (null == result || result.isEmpty()) return chokeTag;
        StringBuilder builder = new StringBuilder(AREA_SPLIT);
        builder.append("\n");
        int allTestResult = CapabilitiesExecutionMsg.SUCCEED;//default 0 === succeed
        for (Map.Entry<Class, CapabilitiesExecutionMsg> entry : result.entrySet()) {
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
                            .append(LangUtil.format(testCase.message()))
                            .append("\n");
                    caseIndex++;

                    List<History> history = testCase.histories();
                    TreeMap<String, List<History>> collect = history.stream()
                            .filter(Objects::nonNull)
                            .collect(Collectors.groupingBy(History::tag, TreeMap::new, Collectors.toList()));
                    if (null == collect || collect.isEmpty()) return chokeTag;
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
            int total = null == methodCaseMap || methodCaseMap.isEmpty() ? 0 : methodCaseMap.size();
            builder.append("☆ ")
                    .append(cla.getSimpleName());
            String annotation = this.getAnnotationName(cla, DisplayName.class);
            if (null != annotation) {
                builder.append("(").append(annotation).append(")");
            }
            builder.append("\n")
                    .append(capabilityBuilder)
                    .append("\n");
            summaryData.summaryOnce(total,
                    total,
                    total - warnSize - errorSize,
                    warnSize,
                    errorSize,
                    0
            );
            builder.append("★ ")
                    .append(LangUtil.format("ONCE_HISTORY",
                            cla.getSimpleName(),
                            res.executionResult() == CapabilitiesExecutionMsg.ERROR ? LangUtil.format("TEST_RESULT_ERROR") : LangUtil.format("TEST_RESULT_SUCCEED"),
                            methodCaseMap.size() - warnSize - errorSize,
                            warnSize,
                            errorSize,
                            0))
                    .append("\n");
            //获取最终结果，如果出现Error，最终结果为Error.仅出现Warn，最终结果为Warn.
            int resResult = res.executionResult();
            if (resResult == CapabilitiesExecutionMsg.ERROR) {
                Optional.ofNullable(cla.getAnnotation(TapGo.class)).ifPresent(go -> {
                    if (((TapGo) go).block()) {
                        chokeTag.blocked();//一个需要阻塞的用例执行失败了，其他用例不再执行
                    }
                });
            }
            if (allTestResult == CapabilitiesExecutionMsg.SUCCEED
                    && (resResult == CapabilitiesExecutionMsg.WARN || resResult == CapabilitiesExecutionMsg.ERROR)) {
                allTestResult = resResult;
            }
            if (allTestResult == CapabilitiesExecutionMsg.WARN && resResult == CapabilitiesExecutionMsg.ERROR) {
                allTestResult = resResult;
            }
            resultExecution.put(cla, allTestResult);
        }
        if (CapabilitiesExecutionMsg.ERROR == allTestResult && !Case.ERROR.equals(TapSummary.hasPass)) {
            TapSummary.hasPass = Case.ERROR;
        } else {
            if (CapabilitiesExecutionMsg.WARN == allTestResult && !Case.ERROR.equals(TapSummary.hasPass)) {
                TapSummary.hasPass = Case.WARN;
            }
        }
        builder.append(AREA_SPLIT).append("\n");

        String finalStr = this.replaceAsLang(builder.toString());

        System.out.print(finalStr);

        resultBuilder.append(finalStr);

        return chokeTag;
    }

    private String fileChooser() {
        JFileChooser chooser = new JFileChooser();
        //打开选择器面板
        int returnVal = chooser.showSaveDialog(new JPanel());
        //保存文件从这里入手，输出的是文件名
        if (returnVal == JFileChooser.APPROVE_OPTION) {
            String path = chooser.getSelectedFile().getPath();
            return !path.endsWith(".txt") ? path.replaceAll("\\.", "_") + ".txt" : path;
        }
        //@TODO default save path
        return "D:\\GavinData\\deskTop\\newFile_" + System.nanoTime() + ".txt";
    }

    public void asFile(String file) {
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
                                showCapabilitiesV2() +
                                resultBuilder.toString() +
                                endingShowV2(file)
                );
            } catch (Exception e) {
            }
        }
    }

    public void asFileV2(String file) {
        String path = CommonUtils.getProperty("tap_log_path");
        String fileName = tapNodeInfo.getTapNodeSpecification().getId() + "-connector-v1.0-TDD-TEST-" + dateTime() + ".log";
        path += fileName;
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(path), StandardCharsets.UTF_8)) {
            writer.write(
                    showLogoV2() + "\n" +
                            showCapabilitiesV2() +
                            resultBuilder.toString()

            );
        } catch (Exception e) {
        }
    }

    public StringBuilder showCapabilities(TapNodeInfo nodeInfo, StringBuilder show) {
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
                .append(LangUtil.format("CONNECTOR_CAPABILITIES_START", "\n"));
        ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
        List<Capability> capabilities = connectorFunctions.getCapabilities();
        AtomicInteger index = new AtomicInteger();
        capabilities.stream().filter(Objects::nonNull).forEach(capability -> show.append("\t")
                .append(index.incrementAndGet())
                .append(". ")
                .append(LangUtil.format(capability.getId()))
                .append("\n")
        );
        show.append(LangUtil.format("TOTAL_CAPABILITIES_OF", tapNodeSpecification.getName(), index.get(), "\n"));
        return show;
    }

    private String replaceAsLang(String txt) {
        ResourceBundle lang = ResourceBundle.getBundle(LangUtil.LANG_PATH_V1, langType);
        int startAt = 0;
        int length = txt.length();
        while (startAt <= length) {
            int splitAt = txt.indexOf("%{", startAt);
            int splitEnd = txt.indexOf("}%", splitAt);
            if (splitAt < 0 || splitEnd < 0) break;
            String key = txt.substring(splitAt + 2, splitEnd);
            try {
                String langValue = lang.getString(key);
                txt = txt.replaceAll("%\\{" + key + "}%", !"".equals(langValue) ? langValue : "?");
                startAt = splitAt + langValue.length() - 1;
            } catch (MissingResourceException e) {
                txt = txt.replaceAll("%\\{" + key + "}%", "?");
                startAt = splitAt + 1;
            } catch (Exception e) {
                startAt = splitEnd + 1;
            }
        }
        return txt;
//        return txt.replaceAll("\\n","\n|");
    }

    public void showLogo() {
        System.out.println(showLogoV2());
    }

    private String showLogoV2() {
        String logo = "\n" + AREA_SPLIT + "\n" +
                "[.___________.    ___      .______    _______       ___   .___________.    ___     ]\n" +
                "[|           |   /   \\     |   _  \\  |       \\     /   \\  |           |   /   \\    ]\n" +
                "[`---|  |----`  /  ^  \\    |  |_)  | |  .--.  |   /  ^  \\ `---|  |----`  /  ^  \\   ]\n" +
                "[    |  |      /  /_\\  \\   |   ___/  |  |  |  |  /  /_\\  \\    |  |      /  /_\\  \\  ]\n" +
                "[    |  |     /  _____  \\  |  |      |  '--'  | /  _____  \\   |  |     /  _____  \\ ]\n" +
                "[    |__|    /__/     \\__\\ | _|      |_______/ /__/     \\__\\  |__|    /__/     \\__\\]\n"
                + AREA_SPLIT;
        return logo;
    }

    private String getAnnotationName(Class objectCla, Class annotationCla) {
        Annotation annotation = objectCla.getAnnotation(annotationCla);
        if (null == annotation) return "?";
        return LangUtil.format(((DisplayName) objectCla.getAnnotation(DisplayName.class)).value());
    }

    public void endingShow(TapSummary summary, String fileName) {
        StringBuilder builder = this.showNotSupport(summary);
        System.out.println(builder.toString());
        resultBuilder.append(builder);

        StringBuilder builderEnd = new StringBuilder("\n\n====================================================================================\n");
        String end = endingShowV2(fileName);
        String connectorName = tapNodeInfo.getTapNodeSpecification().getId();
        int needCaseNum = summaryData.needExecCase();
        int exeCase = summaryData.exeCase();
        int error = summaryData.error();
        int succeed = summaryData.succeed();
        int warn = summaryData.warn();
        int dump = summaryData.dump();
        int totalCase = needCaseNum + dump;// summaryData.totalCase();
        int notExecActual = needCaseNum - exeCase;
        builderEnd.append(LangUtil.format("SUMMARY_END", connectorName, totalCase, exeCase, succeed, warn, error, dump, notExecActual))
                .append("————————————————————————————————————————————————————————————————————————————————————\n")
                .append(end)
                .append("\n")
                .append("====================================================================================\n");

        System.out.println(builderEnd.toString());
        resultBuilder.append(builderEnd)
                .append("\n")
                .append(DateUtil.dateTimeToStr());

    }

    public String endingShowV2(String fileName) {
        String msg = "";
        if (Case.ERROR.equals(TapSummary.hasPass)) {
            msg = LangUtil.format("TEST_ERROR_END", "", fileName);
        } else {
            msg = LangUtil.format("TEST_SUCCEED_END", "", fileName);
            if (Case.WARN.equals(TapSummary.hasPass)) {
                msg += LangUtil.format("SUCCEED_WITH_WARN");
            }
        }

        return msg;
    }

    public static String basePath(String logPath) {
        try {
            Path path = Paths.get(logPath);
            String pathFinal = path.toFile().getCanonicalPath() + "/";
            File file = new File(pathFinal);
            if (!file.exists()) {
                file.mkdir();
            }
            return pathFinal;
        } catch (Throwable throwable) {
            String pathFinal = FilenameUtils.concat(logPath, "../");
            File file = new File(pathFinal);
            if (!file.exists()) {
                file.mkdir();
            }
            return pathFinal;
        }
    }

    private static String dateTime() {
        String format = "yyyyMMdd_HHmmss";
        return (new SimpleDateFormat(format)).format(new Date());
    }

    public void summaryInEnding(StringBuilder builder) {


    }

}
