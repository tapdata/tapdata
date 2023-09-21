package io.tapdata.pdk.cli.commands;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.cli.CommonCli;
import io.tapdata.pdk.core.connector.TapConnector;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.entity.utils.ReflectionUtil;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.basic.BasicTest;
import io.tapdata.pdk.tdd.tests.support.LangUtil;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapSummary;
import io.tapdata.pdk.tdd.tests.support.printf.ChokeTag;
import io.tapdata.pdk.tdd.tests.support.printf.SummaryData;
import io.tapdata.pdk.tdd.tests.v2.WriteRecordTest;
import org.apache.commons.io.FilenameUtils;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.apache.maven.shared.invoker.*;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;
import org.reflections.Reflections;
import picocli.CommandLine;

import java.io.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

@CommandLine.Command(
        description = "Push PDK jar file into Tapdata",
        subcommands = MainCli.class
)
public class TDDCli extends CommonCli {
    private static final String TAG = TDDCli.class.getSimpleName();
    @CommandLine.Parameters(paramLabel = "FILE", description = "One ore more pdk jar files")
    File file;

    @CommandLine.Option(names = {"-i", "--installProjects"}, required = false, description = "Specify the projects which need mvn install first.")
    private List<String> installProjects;
    @CommandLine.Option(names = {"-m", "--mavenHome"}, required = false, description = "Specify the maven home")
    private String mavenHome;
    @CommandLine.Option(names = {"-t", "--testCase"}, required = false, description = "Specify the test class simple name to test")
    private String[] testClass;
    @CommandLine.Option(names = {"-c", "--testConfig"}, required = true, description = "Specify the test json configuration file")
    private String testConfig;
    @CommandLine.Option(names = {"-v", "--verbose"}, required = false, description = "Enable debug log")
    private boolean verbose = false;
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "TapData cli help")
    private boolean helpRequested = false;
    @CommandLine.Option(names = {"-l", "--lang"}, usageHelp = false, description = "TapData cli lang，values zh_CN/zh_TW/en,default is en")
    private String lan = "en";
    @CommandLine.Option(names = {"-p", "--path"}, usageHelp = false, description = "TapData cli path,need test package ,path split as .")
    private String packagePath = "io.tapdata.pdk.tdd.tests";//WriteRecordTest.class.getPackage().getName();
    @CommandLine.Option(names = {"-log", "--logPath"}, usageHelp = false, description = "TapData cli log,need test to log test result ,path to log ,default ./tapdata-pdk-cli/tss-logs/")
    private String logPath = TapSummary.basePath("tdd-logs");

    /**
     * 默认true，或设置TDD_AUTO_EXIT=1，CommonUtils.setProperty("TDD_AUTO_EXIT","1")
     * 对应TDD_AUTO_EXIT ，是否执行完一个数据源的所有用例就自动推出程序，TDDFactory 中设置TDD_AUTO_EXIT = 0 （CommonUtils.setProperty("TDD_AUTO_EXIT","0")），表示手动退出，
     */
    private boolean autoExit = true;

    public static final String LEVEL_BEGINNER = "beginner";
    public static final String LEVEL_INTERMEDIATE = "intermediate";
    public static final String LEVEL_EXPERT = "expert";

    private List<TapSummary> testResultSummaries = new ArrayList<>();
    private SummaryGeneratingListener listener = new SummaryGeneratingListener();

    public void runOne(String testClass, TapSummary testResultSummary) {
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(selectClass("io.tapdata.pdk.tdd.tests." + testClass))
                .build();
        runTests(request, testResultSummary);
    }

    public void runLevel(List<DiscoverySelector> selectors, TapSummary testResultSummary) {
        testResultSummary.setLanType(new Locale(lan)).showCapabilities(nodeInfo());
        System.setProperty("tdd_running_is","1");
        for (DiscoverySelector selector : selectors) {
            LauncherDiscoveryRequestBuilder request = LauncherDiscoveryRequestBuilder.request();
            LauncherDiscoveryRequest build = request.selectors(selector).build();
            if (runTests(build, testResultSummary).hasBlocked()) {
                break;
            }
        }
        testResultSummary.endingShow(testResultSummary, file.getName());
        testResultSummary.asFileV2(file.getName());

        TapSummary.hasPass = "SUCCEED";
        TapSummary.capabilitiesResult = new HashMap<>();
        if (this.autoExit) System.exit(0);
    }

    private ChokeTag runTests(LauncherDiscoveryRequest request, TapSummary testResultSummary) {
        Launcher launcher = LauncherFactory.create();
        //TestPlan testPlan = launcher.discover(request);
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        String pdkId = CommonUtils.getProperty("pdk_test_pdk_id", null);
        TestExecutionSummary summary = listener.getSummary();
        testResultSummary.summary(summary);
        return testResultSummary.setLanType(new Locale(lan)).showTestResult(testResultSummary);
    }

    public Integer execute() {
        TapLogger.enable(false);
        TapSummary.create().showLogo();
        CommonUtils.setProperty("refresh_local_jars", "true");
        if (verbose)
            CommonUtils.setProperty("tap_verbose", "true");
        if (null == lan || (!"zh_CN".equals(lan) && !"zh_TW".equals(lan) && !"en".equals(lan))) {
            TapLogger.fatal(TAG, "can not test file {}, TapData cli lang values only zh_CN/zh_TW/en.", file);
            lan = "en";
        }
        lan = "zh_CN";
        if (null == logPath || "".equals(logPath)) {
            logPath = "/tdd-logs/";
        }
        String tddAutoExit = CommonUtils.getProperty("TDD_AUTO_EXIT");
        this.autoExit = null == tddAutoExit || "1".equals(tddAutoExit);

        CommonUtils.setProperty("tap_lang", lan);
        CommonUtils.setProperty("tap_log_path", logPath);
        try {
            testPDKJar(file, testConfig);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            TapLogger.fatal(TAG, "Run test against file {} failed, {}", file, throwable.getMessage());
        }
        return 0;
    }

    private void testPDKJar(File file, String testConfig) throws Throwable {
        String jarFile = null;
        if (file.isFile()) {
            jarFile = file.getAbsolutePath();
        } else if (file.isDirectory()) {
            if (!file.getAbsolutePath().contains("connectors")) {
                throw new IllegalArgumentException("Connector project is under connectors directory, are you passing the correct connector project directory? " + file.getAbsolutePath());
            }
            if (installProjects != null) {
                System.setProperty("maven.home", getMavenHome(mavenHome));
                for (String installProject : installProjects) {
                    String pomFile = installProject;
                    if (!pomFile.endsWith("pom.xml")) {
                        pomFile = pomFile + File.separator + "pom.xml";
                    }
//                    int state = mavenCli.doMain(new String[]{"install", "-f", pomFile}, "./", System.out, System.out);

                    InvocationRequest request = new DefaultInvocationRequest();
                    request.setPomFile(new File(pomFile));
                    request.setGoals(Collections.singletonList("install"));

                    Invoker invoker = new DefaultInvoker();
                    InvocationResult result = invoker.execute(request);

                    if (result.getExitCode() != 0) {
                        if (result.getExecutionException() != null)
                            System.out.println(result.getExecutionException().getMessage());
                        System.out.println("------------- Dependency project " + pomFile + " installed Failed --------------");
                        if (this.autoExit) System.exit(0);
                        return;
                    } else {
                        System.out.println("------------- Dependency project " + pomFile + " installed successfully -------------");
                    }
                }
            }

            System.setProperty("maven.home", getMavenHome(this.mavenHome));

            System.setProperty("maven.multiModuleProjectDirectory", file.getAbsolutePath());

            System.out.println(file.getName() + " is packaging...");

            String pomFile = file.getAbsolutePath();
            if (!pomFile.endsWith("pom.xml")) {
                pomFile = pomFile + File.separator + "pom.xml";
            }
            InvocationRequest request = new DefaultInvocationRequest();
            request.setPomFile(new File(pomFile));
            request.setGoals(Arrays.asList("clean", "install", "-DskipTests", "-P", "not_encrypt", "-U"));

            Invoker invoker = new DefaultInvoker();
            InvocationResult result = invoker.execute(request);

            if (result.getExitCode() != 0) {
                if (result.getExecutionException() != null)
                    System.out.println(result.getExecutionException().getMessage());
                System.out.println("------------- Maven package Failed --------------");
                if (this.autoExit) System.exit(0);
                return;
            } else {
                MavenXpp3Reader reader = new MavenXpp3Reader();
                Model model = reader.read(new FileReader(FilenameUtils.concat(file.getAbsolutePath(), "pom.xml")));
//                System.out.println("file " + file.getAbsolutePath());
//                jarFile = FilenameUtils.concat("./", "./connectors/dist/" + model.getArtifactId() + "-v" + model.getVersion() + ".jar");
                jarFile = CommonUtils.getProperty("pdk_external_jar_path", "../connectors/dist") + "/" + model.getArtifactId() + "-v" + model.getVersion() + ".jar";
                System.out.println("------------- Maven package successfully -------------");
                System.out.println("Connector jar is " + jarFile);
//                System.setProperty("maven.multiModuleProjectDirectory", ".");
                Thread.currentThread().setContextClassLoader(TDDCli.class.getClassLoader());
            }

//            MavenCli mavenCli = new MavenCli();
//            int state = mavenCli.doMain(new String[]{"clean", "install", "-DskipTests", "-P", "not_encrypt", "-U"}, file.getAbsolutePath(), System.out, System.out);
//            if (0 == state){
//                MavenXpp3Reader reader = new MavenXpp3Reader();
//                Model model = reader.read(new FileReader(FilenameUtils.concat(file.getAbsolutePath(), "pom.xml")));
//                jarFile = FilenameUtils.concat("./", "./dist/" + model.getArtifactId() + "-v" + model.getVersion() + ".jar");
//                System.out.println("------------- Maven package successfully -------------");
//                System.out.println("Connector jar is " + jarFile);
////                System.setProperty("maven.multiModuleProjectDirectory", ".");
//                Thread.currentThread().setContextClassLoader(TDDCli.class.getClassLoader());
//            } else {
//                System.out.println("");
//                System.out.println("------------- Maven package Failed --------------");
//                System.exit(0);
//            }
        } else {
            throw new IllegalArgumentException("File " + file.getAbsolutePath() + " is not exist");
        }

        CommonUtils.setProperty("pdk_test_jar_file", jarFile);
        CommonUtils.setProperty("pdk_test_config_file", testConfig);

        PDKTestBase testBase = new PDKTestBase();
        TapConnector testConnector = testBase.getTestConnector();
        testBase.setup();
        DataMap testOptions = testBase.getTestOptions();
        testBase.tearDown();

        String pdkId = null;
        if (testOptions != null) {
            pdkId = (String) testOptions.get("pdkId");
        }

        Collection<TapNodeInfo> tapNodeInfoCollection = testConnector.getTapNodeClassFactory().getConnectorTapNodeInfos();
        for (TapNodeInfo tapNodeInfo : tapNodeInfoCollection) {
            if (pdkId != null) {
                if (tapNodeInfo.getTapNodeSpecification().getId().equals(pdkId)) {
                    runLevelWithNodeInfo(tapNodeInfo);
                    break;
                }
            } else {
//                PDKLogger.enable(true);
                runLevelWithNodeInfo(tapNodeInfo);
            }
        }
        TapSummary.create(testResultSummaries).showTestResultAll(nodeInfo(), jarFile);
        if (this.autoExit) System.exit(0);
    }

    private void runLevelWithNodeInfo(TapNodeInfo tapNodeInfo) throws Throwable {
        CommonUtils.setProperty("pdk_test_pdk_id", tapNodeInfo.getTapNodeSpecification().getId());
        TapSummary testResultSummary = TapSummary.create();
        testResultSummary.tapNodeInfo(tapNodeInfo);
        testResultSummaries.add(testResultSummary);
        runLevel(generateTestTargets(tapNodeInfo, testResultSummary), testResultSummary);
    }

    private List<DiscoverySelector> generateTestTargets(TapNodeInfo tapNodeInfo, TapSummary testResultSummary) throws Throwable {
        io.tapdata.pdk.apis.TapConnector connector = (io.tapdata.pdk.apis.TapConnector) tapNodeInfo.getNodeClass().getConstructor().newInstance();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
        connector.registerCapabilities(connectorFunctions, codecRegistry);


        //builder.append("\n-------------PDK connector idAndGroupAndVersion " + tapNodeInfo.getTapNodeSpecification().idAndGroup() + "-------------").append("\n");
        //builder.append("             Node class " + tapNodeInfo.getNodeClass() + " run ");
        List<DiscoverySelector> selectors = new ArrayList<>();
        if (testClass != null) {
            for (String clazz : testClass) {
                try {
                    Class<? extends PDKTestBase> theClass = (Class<? extends PDKTestBase>) Class.forName(clazz);
                    selectorsAddClass(selectors, theClass, testResultSummary);
                } catch (Exception ignored) {
                }
            }
        } else {
            List<Class<? extends PDKTestBase>> tests = this.allTest();//
            selectorsAddClass(selectors, BasicTest.class, testResultSummary);
            List<Class<? extends PDKTestBase>> supportTest = new ArrayList<>();
            for (int i = 0; i < tests.size(); i++) {
                Class<? extends PDKTestBase> testClass = tests.get(i);
                boolean allFound = true;
                try {
                    List<SupportFunction> functions = (List<SupportFunction>) ReflectionUtil.invokeStaticMethod(testClass.getName(), "testFunctions");
                    for (SupportFunction supportFunction : functions) {
                        try {
                            if (!PDKTestBase.isSupportFunction(supportFunction, connectorFunctions)) {
                                allFound = false;
                                testResultSummary.doNotSupportFunTest().put(testClass, LangUtil.format(supportFunction.getErrorMessage()));
                            }
                        } catch (NoSuchMethodException e) {
                            allFound = false;
                            testResultSummary.doNotSupportFunTest().put(testClass, LangUtil.format(supportFunction.getErrorMessage()));
                        }
                        if (!allFound) {
                            Set<Class<? extends PDKTestBase>> classes = subTest(testClass);
                            if (null != classes && !classes.isEmpty()) {
                                tests.addAll(tests.size(), classes);
                            }
                            break;
                        }
                    }
                } catch (Exception ignored) {
                }
                if (allFound) {
                    supportTest.add(testClass);
                }
            }

            supportTest.stream().sorted((cla1, cla2) -> {
                Annotation annotation1 = cla1.getAnnotation(TapGo.class);
                Annotation annotation2 = cla2.getAnnotation(TapGo.class);
                return ((TapGo) annotation1).sort() > ((TapGo) annotation2).sort() ? 0 : -1;
            }).forEach(testClass -> {
                        //计算需要执行的用例数
                        SummaryData summaryData = testResultSummary.summaryData();
                        summaryData.needAny(this.caseNum(testClass));
                        this.selectorsAddClass(selectors, testClass, testResultSummary);
                    }
            );
            //if(connectorFunctions.getWriteRecordFunction() != null && connectorFunctions.getCreateTableFunction() == null) {
            //    selectorsAddClass(selectors, DMLTest.class, testResultSummary);
            //}
            //
            //if(connectorFunctions.getCreateTableFunction() != null && connectorFunctions.getDropTableFunction() != null) {
            //    selectorsAddClass(selectors, CreateTableTest.class, testResultSummary);
            //}
        }
        //builder.append(selectors.size() + " test classes").append("\n");
        //for(DiscoverySelector selector : selectors) {
        //    builder.append("             \t" + selector.toString()).append("\n");
        //}
        //builder.append("-------------PDK connector idAndGroupAndVersion " + tapNodeInfo.getTapNodeSpecification().idAndGroup() + "-------------").append("\n");
        //PDKLogger.info(TAG, builder.toString());

        return selectors;
    }

    private void selectorsAddClass(List<DiscoverySelector> selectors, Class<?> theClass, TapSummary testResultSummary) {
        selectors.add(DiscoverySelectors.selectClass(theClass));
        testResultSummary.testClasses().add(theClass);
    }

    private TapNodeInfo nodeInfo() {
        TapConnector tapConnector = new PDKTestBase().testConnector();
        Collection<TapNodeInfo> tapNodeInfoCollection = tapConnector.getTapNodeClassFactory().getConnectorTapNodeInfos();
        if (tapNodeInfoCollection.isEmpty())
            throw new CoreException(PDKRunnerErrorCodes.TDD_TAPNODEINFO_NOT_FOUND, "No connector or processor is found in jar ");
        String pdkId = CommonUtils.getProperty("pdk_test_pdk_id", null);
        if (pdkId == null)
            fail("Test pdkId is not specified");
        for (TapNodeInfo nodeInfo : tapNodeInfoCollection) {
            if (nodeInfo.getTapNodeSpecification().getId().equals(pdkId)) {
                return nodeInfo;
            }
        }
        return null;
    }

    private Set<Class<? extends PDKTestBase>> subTest(Class<? extends PDKTestBase> testClass) {
        TapGo tapGo = testClass.getAnnotation(TapGo.class);
        Class<? extends PDKTestBase>[] subTest = tapGo.subTest();
        return Arrays.asList(subTest).stream().filter(cls -> {
            try {
                TapGo tg = cls.getAnnotation(TapGo.class);
                boolean goTest = tg.goTest();
                boolean isSub = tg.isSub();
                return (PDKTestBase.class.isAssignableFrom(cls)) && goTest && isSub;
            } catch (Exception e) {
                return false;
            }
        }).collect(Collectors.toSet());
    }

    private List<Class<? extends PDKTestBase>> allTest() {
        Reflections reflections = new Reflections(packagePath);
        //返回带有指定注解的所有类对象
        Set<Class<?>> typesAnnotatedWith = reflections.getTypesAnnotatedWith(TapGo.class);
        boolean isDebugMode = "true".equals(System.getProperty("is_debug_mode", "false"));
        if (isDebugMode){
            System.out.println("It's debug mode for running test, maybe ignore some case which Annotation of TapGo's debug value is false, note that please.");
        }
        return typesAnnotatedWith.stream().filter(cls -> {
            try {
                TapGo tapGo = cls.getAnnotation(TapGo.class);
                boolean goTest = tapGo.goTest();
                boolean isSub = tapGo.isSub();
                boolean debug = !isDebugMode || tapGo.debug();
                boolean ignored = tapGo.ignore();
                return (PDKTestBase.class.isAssignableFrom(cls)) && !ignored && goTest && !isSub && debug ;
            } catch (Exception e) {
                return false;
            }
        }).sorted((cla1, cla2) -> {
            Annotation annotation1 = cla1.getAnnotation(TapGo.class);
            Annotation annotation2 = cla2.getAnnotation(TapGo.class);
            return ((TapGo) annotation1).sort() > ((TapGo) annotation2).sort() ? 0 : -1;
        }).map(cla -> {
            try {
                return ((PDKTestBase) cla.newInstance()).getClass();
            } catch (Exception e) {
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private List<Class<? extends PDKTestBase>> testClass() {
        if (null == this.testClass || this.testClass.length <= 0) {
            return allTest();
        }
        List<Class<? extends PDKTestBase>> test = new ArrayList<>();
        for (String aClass : this.testClass) {
            if (null != aClass) {
                try {
                    Class<? extends PDKTestBase> cls = (Class<? extends PDKTestBase>) Class.forName(aClass);
                    test.add(cls);
                } catch (Exception ignored) {
                }
            }
        }
        return test;
    }

    private int caseNum(Class<? extends PDKTestBase> aClass) {
        AtomicInteger caseNum = new AtomicInteger();
        HashSet<Method> methods = new HashSet<>(Arrays.asList(aClass.getDeclaredMethods()));
        for (Method method : methods) {
            Optional.ofNullable(method).ifPresent(m -> {
                Test testAnn = method.getAnnotation(Test.class);
                if (null != testAnn) {
                    caseNum.getAndIncrement();
                }
            });
        }
        return caseNum.get();
    }
}
