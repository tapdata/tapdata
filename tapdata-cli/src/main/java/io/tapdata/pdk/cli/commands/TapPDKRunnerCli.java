package io.tapdata.pdk.cli.commands;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.ReflectionUtil;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.cli.CommonCli;
import io.tapdata.pdk.core.connector.TapConnector;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.run.base.RunClassMap;
import io.tapdata.pdk.run.base.RunnerSummary;
import io.tapdata.pdk.run.support.BatchReadRun;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.basic.BasicTest;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import org.apache.commons.io.FilenameUtils;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.apache.maven.shared.invoker.*;
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

import java.io.File;
import java.io.FileReader;
import java.lang.annotation.Annotation;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

/**
 * https://picocli.info/
 *
 * @author Gavin
 */
@CommandLine.Command(
        description = "Debug/Run javaScript code.",
        subcommands = MainCli.class
)
public class TapPDKRunnerCli extends CommonCli {
    private static final String TAG = TDDCli.class.getSimpleName();
    @CommandLine.Parameters(paramLabel = "FILE", description = "One ore more pdk jar files")
    private File file;

    @CommandLine.Option(names = {"-i", "--installProjects"}, description = "Specify the projects which need mvn install first.")
    private List<String> installProjects;
    @CommandLine.Option(names = {"-m", "--mavenHome"}, description = "Specify the maven home")
    private String mavenHome;
    @CommandLine.Option(names = {"-r", "--runCase"}, description = "Specify the run/debug class simple name to run")
    private String[] runClass;
    @CommandLine.Option(names = {"-c", "--runConfig"}, description = "Specify the run/debug json configuration file")
    private String runConfig;
    @CommandLine.Option(names = {"-v", "--verbose"}, description = "Enable run/debug log")
    private boolean verbose = false;
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "TapData cli help")
    private boolean helpRequested = false;
    @CommandLine.Option(names = {"-l", "--lang"}, description = "TapData cli lang，values zh_CN/zh_TW/en,default is en")
    private String lan = "en";
    @CommandLine.Option(names = {"-p", "--path"}, description = "TapData cli path,need run package ,path split as .")
    private String packagePath = BatchReadRun.class.getPackage().getName();
    @CommandLine.Option(names = {"-log", "--logPath"}, usageHelp = false, description = "TapData cli log,need run/debug to log run result ,path to log ,default ./tapdata-pdk-cli/tss-logs/")
    private String logPath ;//= RunnerSummary.basePath("runner-logs");

    public TapPDKRunnerCli(){
    }
    /**
     * 默认true，或设置TDD_AUTO_EXIT=1，CommonUtils.setProperty("TDD_AUTO_EXIT","1")
     * 对应TDD_AUTO_EXIT ，是否执行完一个数据源的所有用例就自动推出程序，TDDFactory 中设置TDD_AUTO_EXIT = 0 （CommonUtils.setProperty("TDD_AUTO_EXIT","0")），表示手动退出，
     */
    private boolean autoExit = true;

    public static final String LEVEL_BEGINNER = "beginner";
    public static final String LEVEL_INTERMEDIATE = "intermediate";
    public static final String LEVEL_EXPERT = "expert";

    private List<RunnerSummary> runResultSummaries = new ArrayList<>();
    private SummaryGeneratingListener listener = new SummaryGeneratingListener();

    public void runOne(String runClass, RunnerSummary runResultSummary) {
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(selectClass("io.tapdata.pdk.run.support." + runClass))
                .build();
        this.run(request, runResultSummary);
    }

    public void runLevel(List<DiscoverySelector> selectors, RunnerSummary runResultSummary) {
        runResultSummary.setLanType(new Locale(this.lan)).showCapabilities(nodeInfo());
        runResultSummary.showSkipCase();
        for (DiscoverySelector selector : selectors) {
            LauncherDiscoveryRequestBuilder request = LauncherDiscoveryRequestBuilder.request();
            LauncherDiscoveryRequest build = request.selectors(selector).build();
            this.run(build, runResultSummary);
        }
        runResultSummary.endingShow(runResultSummary, this.file.getName());
        runResultSummary.asFileV2(this.file.getName());

        RunnerSummary.hasPass = "SUCCEED";
        RunnerSummary.capabilitiesResult = new HashMap<>();
        if (this.autoExit) System.exit(0);
    }

    private void run(LauncherDiscoveryRequest request, RunnerSummary runResultSummary) {
        Launcher launcher = LauncherFactory.create();
        //TestPlan testPlan = launcher.discover(request);
        launcher.registerTestExecutionListeners(this.listener);
        launcher.execute(request);

        String pdkId = CommonUtils.getProperty("pdk_test_pdk_id", null);
        TestExecutionSummary summary = this.listener.getSummary();
        runResultSummary.summary(summary);
        runResultSummary.setLanType(new Locale(this.lan)).showTestResult(runResultSummary);
    }

    public Integer execute() {
        if ((this.runClass == null || this.runClass.length == 0) ){
            Scanner scanner = new Scanner(System.in);
            System.out.print(RunClassMap.allCaseTable(true) + RunnerSummary.format("logo.tip.after"));
            String commandLine = scanner.nextLine();
            this.runClass = commandLine.split(" ");
        }
        TapLogger.enable(false);
//        LoggerManager.changeLogLeave(LoggerManager.LogLeave.DENY);
        //RunnerSummary.create().showLogo();
        CommonUtils.setProperty("refresh_local_jars", "true");
        if (this.verbose)
            CommonUtils.setProperty("tap_verbose", "true");
        if (null == this.lan || (!"zh_CN".equals(this.lan) && !"zh_TW".equals(this.lan) && !"en".equals(this.lan))) {
            TapLogger.fatal(TAG, "can not test file {}, TapData cli lang values only zh_CN/zh_TW/en.", this.file);
            this.lan = "en";
        }
        this.lan = "zh_CN";
        if (null == this.logPath || "".equals(this.logPath)) {
            this.logPath = "";
        }
        String tddAutoExit = CommonUtils.getProperty("TDD_AUTO_EXIT");
        this.autoExit = null == tddAutoExit || "1".equals(tddAutoExit);

        CommonUtils.setProperty("tap_lang", this.lan);
        CommonUtils.setProperty("tap_log_path", this.logPath);
        try {
            this.runPDKJar(this.file, this.runConfig);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            TapLogger.fatal(TAG, "Run test against file {} failed, {}", this.file, throwable.getMessage());
        }
        return 0;
    }

    private void runPDKJar(File file, String runConfig) throws Throwable {
        String jarFile = null;
        if (file.isFile()) {
            jarFile = file.getAbsolutePath();
        } else if (file.isDirectory()) {
            if (!file.getAbsolutePath().contains("connectors")) {
                throw new IllegalArgumentException("Connector project is under connectors directory, are you passing the correct connector project directory? " + file.getAbsolutePath());
            }
            if (this.installProjects != null) {
                System.setProperty("maven.home", getMavenHome(this.mavenHome));
                for (String installProject : this.installProjects) {
                    String pomFile = installProject;
                    if (!pomFile.endsWith("pom.xml")) {
                        pomFile = pomFile + File.separator + "pom.xml";
                    }
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
                jarFile = CommonUtils.getProperty("pdk_external_jar_path", "../connectors/dist") + "/" + model.getArtifactId() + "-v" + model.getVersion() + ".jar";
                System.out.println("------------- Maven package successfully -------------");
                System.out.println("Connector jar is " + jarFile);
                Thread.currentThread().setContextClassLoader(TDDCli.class.getClassLoader());
            }
        } else {
            throw new IllegalArgumentException("File " + file.getAbsolutePath() + " is not exist");
        }

        CommonUtils.setProperty("pdk_test_jar_file", jarFile);
        CommonUtils.setProperty("pdk_test_config_file", runConfig);

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
                runLevelWithNodeInfo(tapNodeInfo);
            }
        }
        RunnerSummary.create(this.runResultSummaries).showTestResultAll(nodeInfo(), jarFile);
        if (this.autoExit) System.exit(0);
    }

    private void runLevelWithNodeInfo(TapNodeInfo tapNodeInfo) throws Throwable {
        CommonUtils.setProperty("pdk_test_pdk_id", tapNodeInfo.getTapNodeSpecification().getId());
        RunnerSummary runResultSummary = RunnerSummary.create();
        runResultSummary.tapNodeInfo(tapNodeInfo);
        this.runResultSummaries.add(runResultSummary);
        runLevel(generateTestTargets(tapNodeInfo, runResultSummary), runResultSummary);
    }

    private List<DiscoverySelector> generateTestTargets(TapNodeInfo tapNodeInfo, RunnerSummary runResultSummary) throws Throwable {
        io.tapdata.pdk.apis.TapConnector connector = (io.tapdata.pdk.apis.TapConnector) tapNodeInfo.getNodeClass().getConstructor().newInstance();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
        connector.registerCapabilities(connectorFunctions, codecRegistry);
        List<DiscoverySelector> selectors = new ArrayList<>();
        if (this.runClass != null) {
            for (String clazz : this.runClass) {
                try {
                    String classPath = RunClassMap.whichCase(clazz);
                    if (Objects.isNull(classPath)){
                        runResultSummary.skipCase(clazz);
                        continue;
                    }
                    Class<? extends PDKTestBase> theClass = (Class<? extends PDKTestBase>) Class.forName(classPath);
                    boolean allFound = true;
                    try {
                        List<SupportFunction> functions = (List<SupportFunction>) ReflectionUtil.invokeStaticMethod(theClass.getName(), "testFunctions");
                        for (SupportFunction supportFunction : functions) {
                            try {
                                if (!PDKTestBase.isSupportFunction(supportFunction, connectorFunctions)) {
                                    allFound = false;
                                    runResultSummary.doNotSupportFunTest().put(theClass, RunnerSummary.format(supportFunction.getErrorMessage()));
                                }
                            } catch (NoSuchMethodException e) {
                                allFound = false;
                                runResultSummary.doNotSupportFunTest().put(theClass, RunnerSummary.format(supportFunction.getErrorMessage()));
                            }
                            if (!allFound) {
                                break;
                            }
                        }
                    } catch (Exception ignored) {
                        continue;
                    }
                    if (allFound) selectorsAddClass(selectors, theClass, runResultSummary);
                } catch (Exception ignored) {
                    runResultSummary.skipCase(clazz);
                }
            }
        } else {
            List<Class<? extends PDKTestBase>> tests = this.allRunCase();//
            selectorsAddClass(selectors, BasicTest.class, runResultSummary);
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
                                runResultSummary.doNotSupportFunTest().put(testClass, RunnerSummary.format(supportFunction.getErrorMessage()));
                            }
                        } catch (NoSuchMethodException e) {
                            allFound = false;
                            runResultSummary.doNotSupportFunTest().put(testClass, RunnerSummary.format(supportFunction.getErrorMessage()));
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
            }).forEach(testClass -> selectorsAddClass(selectors, testClass, runResultSummary));
        }
        return selectors;
    }

    private void selectorsAddClass(List<DiscoverySelector> selectors, Class<?> theClass, RunnerSummary runResultSummary) {
        selectors.add(DiscoverySelectors.selectClass(theClass));
        runResultSummary.testClasses().add(theClass);
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

    private List<Class<? extends PDKTestBase>> allRunCase() {
        Reflections reflections = new Reflections(this.packagePath);
        //返回带有指定注解的所有类对象
        Set<Class<?>> typesAnnotatedWith = reflections.getTypesAnnotatedWith(TapGo.class);
        return typesAnnotatedWith.stream().filter(cls -> {
            try {
                TapGo tapGo = cls.getAnnotation(TapGo.class);
                boolean goTest = tapGo.goTest();
                boolean isSub = tapGo.isSub();
                return (PDKTestBase.class.isAssignableFrom(cls)) && goTest && !isSub;
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
}
