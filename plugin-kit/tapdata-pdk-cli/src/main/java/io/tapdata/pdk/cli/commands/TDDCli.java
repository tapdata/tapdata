package io.tapdata.pdk.cli.commands;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.cli.CommonCli;
import io.tapdata.pdk.core.connector.TapConnector;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.ReflectionUtil;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.basic.BasicTest;
import io.tapdata.pdk.tdd.tests.source.BatchReadTest;
import io.tapdata.pdk.tdd.tests.source.QueryByAdvanceFilterTest;
import io.tapdata.pdk.tdd.tests.source.StreamReadTest;
import io.tapdata.pdk.tdd.tests.target.DMLTest;
import io.tapdata.pdk.tdd.tests.target.CreateTableTest;
import org.apache.commons.io.FilenameUtils;
import org.apache.maven.cli.MavenCli;
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
import picocli.CommandLine;

import java.io.*;
import java.util.*;

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

@CommandLine.Command(
        description = "Push PDK jar file into Tapdata",
        subcommands = MainCli.class
)
public class TDDCli extends CommonCli {
    private static final String TAG = TDDCli.class.getSimpleName();
    @CommandLine.Parameters(paramLabel = "FILE", description = "One ore more pdk jar files")
    File file;

    @CommandLine.Option(names = { "-i", "--installProjects" }, required = false, description = "Specify the projects which need mvn install first.")
    private List<String> installProjects;
    @CommandLine.Option(names = { "-m", "--mavenHome" }, required = false, description = "Specify the maven home")
    private String mavenHome;
    @CommandLine.Option(names = { "-t", "--testCase" }, required = false, description = "Specify the test class simple name to test")
    private String[] testClass;
    @CommandLine.Option(names = { "-c", "--testConfig" }, required = true, description = "Specify the test json configuration file")
    private String testConfig;
    @CommandLine.Option(names = { "-v", "--verbose" }, required = false, description = "Enable debug log")
    private boolean verbose = false;
    @CommandLine.Option(names = { "-h", "--help" }, usageHelp = true, description = "TapData cli help")
    private boolean helpRequested = false;
    private SummaryGeneratingListener listener = new SummaryGeneratingListener();
    public void runOne(String testClass, TapSummary testResultSummary) {
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(selectClass("io.tapdata.pdk.tdd.tests." + testClass))
                .build();
        runTests(request, testResultSummary);
    }

    public void runLevel(List<DiscoverySelector> selectors, TapSummary testResultSummary) {
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
//                .selectors(selectPackage("io.tapdata.pdk.tdd.tests.basic"),
//                        selectPackage("io.tapdata.pdk.tdd.tests.source." + level),
//                        selectPackage("io.tapdata.pdk.tdd.tests.target." + level))
                .selectors(selectors)
//                .filters(includeClassNamePatterns(".*Test"))
                .build();
        runTests(request, testResultSummary);
    }

    public static final String LEVEL_BEGINNER = "beginner";
    public static final String LEVEL_INTERMEDIATE = "intermediate";
    public static final String LEVEL_EXPERT = "expert";

    private List<TapSummary> testResultSummaries = new ArrayList<>();

    public static class TapSummary {
        public TapSummary() {}
        TapNodeInfo tapNodeInfo;
        TestExecutionSummary summary;
        List<Class<?>> testClasses = new ArrayList<>();
    }

    private void runTests(LauncherDiscoveryRequest request, TapSummary testResultSummary) {
        Launcher launcher = LauncherFactory.create();
//        TestPlan testPlan = launcher.discover(request);
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        String pdkId = CommonUtils.getProperty("pdk_test_pdk_id", null);

        TestExecutionSummary summary = listener.getSummary();
        testResultSummary.summary = summary;
        if(summary.getTestsFailedCount() > 0) {
            System.out.println("*****************************************************TDD Results*****************************************************");
            System.out.println("-------------PDK id '" + testResultSummary.tapNodeInfo.getTapNodeSpecification().getId() + "' class '" + testResultSummary.tapNodeInfo.getNodeClass().getName() + "'-------------");
//            summary.printTo(new PrintWriter(System.out));
            System.out.println("");
            StringBuilder builder = new StringBuilder();
            outputTestResult(testResultSummary, builder);
            System.out.print(builder.toString());

            summary.printFailuresTo(new PrintWriter(System.out));
            System.out.println("");
            System.out.println("-------------PDK id '" + testResultSummary.tapNodeInfo.getTapNodeSpecification().getId() + "' class '" + testResultSummary.tapNodeInfo.getNodeClass().getName() + "'-------------");
            System.out.println("*****************************************************TDD Results*****************************************************");
            System.out.println("Oops, PDK " + file.getName() + " didn't pass all tests, please resolve above issue(s) and try again.");
//            throw new CoreException(ErrorCodes.TDD_TEST_FAILED, "Terminated because test failed");
            System.exit(0);
        }
    }

    public Integer execute() {
        CommonUtils.setProperty("refresh_local_jars", "true");
        if(verbose)
            CommonUtils.setProperty("tap_verbose", "true");
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
        if(file.isFile()) {
            jarFile = file.getAbsolutePath();
        } else if(file.isDirectory()) {
            if(!file.getAbsolutePath().contains("connectors")) {
                throw new IllegalArgumentException("Connector project is under connectors directory, are you passing the correct connector project directory? " + file.getAbsolutePath());
            }



            if(installProjects != null) {
                System.setProperty("maven.home", getMavenHome(mavenHome));
                for(String installProject : installProjects) {
                    String pomFile = installProject;
                    if(!pomFile.endsWith("pom.xml")) {
                        pomFile = pomFile + File.separator + "pom.xml";
                    }
//                    int state = mavenCli.doMain(new String[]{"install", "-f", pomFile}, "./", System.out, System.out);

                    InvocationRequest request = new DefaultInvocationRequest();
                    request.setPomFile( new File( pomFile ) );
                    request.setGoals( Collections.singletonList( "install" ) );

                    Invoker invoker = new DefaultInvoker();
                    InvocationResult result = invoker.execute( request );

                    if ( result.getExitCode() != 0 )
                    {
                        if(result.getExecutionException() != null)
                            System.out.println(result.getExecutionException().getMessage());
                        System.out.println("------------- Dependency project " + pomFile + " installed Failed --------------");
                        System.exit(0);
                    } else {
                        System.out.println("------------- Dependency project " + pomFile + " installed successfully -------------");
                    }
                }
            }

            System.setProperty("maven.home", getMavenHome(this.mavenHome));

            System.setProperty("maven.multiModuleProjectDirectory", file.getAbsolutePath());
            System.out.println(file.getName() + " is packaging...");

            String pomFile = file.getAbsolutePath();
            if(!pomFile.endsWith("pom.xml")) {
                pomFile = pomFile + File.separator + "pom.xml";
            }
            InvocationRequest request = new DefaultInvocationRequest();
            request.setPomFile( new File( pomFile ) );
            request.setGoals(Arrays.asList( "clean", "install", "-DskipTests", "-P", "not_encrypt", "-U"));

            Invoker invoker = new DefaultInvoker();
            InvocationResult result = invoker.execute( request );

            if ( result.getExitCode() != 0 )
            {
                if(result.getExecutionException() != null)
                    System.out.println(result.getExecutionException().getMessage());
                System.out.println("------------- Maven package Failed --------------");
                System.exit(0);
            } else {
                MavenXpp3Reader reader = new MavenXpp3Reader();
                Model model = reader.read(new FileReader(FilenameUtils.concat(file.getAbsolutePath(), "pom.xml")));
                jarFile = FilenameUtils.concat("./", "./dist/" + model.getArtifactId() + "-v" + model.getVersion() + ".jar");
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
//        testBase.setup();
        TapConnector testConnector = testBase.getTestConnector();
        testBase.setup();

        DataMap testOptions = testBase.getTestOptions();

        testBase.tearDown();

        String pdkId = null;
        if(testOptions != null) {
            pdkId = (String) testOptions.get("pdkId");
        }

        Collection<TapNodeInfo> tapNodeInfoCollection = testConnector.getTapNodeClassFactory().getConnectorTapNodeInfos();
        for(TapNodeInfo tapNodeInfo : tapNodeInfoCollection) {
            if(pdkId != null) {
                if(tapNodeInfo.getTapNodeSpecification().getId().equals(pdkId)) {
                    runLevelWithNodeInfo(tapNodeInfo);
                    break;
                }
            } else {
//                PDKLogger.enable(true);
                runLevelWithNodeInfo(tapNodeInfo);
            }
        }
        System.out.println("*****************************************************TDD Results*****************************************************");
        for(TapSummary testSummary : testResultSummaries) {
            StringBuilder builder = new StringBuilder();
            builder.append("-------------PDK id '" + testSummary.tapNodeInfo.getTapNodeSpecification().getId() + "' class '" + testSummary.tapNodeInfo.getNodeClass().getName() + "'-------------").append("\n");
            builder.append("             Node class " + testSummary.tapNodeInfo.getNodeClass() + " run ");

            builder.append(testSummary.testClasses.size() + " test classes").append("\n");
            for(Class<?> testClass : testSummary.testClasses) {
                builder.append("             \t" + testClass.getName()).append("\n");
            }
            builder.append("\n");
            outputTestResult(testSummary, builder);

            builder.append("-------------PDK id '" + testSummary.tapNodeInfo.getTapNodeSpecification().getId() + "' class '" + testSummary.tapNodeInfo.getNodeClass().getName() + "'-------------").append("\n");
            System.out.print(builder.toString());
        }
        System.out.println("*****************************************************TDD Results*****************************************************");
        System.out.println("Congratulations! PDK " + jarFile + " has passed all tests!");
        System.exit(0);
    }

    private void outputTestResult(TapSummary testSummary, StringBuilder builder) {
        builder.append("             " + "Test run finished after " + (testSummary.summary.getTimeFinished() - testSummary.summary.getTimeStarted())).append("\n");
        builder.append("             \t" + testSummary.summary.getTestsFoundCount() + " test(s) found").append("\n");
        builder.append("             \t" + testSummary.summary.getTestsSkippedCount() + " test(s) skipped").append("\n");
        builder.append("             \t" + testSummary.summary.getTestsStartedCount() + " test(s) started").append("\n");
        builder.append("             \t" + testSummary.summary.getTestsSucceededCount() + " test(s) successful").append("\n");
        builder.append("             \t" + testSummary.summary.getTestsFailedCount() + " test(s) failed").append("\n");
    }

    private void runLevelWithNodeInfo(TapNodeInfo tapNodeInfo) throws Throwable {
        CommonUtils.setProperty("pdk_test_pdk_id", tapNodeInfo.getTapNodeSpecification().getId());
        TapSummary testResultSummary = new TapSummary();
        testResultSummary.tapNodeInfo = tapNodeInfo;
        testResultSummaries.add(testResultSummary);
        runLevel(generateTestTargets(tapNodeInfo, testResultSummary), testResultSummary);
    }

    private List<DiscoverySelector> generateTestTargets(TapNodeInfo tapNodeInfo, TapSummary testResultSummary) throws Throwable {
        io.tapdata.pdk.apis.TapConnector connector = (io.tapdata.pdk.apis.TapConnector) tapNodeInfo.getNodeClass().getConstructor().newInstance();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
        connector.registerCapabilities(connectorFunctions, codecRegistry);

        List<Class<? extends PDKTestBase>> tests = Arrays.asList(
                DMLTest.class,
                CreateTableTest.class,
                BatchReadTest.class,
                StreamReadTest.class,
                QueryByAdvanceFilterTest.class
        );

//        builder.append("\n-------------PDK connector idAndGroupAndVersion " + tapNodeInfo.getTapNodeSpecification().idAndGroup() + "-------------").append("\n");
//        builder.append("             Node class " + tapNodeInfo.getNodeClass() + " run ");
        List<DiscoverySelector> selectors = new ArrayList<>();
        if(testClass != null) {
            for(String clazz : testClass) {
                Class<?> theClass = Class.forName(clazz);
                selectorsAddClass(selectors, theClass, testResultSummary);
            }
        } else {
            selectorsAddClass(selectors, BasicTest.class, testResultSummary);

            for(Class<? extends PDKTestBase> testClass : tests) {
                boolean allFound = true;
                List<SupportFunction> functions = (List<SupportFunction>) ReflectionUtil.invokeStaticMethod(testClass.getName(), "testFunctions");
                for(SupportFunction supportFunction : functions) {
                    try {
                        if(!PDKTestBase.isSupportFunction(supportFunction, connectorFunctions)) {
                            allFound = false;
                            break;
                        }
                    } catch (NoSuchMethodException e) {
                        e.printStackTrace();
                        allFound = false;
                        break;
                    }
                }
                if(allFound) {
                    selectorsAddClass(selectors, testClass, testResultSummary);
                }
            }
//            if(connectorFunctions.getWriteRecordFunction() != null && connectorFunctions.getCreateTableFunction() == null) {
//                selectorsAddClass(selectors, DMLTest.class, testResultSummary);
//            }
//
//            if(connectorFunctions.getCreateTableFunction() != null && connectorFunctions.getDropTableFunction() != null) {
//                selectorsAddClass(selectors, CreateTableTest.class, testResultSummary);
//            }
        }
//        builder.append(selectors.size() + " test classes").append("\n");
//        for(DiscoverySelector selector : selectors) {
//            builder.append("             \t" + selector.toString()).append("\n");
//        }
//        builder.append("-------------PDK connector idAndGroupAndVersion " + tapNodeInfo.getTapNodeSpecification().idAndGroup() + "-------------").append("\n");
//        PDKLogger.info(TAG, builder.toString());
        return selectors;
    }

    private void selectorsAddClass(List<DiscoverySelector> selectors, Class<?> theClass, TapSummary testResultSummary) {
        selectors.add(DiscoverySelectors.selectClass(theClass));
        testResultSummary.testClasses.add(theClass);
    }


}
