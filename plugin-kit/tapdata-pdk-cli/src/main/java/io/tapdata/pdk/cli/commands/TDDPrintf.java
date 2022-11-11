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
import io.tapdata.pdk.tdd.tests.v2.CapabilitiesExecutionMsg;
import io.tapdata.pdk.tdd.tests.v2.History;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TDDPrintf {
    List<TDDCli.TapSummary> summarys;

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

    public void showError(TDDCli.TapSummary summary,String fileName){
        TestExecutionSummary executionSummary = summary.summary;
        Map<Class, CapabilitiesExecutionMsg> result = summary.capabilitiesResult;
        if(executionSummary.getTestsFailedCount() > 0) {
            StringBuilder builder = new StringBuilder();
            builder.append("*****************************************************TDD Results*****************************************************\n")
                    .append("-------------PDK id '")
                    .append(summary.tapNodeInfo.getTapNodeSpecification().getId())
                    .append("' class '")
                    .append(summary.tapNodeInfo.getNodeClass().getName())
                    .append("'-------------\n");

            if (null != summary.doNotSupportFunTest && !summary.doNotSupportFunTest.isEmpty()){
                summary.doNotSupportFunTest.forEach(((aClass, s) -> {
                    builder.append(aClass.getSimpleName()).append("=======>Error to test\n")
                            .append("\t\t").append(s).append("\n");
                }));
            }

            builder.append("\n");

            result.forEach((cla,res)->{
                builder.append(cla.getSimpleName()).append("=======>")
                        .append(res.executionResult() == CapabilitiesExecutionMsg.ERROR?"Test error":"Test succeed")
                        .append("(");
                List<History> history = res.history();
                Map<String, List<History>> collect = history.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(History::tag));
                if (null == collect || collect.isEmpty()) return;
                List<History> warnHistory = collect.get(History.WARN);
                List<History> errorHistory = collect.get(History.ERROR);
                int warnSize = null==warnHistory?0:warnHistory.size();
                int errorSize = null==errorHistory?0:errorHistory.size();
                builder.append("succeed: ").append(res.executionTimes()-warnSize-errorSize)
                        .append(",warn: ").append(warnSize)
                        .append(",error: ").append(errorSize)
                        .append(")");

                if (res.executionResult() != CapabilitiesExecutionMsg.ERROR && null != warnHistory && !warnHistory.isEmpty()){
                    builder.append(", but have ")
                            .append(warnHistory.size())
                            .append(" warn.");
                }
                builder.append(".\n");
                AtomicInteger index = new AtomicInteger(1);
                collect.forEach((tag,his)->{
                    builder.append("\t\t").append(index.getAndIncrement()).append(") Test of ").append(tag).append(":\n");
                    for (int i = 0; i < his.size(); i++) {
                        builder.append("\t\t\t[")
                                .append(
                                        History.SUCCEED.equals(tag)? "✓":
                                        (History.ERROR.equals(tag)? "✗":
                                        (History.WARN.equals(tag)? "！":"？")))
                                .append("].").append(his.get(i).message()).append("\n");
                    }
                });
            });

            builder.append("\n\n(╳) Oops, PDK ")
                    .append(fileName)
                    .append(" didn't pass all tests, please resolve above issue(s) and try again.\n\n");

            System.out.print(builder.toString());
            System.exit(0);
        }
    }

    public void tddShow(){
        StringBuilder builder = new StringBuilder("TDD Result==========>");
        for(TDDCli.TapSummary testSummary : summarys) {


        }
        System.out.println(builder.toString());
    }

    public void showCapabilities(TapNodeInfo nodeInfo){
        String dagId = UUID.randomUUID().toString();
        if (null == nodeInfo) return;
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

        StringBuilder show = new StringBuilder(tapNodeSpecification.getName());
        show.append(" support capabilities========>\n");
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
        show.append("=====>total Capabilities of ").append(tapNodeSpecification.getName()).append(" is ").append(index).append(".\n\n");
        System.out.print(show.toString());
    }
}
