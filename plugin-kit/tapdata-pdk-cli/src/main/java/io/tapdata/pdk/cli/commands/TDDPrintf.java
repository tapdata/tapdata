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

import java.lang.reflect.Field;
import java.util.List;
import java.util.UUID;

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
