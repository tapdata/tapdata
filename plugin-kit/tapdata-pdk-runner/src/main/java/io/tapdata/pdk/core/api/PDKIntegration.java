package io.tapdata.pdk.core.api;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVMapFactory;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.TapProcessor;
import io.tapdata.pdk.apis.context.ConfigContext;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.context.TapProcessorContext;
import io.tapdata.pdk.apis.entity.ConnectorCapabilities;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.ProcessorFunctions;
import io.tapdata.pdk.apis.functions.common.MemoryFetcherFunctionV2;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.pdk.core.dag.TapDAGNode;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.pdk.core.memory.MemoryManager;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.tapnode.TapNodeInstance;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

public class PDKIntegration {
    private static TapConnectorManager tapConnectorManager;

    private static MemoryManager memoryManager;
    private static final String TAG = PDKIntegration.class.getSimpleName();

    private PDKIntegration() {}

    public abstract static class ConnectionBuilder<T extends Node> {
        protected String associateId;
        protected DataMap connectionConfig;
        protected DataMap nodeConfig;
        protected String pdkId;
        protected String group;
        protected String version;
        protected Log log;

        public String verify() {
            if(associateId == null)
                return "missing associateId";
            if(pdkId == null)
                return "missing pdkId";
            if(group == null)
                return "missing group";
            if(version == null)
                return "missing version";
//            if(log == null)
//                return "missing log";
            return null;
        }
        public ConnectionBuilder<T> withLog(Log log) {
            this.log = log;
            return this;
        }
        /**
         * Each
         * @param associateId
         * @return
         */
        public ConnectionBuilder<T> withAssociateId(String associateId) {
            this.associateId = associateId;
            return this;
        }

        public ConnectionBuilder<T> withPdkId(String pdkId) {
            this.pdkId = pdkId;
            return this;
        }

        public ConnectionBuilder<T> withGroup(String group) {
            this.group = group;
            return this;
        }

        public ConnectionBuilder<T> withVersion(String version) {
            this.version = version;
            return this;
        }

        public ConnectionBuilder<T> withConnectionConfig(DataMap connectionConfig) {
            this.connectionConfig = connectionConfig;
            return this;
        }

        public ConnectionBuilder<T> withNodeConfig(DataMap nodeConfig) {
            this.nodeConfig = nodeConfig;
            return this;
        }

        public ConnectionBuilder<T> withTapDAGNode(TapDAGNode node) {
            this.pdkId = node.getPdkId();
            this.group = node.getGroup();
            this.version = node.getVersion();
            this.connectionConfig = node.getConnectionConfig();
            this.associateId = node.getId();
            return this;
        }

        protected void checkParams() {
            String result = verify();
            if(result != null)
                throw new CoreException(PDKRunnerErrorCodes.PDK_ILLEGAL_PARAMETER, "Illegal parameter, " + result);
        }

        public abstract T build();
    }

    public abstract static class ProcessorBuilder<T extends Node> {
        protected DataMap nodeConfig;
        protected String dagId;

        protected String associateId;
        protected DataMap connectionConfig;
        protected String pdkId;
        protected String group;
        protected String version;
        protected Log log;

        public String verify() {
            if(associateId == null)
                return "missing associateId";
            if(pdkId == null)
                return "missing pdkId";
            if(group == null)
                return "missing group";
            if(version == null)
                return "missing version";
            if(dagId == null)
                return "missing dagId";
            return null;
        }

        public ProcessorBuilder<T> withLog(Log log) {
            this.log = log;
            return this;
        }
        /**
         * Each
         * @param associateId
         * @return
         */
        public ProcessorBuilder<T> withAssociateId(String associateId) {
            this.associateId = associateId;
            return this;
        }

        public ProcessorBuilder<T> withPdkId(String pdkId) {
            this.pdkId = pdkId;
            return this;
        }

        public ProcessorBuilder<T> withGroup(String group) {
            this.group = group;
            return this;
        }

        public ProcessorBuilder<T> withVersion(String version) {
            this.version = version;
            return this;
        }

        public ProcessorBuilder<T> withConnectionConfig(DataMap connectionConfig) {
            this.connectionConfig = connectionConfig;
            return this;
        }

        public ProcessorBuilder<T> withTapDAGNode(TapDAGNode node) {
            this.pdkId = node.getPdkId();
            this.group = node.getGroup();
            this.version = node.getVersion();
            this.connectionConfig = node.getConnectionConfig();
            this.associateId = node.getId();
            this.nodeConfig = node.getNodeConfig();
            return this;
        }


        public ProcessorBuilder<T> withDagId(String dagId) {
            this.dagId = dagId;
            return this;
        }

        public ProcessorBuilder<T> withNodeConfig(DataMap nodeConfig) {
            this.nodeConfig = nodeConfig;
            return this;
        }

        protected void checkParams() {
            String result = verify();
            if(result != null)
                throw new CoreException(PDKRunnerErrorCodes.PDK_ILLEGAL_PARAMETER, "Illegal parameter, " + result);
        }

        public abstract T build();
    }

    public abstract static class ConnectorBuilder<T extends Node> {
        protected DataMap nodeConfig;
        protected String dagId;
        protected String associateId;
        protected DataMap connectionConfig;
        protected String pdkId;
        protected String group;
        protected String version;
        protected List<Map<String, Object>> tasks;
        protected String table;
        protected List<String> tables;
        protected ConfigContext configContext;
        protected KVReadOnlyMap<TapTable> tableMap;
        protected KVMap<Object> stateMap;
        protected KVMap<Object> globalStateMap;
        protected ConnectorCapabilities connectorCapabilities;
        protected Log log;

        public String verify() {
            if(associateId == null)
                return "missing associateId";
            if(pdkId == null)
                return "missing pdkId";
            if(group == null)
                return "missing group";
            if(version == null)
                return "missing version";
            if(dagId == null)
                return "missing dagId";
            /*if((tables == null || tables.isEmpty()) && table == null)
                return "missing tables or table";*/
            if(tableMap == null)
                return "missing tableMap";
            if(stateMap == null)
                return "missing stateMap";
            if(globalStateMap == null)
                return "missing globalStateMap";
//            if(log == null)
//                return "missing log";
            return null;
        }

        public ConnectorBuilder<T> withLog(Log log) {
            this.log = log;
            return this;
        }

        public ConnectorBuilder<T> withConnectorCapabilities(ConnectorCapabilities connectorCapabilities) {
            this.connectorCapabilities = connectorCapabilities;
            return this;
        }
        /**
         * Each
         * @param associateId
         * @return
         */
        public ConnectorBuilder<T> withAssociateId(String associateId) {
            this.associateId = associateId;
            return this;
        }

        public ConnectorBuilder<T> withConfigContext(ConfigContext configContext) {
            this.configContext = configContext;
            return this;
        }

        public ConnectorBuilder<T> withPdkId(String pdkId) {
            this.pdkId = pdkId;
            return this;
        }

        public ConnectorBuilder<T> withGroup(String group) {
            this.group = group;
            return this;
        }

        public ConnectorBuilder<T> withTableMap(KVReadOnlyMap<TapTable> tableMap) {
            this.tableMap = tableMap;
            return this;
        }

        public ConnectorBuilder<T> withStateMap(KVMap<Object> stateMap) {
            this.stateMap = stateMap;
            return this;
        }

        public ConnectorBuilder<T> withGlobalStateMap(KVMap<Object> globalStateMap) {
            this.globalStateMap = globalStateMap;
            return this;
        }

        public ConnectorBuilder<T> withVersion(String version) {
            this.version = version;
            return this;
        }

        public ConnectorBuilder<T> withConnectionConfig(DataMap connectionConfig) {
            this.connectionConfig = connectionConfig;
            return this;
        }

        public ConnectorBuilder<T> withTapDAGNode(TapDAGNode node) {
            this.pdkId = node.getPdkId();
            this.group = node.getGroup();
            this.version = node.getVersion();
            this.connectionConfig = node.getConnectionConfig();
            this.associateId = node.getId();
            this.nodeConfig = node.getNodeConfig();
            this.tasks = node.getTasks();
            this.table = node.getTable();
            this.tables = node.getTables();
            KVMapFactory mapFactory = InstanceFactory.instance(KVMapFactory.class);
            mapFactory.getCacheMap("tableMap_" + this.associateId, TapTable.class);
            this.tableMap = mapFactory.createKVReadOnlyMap("tableMap_" + this.associateId);
            this.stateMap = mapFactory.getPersistentMap("stateMap_" + this.associateId, Object.class);
            this.globalStateMap = mapFactory.getPersistentMap("stateMap", Object.class);
            return this;
        }

        public ConnectorBuilder<T> withTasks(List<Map<String, Object>> tasks) {
            this.tasks = tasks;
            return this;
        }

        public ConnectorBuilder<T> withTable(String table) {
            this.table = table;
            return this;
        }

        public ConnectorBuilder<T> withTables(List<String> tables) {
            this.tables = tables;
            return this;
        }

        public ConnectorBuilder<T> withDagId(String dagId) {
            this.dagId = dagId;
            return this;
        }

        public ConnectorBuilder<T> withNodeConfig(DataMap nodeConfig) {
            this.nodeConfig = nodeConfig;
            return this;
        }

        protected void checkParams() {
            String result = verify();
            if(result != null)
                throw new CoreException(PDKRunnerErrorCodes.PDK_ILLEGAL_PARAMETER, "Illegal parameter, " + result);
        }

        public abstract T build();
    }

    public static class ConnectionConnectorBuilder extends ConnectionBuilder<ConnectionNode> {
        public ConnectionNode build() {
            checkParams();
            TapNodeInstance nodeInstance = TapConnectorManager.getInstance().createConnectorInstance(associateId, pdkId, group, version);
            if(nodeInstance == null)
                throw new CoreException(PDKRunnerErrorCodes.PDK_PROCESSOR_NOTFOUND, MessageFormat.format("Source not found for pdkId {0} group {1} version {2} for associateId {3}", pdkId, group, version, associateId));
            ConnectionNode connectionNode = new ConnectionNode();
            connectionNode.init((TapConnector) nodeInstance.getTapNode());
            connectionNode.associateId = associateId;
            connectionNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            if(log == null)
                log = new TapLog();
            connectionNode.connectionContext = new TapConnectionContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), connectionConfig, nodeConfig, log);

            PDKInvocationMonitor.getInstance().invokePDKMethod(connectionNode, PDKMethod.REGISTER_CAPABILITIES,
                    connectionNode::registerCapabilities,
                    MessageFormat.format("call connection functions {0} associateId {1}", TapNodeSpecification.idAndGroup(pdkId, group, version), associateId), TAG);
            connectionNode.registerMemoryFetcher();
            return connectionNode;
        }
    }

    public static class ConnectorBuilderEx extends ConnectorBuilder<ConnectorNode> {
        public ConnectorNode build() {
            checkParams();
            TapNodeInstance nodeInstance = TapConnectorManager.getInstance().createConnectorInstance(associateId, pdkId, group, version);
            if(nodeInstance == null)
                throw new CoreException(PDKRunnerErrorCodes.PDK_CONNECTOR_NOTFOUND, MessageFormat.format("Source not found for pdkId {0} group {1} version {2} for associateId {3}", pdkId, group, version, associateId));
            ConnectorNode connectorNode = new ConnectorNode();
            connectorNode.init((TapConnector) nodeInstance.getTapNode());
            connectorNode.dagId = dagId;
            connectorNode.associateId = associateId;
            connectorNode.tasks = tasks;
            connectorNode.table = table;
            connectorNode.tables = tables;
            connectorNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            if(log == null)
                log = new TapLog();
            connectorNode.connectorContext = new TapConnectorContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), connectionConfig, nodeConfig, log);
            connectorNode.connectorContext.setConfigContext(configContext);
            if(connectorCapabilities == null)
                connectorCapabilities = new ConnectorCapabilities();
            connectorNode.connectorContext.setConnectorCapabilities(connectorCapabilities);
            connectorNode.connectorContext.setTableMap(tableMap);
            connectorNode.connectorContext.setStateMap(stateMap);
            connectorNode.connectorContext.setGlobalStateMap(globalStateMap);

            PDKInvocationMonitor.getInstance().invokePDKMethod(connectorNode, PDKMethod.REGISTER_CAPABILITIES,
                    connectorNode::registerCapabilities,
                    MessageFormat.format("call source functions {0} associateId {1}", TapNodeSpecification.idAndGroup(pdkId, group, version), associateId), TAG);
            connectorNode.registerMemoryFetcher();
            return connectorNode;
        }
    }

    public static class ProcessorConnectorBuilder extends ProcessorBuilder<ProcessorNode> {
        public ProcessorNode build() {
            checkParams();
            TapNodeInstance nodeInstance = TapConnectorManager.getInstance().createProcessorInstance(associateId, pdkId, group, version);
            if(nodeInstance == null)
                throw new CoreException(PDKRunnerErrorCodes.PDK_PROCESSOR_NOTFOUND, MessageFormat.format("Processor not found for pdkId {0} group {1} version {2} for associateId {3}", pdkId, group, version, associateId));
            ProcessorNode processorNode = new ProcessorNode();
            processorNode.dagId = dagId;
            processorNode.associateId = associateId;
            processorNode.processor = (TapProcessor) nodeInstance.getTapNode();
            processorNode.processorFunctions = new ProcessorFunctions();
            processorNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            if(log == null)
                log = new TapLog();
            processorNode.processorContext = new TapProcessorContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), connectionConfig, log);
            PDKInvocationMonitor.getInstance().invokePDKMethod(processorNode, PDKMethod.PROCESSOR_FUNCTIONS,
                    () -> processorNode.processorFunctions(processorNode.processorFunctions),
                    MessageFormat.format("call processor functions {0} associateId {1}", TapNodeSpecification.idAndGroup(pdkId, group, version), associateId), TAG);
            return processorNode;
        }
    }

    public static void init() {
        if(tapConnectorManager == null) {
            tapConnectorManager = TapConnectorManager.getInstance().start();
            memoryManager = MemoryManager.create();
            memoryManager.register(TapConnectorManager.class.getSimpleName(), tapConnectorManager);
            memoryManager.register(PDKInvocationMonitor.class.getSimpleName(), PDKInvocationMonitor.getInstance());
        }
    }

    public static void releaseAssociateId(String associateId) {
        tapConnectorManager.releaseAssociateId(associateId);
    }

    public static void refreshJars() {
        refreshJars(null);
    }

    public static void refreshJars(String oneJarPath) {
        init();
        tapConnectorManager.refreshJars(oneJarPath);
    }
    public static boolean hasJar(String oneJarPath) {
        init();
        return tapConnectorManager.checkTapConnectorByJarName(oneJarPath);
    }

    public static ProcessorBuilder<ProcessorNode> createProcessorBuilder() {
        init();
        return new ProcessorConnectorBuilder();
    }

    public static ConnectionConnectorBuilder createConnectionConnectorBuilder() {
        init();
        return new ConnectionConnectorBuilder();
    }

    public static ConnectorBuilder<ConnectorNode> createConnectorBuilder() {
        init();
        return new ConnectorBuilderEx();
    }

    public static void registerMemoryFetcher(String key, MemoryFetcher memoryFetcher) {
        init();
        memoryManager.register(key, memoryFetcher);
    }

    public static void unregisterMemoryFetcher(String key) {
        init();
        memoryManager.unregister(key);
    }

    public static String outputMemoryFetchers(List<String> keys, String keyRegex, String memoryLevel) {
        init();
        return memoryManager.output(keys, keyRegex, memoryLevel);
    }
    public static DataMap outputMemoryFetchersInDataMap(List<String> keys, String keyRegex, String memoryLevel) {
        init();
        return memoryManager.outputDataMap(keys, keyRegex, memoryLevel);
    }
}
