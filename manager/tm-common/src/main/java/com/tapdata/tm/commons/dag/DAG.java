package com.tapdata.tm.commons.dag;

import cn.hutool.core.map.MapUtil;
import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tapdata.tm.commons.dag.check.DAGCheckUtil;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateProcessorNode;
import com.tapdata.tm.commons.dag.process.ProcessorNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.vo.CustomTypeMapping;
import com.tapdata.tm.commons.dag.vo.FieldChangeRuleGroup;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.Message;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.util.Loader;
import com.tapdata.tm.commons.util.ThrowableUtils;
import io.github.openlg.graphlib.Graph;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/11/3 下午4:06
 * @description
 */
public class DAG implements Serializable, Cloneable {

    private static Logger logger = LoggerFactory.getLogger(DAG.class);
    public static Map<String, Class<? extends Node>> nodeMapping = new HashMap<>();

    private final transient Graph<Node, Edge> graph;

    private String id;
    /**
     * 任务id
     */
    @Getter
    @Setter
    private ObjectId taskId;

    @Setter
    @Getter
    private String syncType;

    /**
     * DAG owner id
     */
    @Getter
    @Setter
    private String ownerId;

    static {

        ClassPathScanningCandidateComponentProvider classPathScanningCandidateComponentProvider =
                new ClassPathScanningCandidateComponentProvider(true);
        classPathScanningCandidateComponentProvider.addIncludeFilter(new AnnotationTypeFilter(NodeType.class));
        Set<BeanDefinition> result = classPathScanningCandidateComponentProvider.findCandidateComponents(DAG.class.getPackage().getName());
        result.forEach(beanDefinition -> {
            try {
                Class<?> nodeClass = Class.forName(beanDefinition.getBeanClassName());
                if (!Loader.isExtends(nodeClass, Node.class)) {
                    logger.debug("Class {} not extends {}, skip.", nodeClass.getName(), Node.class.getName() );
                    return;
                }
                NodeType nodeType = nodeClass.getAnnotation(NodeType.class);
                nodeMapping.put(nodeType.value(), (Class<? extends Node>)nodeClass);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        });
        /*try {

            List<String> list = Loader.getResourceFiles(DAG.class.getPackage().getName());

            list.forEach(url -> {
                try {
                    Class<?> nodeClass = Class.forName(url);
                    if (!Loader.isExtends(nodeClass, Node.class)) {
                        logger.debug("Class {} not extends {}, skip.", nodeClass.getName(), Node.class.getName() );
                        return;
                    }
                    NodeType nodeType = nodeClass.getAnnotation(NodeType.class);
                    if (nodeType == null) {
                        logger.debug("Class {} no have NodeType annotation , skip.", nodeClass.getName());
                        return;
                    }
                    nodeMapping.put(nodeType.value(), (Class<? extends Node>)nodeClass);
                } catch (ClassNotFoundException e) {
                    logger.error("Load node type failed.", e);
                    e.printStackTrace();
                }
            });

        } catch (IOException e) {
            logger.error("Load node type failed.", e);
            e.printStackTrace();
        }*/

    }

    public DAG(Graph<Node, Edge> graph) {
        this.graph = graph;
    }


    /**
     * 根据 Dag实体 列表构建 DAG
     * @see Dag
     * @param taskDag
     * @return
     */
    public static DAG build(Dag taskDag) {
        return ignoreDisabledNode(taskDag, false);
    }

    /**
     * 根据 Dag实体 列表构建 DAG
     * @see Dag
     * @param beforeDAG
     * @param ignore 是否将已禁用的节点不加入DAG中
     * @return DAG
     */
    private static DAG ignoreDisabledNode(Dag beforeDAG, boolean ignore) {
        Graph<Node, Edge> graph = new Graph<>();
        DAG dag = new DAG(graph);

        List<Edge> edges = beforeDAG.getEdges();
        ConcurrentHashMap<String, String> edgeMap = new ConcurrentHashMap<>();
        List<Node> nodes = beforeDAG.getNodes();
        Set<String> nodeIds = ignore ? new HashSet<>() : null;
        if (ignore && CollectionUtils.isNotEmpty(nodes)) {
            nodes = filterDisabledNode(nodes, nodeIds);
        }
        if (CollectionUtils.isNotEmpty(edges)) {
            for (Edge edge : edges) {
                if(null == edge) continue;
                String from = edge.getSource();
                String target = edge.getTarget();
                if (ignore && (!nodeIds.contains(from) || !nodeIds.contains(target))) continue;
                graph.setEdge(from, target, edge);
                edgeMap.put(target, from);
                edge.setDag(dag);
                edge.setGraph(graph);
            }
        }
        if (CollectionUtils.isNotEmpty(nodes)) {
            List<String> tableNamesList = Lists.newArrayList();
            Set<String> targetIdList = graph.getSinks();
            Set<String> resourceIdList = graph.getSources();

            nodes.stream().filter(node -> node instanceof DatabaseNode && resourceIdList.contains(node.getId())
                    && CollectionUtils.isNotEmpty(((DatabaseNode) node).getTableNames()))
                    .forEach(resource -> tableNamesList.addAll(((DatabaseNode) resource).getTableNames()));

            ArrayList<String> objectNames = Lists.newArrayList(tableNamesList);
            LinkedList<Node> nodeLists = parseLinkedNode(beforeDAG);

            LinkedHashMap<String, String> tableNameRelation = generateTableNameRelation(nodeLists, objectNames);

            for (Node<?> node : nodes) {
                if (node == null) {
                    continue;
                }

                // 补充目标节点 syncObjects
                String targetId = node.getId();
                if (node instanceof DatabaseNode && targetIdList.contains(targetId)) {
                    SyncObjects syncObjects = new SyncObjects();
                    syncObjects.setType("table");
                    syncObjects.setTableNameRelation(tableNameRelation);
                    if (CollectionUtils.isNotEmpty(objectNames)) {
                        syncObjects.setObjectNames(objectNames);
                    } else {
                        syncObjects.setObjectNames(Lists.newArrayList());
                    }

                    List<SyncObjects> list = new ArrayList<>();
                    list.add(syncObjects);

                    ((DatabaseNode) node).setSyncObjects(list);
                    ((DatabaseNode) node).setTableNames(null);
                }

                graph.setNode(targetId, node);
                node.setGraph(graph);
                node.setDag(dag);
            }
        }

        return dag;
    }

    public static LinkedHashMap<String, String> generateTableNameRelation(LinkedList<Node> nodeLists, ArrayList<String> tableNames) {
        Map<String, String> renameNodeConvertTableName = new HashMap<>();
        LinkedHashMap<String, String> tableNameRelation = new LinkedHashMap<>();
        if (CollectionUtils.isNotEmpty(nodeLists)) {
            for (Node nodeList : nodeLists) {
                if (nodeList instanceof TableRenameProcessNode) {
                    renameNodeConvertTableName = getConvertTableNameMap((TableRenameProcessNode) nodeList, tableNames);
                }
            }
        }
        for (int i = 0; i < tableNames.size(); i++) {
            String originalName = tableNames.get(i);
            if (renameNodeConvertTableName.containsKey(originalName)) {
                String currentTableName = renameNodeConvertTableName.get(originalName);
                tableNameRelation.put(originalName, currentTableName);
                tableNames.set(i, currentTableName);
            } else {
                tableNameRelation.put(originalName, originalName);
            }
        }
        return tableNameRelation;
    }

    public static Map<String, String> getConvertTableNameMap(TableRenameProcessNode renameProcessNode, List<String> tableNames) {
        Map<String, String> tableRenameTableMap = new HashMap<>();
        Map<String, TableRenameTableInfo> originalMap = renameProcessNode.originalMap();
        for (int i = 0; i < tableNames.size(); i++) {
            String originalName = tableNames.get(i);
            String currentTableName = renameProcessNode.convertTableName(originalMap, originalName, false);
            tableRenameTableMap.put(originalName, currentTableName);
        }
        return tableRenameTableMap;
    }
    private static List<Node> filterDisabledNode(List<Node> nodes, Set<String> nodeIds) {
        return nodes.stream().filter(next -> {
            Map<String, Object> attrs = next.getAttrs();
            if (null != attrs && !attrs.isEmpty()) {
                Object disable = attrs.get("disabled");
                return !((disable instanceof Boolean) && (Boolean)disable);
            }
            return true;
        }).filter(next -> {
            nodeIds.add(next.getId());
            return true;
        }).collect(Collectors.toList());
    }

    public static LinkedList<Node> parseLinkedNode(Dag dag) {
        List<Edge> edges = dag.getEdges();
        List<String> sourceList = edges.stream().map(Edge::getSource).collect(Collectors.toList());
        List<String> targetList = edges.stream().map(Edge::getTarget).collect(Collectors.toList());

        List<String> firstSourceList = sourceList.stream().filter(s -> !targetList.contains(s)).collect(Collectors.toList());

        LinkedList<Node> linkedNode = new LinkedList<>();
        if (CollectionUtils.isEmpty(firstSourceList)) {
            return linkedNode;
        }

        LinkedList<String> list = new LinkedList<>();
        String source = firstSourceList.get(0);
        list.add(source);
        Map<String, String> edgeMap = new HashMap<>();
        for (Edge edge : edges) {
            edgeMap.put(edge.getSource(), edge.getTarget());
        }

        while ((source = edgeMap.get(source)) != null) {
            list.add(source);
        }

        List<Node> nodes = dag.getNodes();
        Map<String, Node> nodeMap = nodes.stream().filter(Objects::nonNull).collect(Collectors.toMap(Element::getId, n -> n, (m1, m2) -> m1));
        for (String s : list) {
            linkedNode.add(nodeMap.get(s));
        }
        return linkedNode;
    }

    /**
     * 将DAG转成Dag
     * @see Dag
     * @return
     */
    public Dag toDag() {
        Dag dag = new Dag();
        dag.setNodes(getNodes());
        dag.setEdges(getEdges());
        return dag;
    }

    /**
     * 按照所有相连的节点拆分为DAG
     * @return
     */
    public List<DAG> split() {
        return graph.components().stream().map(DAG::new).collect(Collectors.toList());
    }

//    /**
//     * 根据新的 dag 更新当前 dag
//     * 这个方法写的过于复杂，但是目前没有想到更合理的方法， 有时间结合flowengin的运行情况，可以好好优化一下、
//     * @param newDags
//     * @param subTaskDtos
//     * @return
//     */
//    public List<SubTaskStatus> update(List<DAG> newDags, List<SubTaskDto> subTaskDtos) {
//        //先给dag赋值一个唯一id方便后面的清查
//        for (DAG newDag : newDags) {
//            newDag.id = UUID.randomUUID().toString();
//        }
//
//
//        Map<String, Integer> levelMap = new HashMap<>();
//        Map<String, ObjectId> subTaskMap = new HashMap<>();
//
//
//        //每个节点赋值权重
//        for (SubTaskDto subTaskDto : subTaskDtos) {
//            if (subTaskDto.getTempDag() != null) {
//                subTaskDto.setDag(subTaskDto.getTempDag());
//            }
//
//            DAG oldDag = subTaskDto.getDag();
//
//            for (Node node : oldDag.getNodes()) {
//                subTaskMap.put(node.getId(), subTaskDto.getId());
//            }
//            Set<String> sources = oldDag.graph.getSources();
//            for (String source : sources) {
//                int level = 1;
//                levelMap.put(source, level);
//                Node node = oldDag.getNode(source);
//                List<Node> successors = node.successors();
//
//                if (CollectionUtils.isNotEmpty(successors)) {
//                    setLevel(levelMap, successors, level);
//                }
//            }
//        }
//
//
//        //新节点确认对应老节点的匹配项
//        Map<String, Map<ObjectId, List<Integer>>> newCache = new HashMap<>();
//        for (DAG newDag : newDags) {
//            Map<ObjectId, List<Integer>> oldLinkMap = new HashMap<>();
//            for (Node node : newDag.getNodes()) {
//                ObjectId subId = subTaskMap.get(node.getId());
//                if (subId != null) {
//                    List<Integer> values = oldLinkMap.get(subId);
//                    if (values == null) {
//                        values = new ArrayList<>();
//                    }
//                    values.add(levelMap.get(node.getId()));
//                    oldLinkMap.put(subId, values);
//                }
//            }
//            newCache.put(newDag.id, oldLinkMap);
//        }
//
//        //节点的最大深度
//        int max = levelMap.values().stream().max(Comparator.naturalOrder()).orElse(0);
//
//        Map<String, DAG> newDagMap = newDags.stream().collect(Collectors.toMap(d -> d.id, d -> d));
//
//
//        List<SubTaskStatus> updateDags = new ArrayList<>();
//
//        //匹配新老节点
//        //已经匹配过了的新节点，不重复匹配
//        Set<String> repeatSet = new HashSet<>();
//        next:
//        for (SubTaskDto subTaskDto : subTaskDtos) {
//
//            //根据最大深度，进行最左原则匹配
//            for (int i = 1; i <= max; i++) {
//
//                Map<String, List<Integer>> newDagsMap = new HashMap<>();
//                for (Map.Entry<String, Map<ObjectId, List<Integer>>> entry : newCache.entrySet()) {
//                    if (repeatSet.contains(entry.getKey())) {
//                        //已经被有缘的老节点匹配上了
//                        continue;
//                    }
//
//                    Map<ObjectId, List<Integer>> value = entry.getValue();
//                    if (value != null) {
//                        List<Integer> values = value.get(subTaskDto.getId());
//                        if (CollectionUtils.isNotEmpty(values)) {
//                            if (values.contains(i)) {
//                                newDagsMap.put(entry.getKey(), values);
//                            }
//                        }
//                    }
//                }
//
//                if (newDagsMap.size() != 0) {
//                    String dagId = compareDag(newDagsMap, i, max);
//                    if (dagId != null) {
//                        repeatSet.add(dagId);
//                        SubTaskStatus subTaskStatus = new SubTaskStatus(newDagMap.get(dagId), subTaskDto.getId(), Action.update.name());
//                        updateDags.add(subTaskStatus);
//                        //当前的老节点已经匹配到新的任务，跳出循环
//                        continue next;
//                    }
//                }
//            }
//        }
//
//        //老节点没有匹配上的则删除
//        //新节点没有匹配上的则新增
//        Set<ObjectId> updateSubTaskIds = updateDags.stream().map(SubTaskStatus::getSubTaskId).collect(Collectors.toSet());
//        for (SubTaskDto subTaskDto : subTaskDtos) {
//            boolean contains = updateSubTaskIds.contains(subTaskDto.getId());
//            if (!contains) {
//                updateDags.add(new SubTaskStatus(subTaskDto.getDag(), subTaskDto.getId(), Action.delete.name()));
//            }
//        }
//
//        for (DAG newDag : newDags) {
//            boolean contains = repeatSet.contains(newDag.id);
//            if (!contains) {
//                updateDags.add(new SubTaskStatus(newDag, null, Action.create.name()));
//            }
//        }
//
//        return updateDags;
//    }


    //根据最左节点院子，多个新节点匹配到老节点时，需要比较后面的节点比较是否匹配，能匹配到最优的新节点
    private String compareDag(Map<String, List<Integer>> newDagsMap, int start, int end) {
        List<String> keyList = new ArrayList<>(newDagsMap.keySet());

        if (keyList.size() == 1) {
            return keyList.get(0);
        }
        Set<String> disuseSet = new HashSet<>();
        int size = newDagsMap.size();
        for (int i = start; i <= end; i++) {
            for (Map.Entry<String, List<Integer>> entry : newDagsMap.entrySet()) {
                List<Integer> values = entry.getValue();
                if (!values.contains(i)) {
                    disuseSet.add(entry.getKey());
                    if (disuseSet.size() + 1 == size) {
                        keyList.removeAll(disuseSet);
                        return keyList.get(0);
                    }
                }
            }
        }

        return null;
    }


    //设置node在dag中的层级， 当前节点如果根据不同的源节点能匹配到多个层级，则去层级最小的值
    private void setLevel(Map<String, Integer> levelMap, List<Node> successors, int level) {
        level++;
        for (Node successor : successors) {
            Integer v = levelMap.get(successor.getId());
            if (v == null || v > level) {
                levelMap.put(successor.getId(), level);
            }
            successors = successor.successors();
            if (CollectionUtils.isNotEmpty(successors)) {
                setLevel(levelMap, successors, level);
            }
        }
    }

    /**
     * 模型推演入口
     * @param nodeId 节点id，不指定默认全部推演一遍
     * @return 错误消息列表，推演成功返回 0 长度map，推演失败返回错误消息列表
     */
    public Map<String, List<Message>> transformSchema(String nodeId, DAGDataService dagDataService) {
        return transformSchema(nodeId, dagDataService, null);
    }
    public Map<String, List<Message>> transformSchema(String nodeId, DAGDataService dagDataService, Options options) {
        return transformSchema(nodeId, dagDataService, options,null);
    }
    /**
     * 模型推演入口
     * @param nodeId 节点id，不指定默认全部推演一遍
     * @return 错误消息列表，推演成功返回 0 长度map，推演失败返回错误消息列表
     */
    public Map<String, List<Message>> transformSchema(String nodeId, DAGDataService dagDataService, Options options, Consumer<Exception> consumer) {
        try {

            long start = System.currentTimeMillis();
            if (dagDataService != null) {
                graph.getNodes().stream().map(graph::getNode)
                        .filter(Objects::nonNull)
                        .forEach(node -> node.setService(dagDataService));

            }

            if (dagDataService instanceof DAGDataServiceImpl) {
                ObjectId taskId1 = ((DAGDataServiceImpl) dagDataService).getTaskId();
                this.setTaskId(taskId1);

                addNodeEventListener(new Node.EventListener<Object>() {
                    final Map<String, List<SchemaTransformerResult>> results = new HashMap<>();
                    final Map<String, List<SchemaTransformerResult>> lastBatchResults = new HashMap<>();
                    @Override
                    public void onTransfer(List<Object> inputSchemaList, Object schema, Object outputSchema, String nodeId) {
                        List<SchemaTransformerResult> schemaTransformerResults = results.get(nodeId);
                        if (schemaTransformerResults == null) {
                            return;
                        }
                        List<Schema> outputSchemaList;
                        if (outputSchema instanceof List) {
                            outputSchemaList = (List) outputSchema;

                        } else {
                            Schema outputSchema1 = (Schema) outputSchema;
                            outputSchemaList = Lists.newArrayList(outputSchema1);
                        }

                        List<MetadataInstancesDto> all = outputSchemaList.stream().map(s ->  ((DAGDataServiceImpl) dagDataService).getMetadata(s.getQualifiedName())).filter(Objects::nonNull).collect(Collectors.toList());
                        Map<String, MetadataInstancesDto> metaMaps = all.stream().collect(Collectors.toMap(MetadataInstancesDto::getQualifiedName, m -> m, (m1, m2) -> m1));
                        for (SchemaTransformerResult schemaTransformerResult : schemaTransformerResults) {
                            if (Objects.isNull(schemaTransformerResult)) {
                                continue;
                            }
                            MetadataInstancesDto metadataInstancesEntity = metaMaps.get(schemaTransformerResult.getSinkQulifiedName());
                            if (metadataInstancesEntity != null && metadataInstancesEntity.getId() != null) {
                                schemaTransformerResult.setSinkTableId(metadataInstancesEntity.getId().toHexString());
                            }
                        }
                    }

                @Override
                public void schemaTransformResult(String nodeId, Node node, List<SchemaTransformerResult> schemaTransformerResults) {
                    List<SchemaTransformerResult> results1 = results.get(nodeId);
                    if (CollectionUtils.isNotEmpty(results1)) {
                        results1.addAll(schemaTransformerResults);
                    } else {
                        results.put(nodeId, schemaTransformerResults);
                    }
                    lastBatchResults.put(nodeId, schemaTransformerResults);
                }

                    @Override
                    public List<SchemaTransformerResult> getSchemaTransformResult(String nodeId) {
                        return lastBatchResults.get(nodeId);
                    }
                });
            }

            if (taskId != null && dagDataService != null) {
                AtomicInteger finishedTransferCount = new AtomicInteger(0);
                int total = graph.getNodes().size();
                String id = taskId.toHexString();
                addNodeEventListener(new Node.EventListener<Object>() {
                    @Override
                    public void onTransfer(List<Object> inputSchemaList, Object schema, Object outputSchema, String nodeId) {
                        int finished = finishedTransferCount.addAndGet(1);
                        dagDataService.updateTransformRate(id, total, Math.min(finished, total));
                    }

                @Override
                public void schemaTransformResult(String nodeId, Node node, List<SchemaTransformerResult> schemaTransformerResults) {

                    }

                    @Override
                    public List<SchemaTransformerResult> getSchemaTransformResult(String nodeId) {
                        return null;
                    }
                });
            }

            List<Node> allNodes = getNodes();
            for (Node allNode : allNodes) {
                allNode.setSchema(null);
            }
            setIsomorphismValueToOptions(options, allNodes);
            if (nodeId != null) {
                Node node = graph.getNode(nodeId);
                clearTransFlag(node);
                graph.getNode(nodeId).transformSchema(options);
            } else {
                List<Node> nodes = this.getNodes();
                if (CollectionUtils.isNotEmpty(nodes)) {
                    for (Node node : nodes) {
                        node.setTransformed(false);
                    }
                }
                graph.getSources().forEach(source -> graph.getNode(source).transformSchema(options));
            }
            logger.debug("Transform schema cost {}ms", System.currentTimeMillis() - start);
        } catch (Exception e) {
            logger.error("transformSchema error:" + ThrowableUtils.getStackTraceByPn(e));
            if(consumer != null)consumer.accept(e);
            Map<String, List<Message>> msg = Maps.newHashMap();
            msg.put(taskId.toHexString(), Lists.newArrayList(new Message("error", e.getMessage(), JSON.toJSONString(e.getStackTrace()), null)));
            return msg;
        }
        return new HashMap<>();
    }

    /**
     * 获取所有源节点
     * @return
     */
    public List<Node> getSourceNodes() {
        return graph.getSources().stream().map(graph::getNode).collect(Collectors.toList());
    }

    private void clearTransFlag(Node node) {
        node.setTransformed(false);
        List<Node> successors = node.successors();
        if (CollectionUtils.isNotEmpty(successors)) {
            for (Node successor : successors) {
                clearTransFlag(successor);
            }
        }
    }

    /**
     * 根据属性查询出所有匹配的节点
     * @param key 属性名称
     * @param value 属性值，为 null 时，返回所有包含 key 属性的节点
     * @return
     */
    public List<Node> findByAttribute(String key, Object value) {
        return _findByAttribute(key, value).collect(Collectors.toList());
    }

    /**
     * 根据属性查询出第一个匹配的节点
     * @param key 属性名称
     * @param value 属性值，为 null 时，返回第一个包含 key 属性的节点
     * @return
     */
    public Optional<Node> findOneByAttribute(String key, Object value) {
        if (key == null) {
            return Optional.empty();
        }
        return _findByAttribute(key, value).findFirst();
    }

    private Stream<Node> _findByAttribute(String key, Object value) {
        if (key == null) {
            return Stream.empty();
        }
        return graph.getNodes().stream().filter( id -> {
            Map<String, Object> attrs = graph.getNode(id).getAttrs();
            if (attrs != null && attrs.containsKey(key)) {
                if (value != null) {
                    return value.equals(attrs.get(key));
                }
                return true;
            }
            return false;
        }).map(graph::getNode);
    }

    /**
     * 校验 DAG 以及所有节点配置，并收集校验失败的错误消息
     * @return 错误消息列表，nodeId -> [{ code: "", msg: ""}]
     */
    public Map<String, List<Message>> validate() {
        Map<String, List<Message>> messages = new HashMap<>();

        //检查DAG前去除被disabled的节点
        DAG ignoreDisabledDAG = ignoreDisabledNode(this.toDag(), true);
        //校验dag
        Map<String, List<Message>> checkDagMessage = ignoreDisabledDAG.checkDag();
        if (!checkDagMessage.isEmpty()) {
            return checkDagMessage;
        }

        ignoreDisabledDAG.graph.getNodes().forEach(nodeId -> {
            Node node = ignoreDisabledDAG.graph.getNode(nodeId);
            boolean result = node.validate();
            if (!result) {
                messages.put(nodeId, node.getMessages());
            }
        });

        return messages;
    }

    /**
     * 返回当前DAG中所有的节点数据
     * @return all node
     */
    public List<Node> getNodes(){
        return graph.getNodes().stream().map(graph::getNode).collect(Collectors.toList());
    }

    /**
     * 返回当前DAG中所有的连线数据
     * @return all edge
     */
    public LinkedList<Edge> getEdges() {
        return graph.getEdges().stream().map(graph::getEdge).collect(Collectors.toCollection(LinkedList::new));
    }

    public Node<?> getNode(String nodeId) {
        return graph.getNode(nodeId);
    }

    /**
     * get pre all node, only migrate task use
     * @param nodeId
     * @return
     */
    public LinkedList<Node<?>> getPreNodes(String nodeId) {
        return nodeMap().get(nodeId);
    }

    /**
     * get nodeMap only migrate task use
     * @return
     */
    public Map<String, LinkedList<Node<?>>> nodeMap() {
        Map<String, LinkedList<Node<?>>> nodeMap = Maps.newHashMap();

        Collection<io.github.openlg.graphlib.Edge> edges = graph.getEdges();
        Map<String, io.github.openlg.graphlib.Edge> sourceMap = edges.stream().collect(Collectors.toMap(io.github.openlg.graphlib.Edge::getSource, Function.identity()));

        // inspect edges order
        List<String> sourceList = edges.stream().map(io.github.openlg.graphlib.Edge::getSource).collect(Collectors.toList());
        List<String> targetList = edges.stream().map(io.github.openlg.graphlib.Edge::getTarget).collect(Collectors.toList());

        List<String> firstSourceList = sourceList.stream().filter(s -> !targetList.contains(s)).collect(Collectors.toList());

        // more edge line  ex: a->b->c && e->d
        for (String temp : firstSourceList) {
            // get order correct edge
            LinkedList<io.github.openlg.graphlib.Edge> edgeLinkedList = Lists.newLinkedList();
            for (int i = 0; i < edges.size(); i++) {
                io.github.openlg.graphlib.Edge edge = sourceMap.get(temp);
                if (Objects.isNull(edge)) {
                    continue;
                }
                edgeLinkedList.add(edge);
                temp = edge.getTarget();
            }

            edgeLinkedList.forEach(edge -> {
                String source = edge.getSource();
                String target = edge.getTarget();

                if (!nodeMap.containsKey(target)) {
                    Node<?> sourceNode = this.getNode(source);
                    LinkedList<Node<?>> pre = nodeMap.get(source);
                    if (Objects.nonNull(pre)) {
                        ImmutableList<Node<?>> copyList = ImmutableList.copyOf(pre);
                        nodeMap.put(target, new LinkedList<Node<?>>(){{addAll(copyList);add(sourceNode);}});
                    } else {
                        nodeMap.put(target, new LinkedList<Node<?>>(){{add(sourceNode);}});
                    }
                }
            });
        }

        return nodeMap;
    }

    public Map<String, LinkedList<Edge>> edgeMap() {
        Map<String, LinkedList<Edge>> edgeMap = Maps.newHashMap();

        Collection<io.github.openlg.graphlib.Edge> edges = graph.getEdges();

        // inspect edges order
        List<String> sourceList = edges.stream().map(io.github.openlg.graphlib.Edge::getSource).collect(Collectors.toList());
        List<String> targetList = edges.stream().map(io.github.openlg.graphlib.Edge::getTarget).collect(Collectors.toList());

        List<String> firstSourceList = sourceList.stream().filter(s -> !targetList.contains(s)).collect(Collectors.toList());
        if (firstSourceList.size() != NumberUtils.INTEGER_ONE) {
            return edgeMap;
        }
        // get order correct edge
        LinkedList<io.github.openlg.graphlib.Edge> edgeLinkedList = Lists.newLinkedList();
        Map<String, io.github.openlg.graphlib.Edge> sourceMap = edges.stream().collect(Collectors.toMap(io.github.openlg.graphlib.Edge::getSource, Function.identity()));
        String temp = firstSourceList.get(0);
        for (int i = 0; i < edges.size(); i++) {
            io.github.openlg.graphlib.Edge edge = sourceMap.get(temp);
            edgeLinkedList.add(edge);
            temp = edge.getTarget();
        }

        edgeLinkedList.forEach(edge -> {
            String source = edge.getSource();
            String target = edge.getTarget();

            if (edgeMap.containsKey(source)) {
                edgeMap.put(target, new LinkedList<Edge>(){{addAll(edgeMap.get(source));add(new Edge(edge.getSource(), edge.getTarget()));}});
            } else {
                edgeMap.put(target, new LinkedList<Edge>(){{add(new Edge(edge.getSource(), edge.getTarget()));}});
            }
        });

        return edgeMap;
    }


    public List<Node> getTarget(String nodeId) {
        return graph.getNode(nodeId).successors();
    }
    public boolean hasNode(String nodeId) {
        return graph.hasNode(nodeId);
    }
    public List<Node> predecessors(String id) {
        return graph.predecessors(id).stream().map(graph::getNode).collect(Collectors.toList());
    }
    public List<Node> successors(String id) {
        return graph.successors(id).stream().map(graph::getNode).collect(Collectors.toList());
    }

    //校验dag
    private Map<String, List<Message>> checkDag() {


        List<Message> messageList = new ArrayList<>();
        Map<String, List<Message>> returnMap = new HashMap<>();
        if (isLogCollectorDag()) {
            //日志挖掘任务不做dag校验
            return returnMap;
        }else if( isShareCacheDag() ) {
            //共享缓存不做dag校验
            return returnMap;
        }


        List<Node> nodes = this.getNodes();

        if (CollectionUtils.isEmpty(nodes)) {
            logger.warn("the number of node is zero");
            Message message = new Message();
            message.setCode("DAG.NotNodes");
            message.setMsg("dag nodes not found");
            messageList.add(message);
            returnMap.put("nullNode", messageList);
            return returnMap;
        }

        Set<String> sources = graph.getSources();
        if (CollectionUtils.isNotEmpty(sources)) {
            String node = new ArrayList<>(sources).get(0);
            returnMap.put(node, messageList);
        } else {
            String node = new ArrayList<>(graph.getNodes()).get(0);
            returnMap.put(node, messageList);
        }


        List<Edge> edges = this.getEdges();
        if (CollectionUtils.isEmpty(edges)) {
            logger.warn("the number of edges is zero");
            Message message = new Message();
            message.setCode("DAG.NotEdges");
            message.setMsg("dag edges not found");
            messageList.add(message);
            return returnMap;
        }

        //判断dag的数据节点数量是否大于2
        Optional<Integer> dataNodeCountOp = nodes.stream().map(n -> n.isDataNode() ? 1 : 0).reduce(Integer::sum);

        int dataNodeCount = dataNodeCountOp.orElse(0);
        logger.debug("The number of nodes of the data type is {}", dataNodeCount);
        if (dataNodeCount < 2) {
            Message message = new Message();
            message.setCode("DAG.NodeTooFew");
            message.setMsg("The number of nodes of the data type is less than two");
            messageList.add(message);
        }

        //判断每个节点是否都存在连线, 不能有孤立的节点
        Set<String> edgeNodeIds = new HashSet<>();
        for (Edge edge : edges) {
            edgeNodeIds.add(edge.getSource());
            edgeNodeIds.add(edge.getTarget());
        }

        for (Node node : nodes) {
            if (!edgeNodeIds.contains(node.getId())) {
                Message message = new Message();
                message.setCode("DAG.NodeIsolated");
                message.setMsg("this node not connect other, name = " + node.getName());
                messageList.add(message);
            }

            //检查Join节点
            if (node instanceof JoinProcessorNode) {
                DAGCheckUtil.checkJoinNode((JoinProcessorNode) node, this.getEdges(), messageList);
            }
        }

        //每个连线的节点必须存在
        Set<String> nodeIds = nodes.stream().map(Node::getId).collect(Collectors.toSet());
        for (String nodeId : edgeNodeIds) {
            if (!nodeIds.contains(nodeId)) {
                Message message = new Message();
                message.setCode("DAG.EdgeNotLink");
                message.setMsg("edge node is not found");
                messageList.add(message);
            }
        }

        //判断dag是否存在环
        List<List<String>> cycles = this.graph.findCycles();
        if (CollectionUtils.isNotEmpty(cycles)) {
            Message message = new Message();
            message.setCode("DAG.IsCyclic");
            message.setMsg("dag is cyclic");
            messageList.add(message);
        }

        //TODO: 校验所有的源节点和所有的目标节点都是数据节点
        Map<String, Node> nodeMap = nodes.stream().collect(Collectors.toMap(Node::getId, n -> n));
        for (String source : sources) {
            Node node = nodeMap.get(source);
            if (!node.isDataNode()) {
                Message message = new Message();
                message.setCode("DAG.SourceIsNotData");
                message.setMsg("source is not data node, source =" + source);
                messageList.add(message);
            }

            if (node instanceof DatabaseNode) {
                DatabaseNode databaseNode = (DatabaseNode) node;
                List<String> tableNames = databaseNode.getTableNames();
                if (CollectionUtils.isEmpty(tableNames) && !"expression".equals(databaseNode.getMigrateTableSelectType())) {
                    Message message = new Message();
                    message.setCode("DAG.MigrateTaskNotContainsTable");
                    message.setMsg("task not contains tables");
                    messageList.add(message);
                }
            }
        }

        Set<String> sinks = graph.getSinks();
        for (String sink : sinks) {
            Node node = nodeMap.get(sink);
            if (!node.isDataNode()) {
                Message message = new Message();
                message.setCode("DAG.TailIsNotData");
                message.setMsg("tail is not data node, sink =" + sink);
                messageList.add(message);
            }
        }
        if (CollectionUtils.isEmpty(messageList)) {
            returnMap.clear();
        }
        return returnMap;
    }

    private transient List<Node.EventListener> nodeEventListeners;
    private Node.EventListener privateNodeEventListener;
    public <S> void addNodeEventListener(final Node.EventListener<S> nodeEventListener) {
        if (nodeEventListeners == null) {
            nodeEventListeners = new ArrayList<>();
            nodeEventListeners.add(nodeEventListener);
            privateNodeEventListener = new Node.EventListener<S>(){
                @Override
                public void onTransfer(List<S> inputSchemaList, S schema, S outputSchema, String nodeId) {
                    nodeEventListeners.forEach(nodeEventListener -> {
                        try {
                            nodeEventListener.onTransfer(inputSchemaList, schema, outputSchema, nodeId);
                        } catch (Exception e) {
                            logger.error("Trigger transfer listener failed {}", e.getMessage());
                        }
                    });
                }

                @Override
                public void schemaTransformResult(String nodeId,Node node, List<SchemaTransformerResult> schemaTransformerResults) {
                    nodeEventListeners.forEach(nodeEventListener -> {
                        try {
                            nodeEventListener.schemaTransformResult(nodeId, node, schemaTransformerResults);
                        } catch (Exception e) {
                            logger.error("Trigger transfer listener failed {}", e.getMessage());
                        }
                    });
                }

                @Override
                public List<SchemaTransformerResult> getSchemaTransformResult(String nodeId) {
                    return nodeEventListener.getSchemaTransformResult(nodeId);
                }
            };
            getNodes().forEach(n -> n.setListener(privateNodeEventListener));
        }
    }

    public List<Node> getSources() {
        List<Node> sourceList = new ArrayList<>();
        Set<String> sources = this.graph.getSources();
        for (String source : sources) {
            sourceList.add(this.graph.getNode(source));
        }
        return sourceList;
    }

    public List<Node> getTargets() {
        List<Node> sinkList = new ArrayList<>();
        Set<String> sinks = this.graph.getSinks();
        for (String sink : sinks) {
            sinkList.add(this.graph.getNode(sink));
        }
        return sinkList;
    }

    public boolean isLogCollectorDag() {
        Set<String> sources = this.graph.getSources();
        for (String source : sources) {
            Node node = this.graph.getNode(source);
            if (node.getCatalog() == Node.NodeCatalog.logCollector) {
                return true;
            }
        }
        return false;
    }

    public boolean isShareCacheDag() {
        Collection<String> nodes = this.graph.getNodes();
        for (String nodeName : nodes) {
            Node node = this.graph.getNode(nodeName);
            if (node != null) {
                if (node.getCatalog() == Node.NodeCatalog.memCache) {
                    return true;
                }
            }
        }
        return false;
    }

    public LinkedList<DatabaseNode> getSourceNode() {
        return graph.getNodes()
                .stream()
                .map(graph::getNode)
                .collect(Collectors.toList())
                .stream()
                .filter(node -> node instanceof DatabaseNode && graph.getSources().contains(node.getId()))
                .map(node -> (DatabaseNode) node).collect(Collectors.toCollection(LinkedList::new));
    }

    public LinkedList<DataParentNode> getSourceDataParentNode() {
        return graph.getNodes()
                .stream()
                .map(graph::getNode)
                .collect(Collectors.toList())
                .stream()
                .filter(node -> node instanceof DataParentNode && graph.getSources().contains(node.getId()))
                .map(node -> (DataParentNode) node).collect(Collectors.toCollection(LinkedList::new));
    }

    public DatabaseNode getSourceNode(String nodeId) {
        String sourceNodeId = getSourceNodeIdByNode(nodeId);
        Node<?> node = graph.getNode(sourceNodeId);
        return node instanceof DatabaseNode ? (DatabaseNode) node : null;
    }

    private String getSourceNodeIdByNode(String nodeId) {
        Collection<String> collection = graph.predecessors(nodeId);
        if (collection.isEmpty()) {
            return nodeId;
        } else {
            return getSourceNodeIdByNode(collection.iterator().next());
        }
    }

    private String getTargetNodeIdByMigrateNode(String nodeId) {
        Collection<String> collection = graph.successors(nodeId);
        if (collection.isEmpty()) {
            return nodeId;
        } else {
            return getTargetNodeIdByMigrateNode(collection.iterator().next());
        }
    }

    public LinkedList<DatabaseNode> getTargetNode() {
        return graph.getNodes()
                .stream()
                .map(graph::getNode)
                .collect(Collectors.toList())
                .stream()
                .filter(node -> node instanceof DatabaseNode && graph.getSinks().contains(node.getId()))
                .map(node -> (DatabaseNode) node).collect(Collectors.toCollection(LinkedList::new));
    }

    public LinkedList<DataParentNode> getTargetDataParentNode() {
        return graph.getNodes()
                .stream()
                .map(graph::getNode)
                .collect(Collectors.toList())
                .stream()
                .filter(node -> node instanceof DataParentNode && graph.getSinks().contains(node.getId()))
                .map(node -> (DataParentNode) node).collect(Collectors.toCollection(LinkedList::new));
    }

    public DatabaseNode getTargetNode(String nodeId) {
        Node<?> node = graph.getNode(getTargetNodeIdByMigrateNode(nodeId));
        return node instanceof DatabaseNode ? (DatabaseNode) node : null;
    }

    @Data
    @AllArgsConstructor
    public static class SubTaskStatus {
        private DAG dag;
        private ObjectId subTaskId;
        private String action; // create/delete/update
    }

    public enum Action {
        create,
        update,
        delete,

    }

    public static Class<? extends Node> getClassByType(String type) {
        return nodeMapping.get(type);
    }

    @Data
    @AllArgsConstructor
    public static class Options {

        public Options() {
        }

        public Options(String rollback, String rollbackTable) {
            this.rollback = rollback;
            this.rollbackTable = rollbackTable;
        }

        private String rollback; //: "table"/"all"
        private String rollbackTable; //: "Leon_CAR_CUSTOMER";
        private List<CustomTypeMapping> customTypeMappings;
        private String fieldsNameTransform;
        private List<String> includes; //: "Leon_CAR_CUSTOMER";
        private int batchNum;
        private String uuid;

        private String syncType;
        private FieldChangeRuleGroup fieldChangeRules;
        private boolean isIsomorphismTask;
        private boolean preview;
        private Map<String,List<DifferenceField>> differenceFields;
        private Map<String, String> tableRenameRelationMap;

        public Options(String rollback, String rollbackTable, List<CustomTypeMapping> customTypeMappings) {
            this.rollback = rollback;
            this.rollbackTable = rollbackTable;
            this.customTypeMappings = customTypeMappings;
        }
        public void processRule(MetadataInstancesDto dto, DefaultExpressionMatchingMap map) {
            if (null == fieldChangeRules) return;
            String nodeId = dto.getNodeId();
            for (Field f : dto.getFields()) {
                fieldChangeRules.process(nodeId, dto.getQualifiedName(), f, map);
            }
        }
        public void processDifferenceField(MetadataInstancesDto dto) {
            List<DifferenceField> differenceFieldList;
            if(MapUtil.isNotEmpty(differenceFields) && differenceFields.containsKey(dto.getQualifiedName())){
                differenceFieldList = differenceFields.get(dto.getQualifiedName());
            }else{
                differenceFieldList = new ArrayList<>();
            }
            if (CollectionUtils.isEmpty(differenceFieldList)) return;
            Map<String,Field> fieldMap = dto.getFields().stream().collect(Collectors.toMap(Field::getFieldName, Function.identity()));
            differenceFieldList.forEach(differenceField -> {
                differenceField.getType().processDifferenceField(fieldMap.get(differenceField.getColumnName()), dto.getFields(), differenceField);
            });
        }
    }


    public void filedDdlEvent(String nodeId, TapDDLEvent event) throws Exception {
        Node node = this.getNode(nodeId);
        List<Node> results = new ArrayList<>();
        results.add(node);

        if (event instanceof TapCreateTableEvent || event instanceof TapDropTableEvent) {
            if (node instanceof DatabaseNode ||  node instanceof MigrateProcessorNode) {
                next(event, node, results);
                Dag dag = toDag();
                DAG newDag = build(dag);
                BeanUtils.copyProperties(newDag, this);
            }
            return;
        }

        next(event, node, results);
    }

    private void next(TapDDLEvent event, Node node, List<Node> results) throws Exception {
        //递归找到所有需要ddl处理的节点
        List<Node> successors = node.successors();
        for (Node successor : successors) {
            nextDdlNode(successor, results);
        }


        for (Node result : results) {
            //将所有的节点进行ddl事件处理
            result.fieldDdlEvent(event);
        }
    }

    private void nextDdlNode(Node node, List<Node> results) {
//        if (node.isDataNode()) {
//            return;
//        }

        if (node instanceof ProcessorNode || node instanceof MigrateProcessorNode) {
            results.add(node);
            List<Node> successors = node.successors();
            for (Node successor : successors) {
                nextDdlNode(successor, results);
            }
        }
    }

    public boolean checkMultiDag() {
        List<DAG> split = split();
        return split.size() > 1;
    }

    @Override
    public DAG clone() throws CloneNotSupportedException {
        Dag dag = this.toDag();
        String json = JsonUtil.toJsonUseJackson(dag);
        Dag dag1 = JsonUtil.parseJsonUseJackson(json, new TypeReference<Dag>() {
        });
        return DAG.build(dag1);
    }

    /**确认当前任务是否是同构任务，源和目标的dataType一样，并且没有中间节点*/
    public boolean getTaskDtoIsomorphism(List<Node> nodes) {
        if (null == nodes || nodes.size() != 2) {
            return false;
        }

        Node node1 = nodes.get(0);
        if (!(node1 instanceof DataParentNode)) return false;
        Node node2 = nodes.get(1);
        if (!(node2 instanceof DataParentNode)) return false;
        DataParentNode dataParentNode1 = (DataParentNode)node1;
        DataParentNode dataParentNode2 = (DataParentNode)node2;
        String databaseType = String.valueOf(dataParentNode1.getDatabaseType());
        return databaseType.equals(dataParentNode2.getDatabaseType());
    }

    protected void setIsomorphismValueToOptions(Options options, List<Node> allNodes) {
        if (null != options) {
            options.setIsomorphismTask(getTaskDtoIsomorphism(allNodes));
        }
    }

    public List<Node> getAllTypeTargetNodes() {
        Set<String> sinks = graph.getSinks();
        return getNodes().stream().filter(node -> null != node && sinks.contains(node.getId())).collect(Collectors.toList());
    }

    public List<Node> getTargetNodes() {
        return graph.getSinks().stream().map(graph::getNode).collect(Collectors.toList());
    }

    public void replaceNode(Node oldNode, Node newNode) {
        LinkedList<Edge> edges = getEdges();
        LinkedList<Edge> edgesAsSource = edges.stream().filter(edge -> edge.getSource().equals(oldNode.getId())).collect(Collectors.toCollection(LinkedList::new));
        LinkedList<Edge> edgesAsTarget = edges.stream().filter(edge -> edge.getTarget().equals(oldNode.getId())).collect(Collectors.toCollection(LinkedList::new));
        graph.removeNode(oldNode.getId());
        graph.setNode(newNode.getId(), newNode);
        edgesAsSource.forEach(edge -> {
            edge.setSource(newNode.getId());
            graph.setEdge(newNode.getId(), edge.getTarget(), edge);
        });
        edgesAsTarget.forEach(edge->{
            edge.setTarget(newNode.getId());
            graph.setEdge(edge.getSource(), newNode.getId(), edge);
        });
        newNode.setGraph(graph);
        newNode.setDag(this);
    }

    public void addTargetNode(Node sourceNode, Node targetNode) {
        graph.setNode(targetNode.getId(), targetNode);
        LinkedList<Edge> edges = getEdges();
        if (null != edges) {
            List<Edge> edgesAsSource = edges.stream().filter(edge -> edge.getSource().equals(sourceNode.getId())).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(edgesAsSource)) {
                for (Edge edge : edgesAsSource) {
                    graph.removeEdge(edge.getSource(), edge.getTarget());
                    Edge newEdge = new Edge(targetNode.getId(), edge.getTarget());
                    graph.setEdge(newEdge.getSource(), newEdge.getTarget(), newEdge);
                }
            }
        }
        Edge edge = new Edge(sourceNode.getId(), targetNode.getId());
        graph.setEdge(sourceNode.getId(), targetNode.getId(), edge);
        targetNode.setGraph(graph);
        targetNode.setDag(this);
    }

    public void addSourceNode(Node sourceNode, Node targetNode) {
        graph.setNode(sourceNode.getId(), sourceNode);
        LinkedList<Edge> edges = getEdges();
        if (null != edges) {
            List<Edge> edgeAsTarget = edges.stream().filter(edge -> edge.getTarget().equals(targetNode.getId())).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(edgeAsTarget)) {
                for (Edge edge : edgeAsTarget) {
                    graph.removeEdge(edge.getSource(), edge.getTarget());
                    Edge newEdge = new Edge(edge.getSource(), sourceNode.getId());
                    graph.setEdge(newEdge.getSource(), newEdge.getTarget(), newEdge);
                }
            }
        }
        Edge edge = new Edge(sourceNode.getId(), targetNode.getId());
        graph.setEdge(sourceNode.getId(), targetNode.getId(), edge);
        sourceNode.setGraph(graph);
        sourceNode.setDag(this);
    }
}
