package com.tapdata.tm.lineagegraph.service;

import com.harium.graph.Graph;
import com.harium.graph.Node;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.dataflow.entity.DataFlow;
import com.tapdata.tm.dataflow.service.DataFlowService;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.lineagegraph.bean.SourceDto;
import com.tapdata.tm.lineagegraph.dto.LineageGraphDto;
import com.tapdata.tm.lineagegraph.entity.LineageGraphEntity;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.utils.StageUtil;
import com.tapdata.tm.utils.UUIDUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author: Zed
 * @Date: 2021/10/15
 * @Description:
 */
@Component
@Slf4j
public class LineageMain {
    @Autowired
    private LineageGraphService lineageGraphService;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private DataFlowService dataFlowService;

    @Autowired
    private MetadataInstancesService metadataInstancesService;

    //@Value("${node.pid:}")
    private String pid = UUIDUtil.getUUID();

    @Autowired
    private LineageTable lineageTable;

    private static final String lineageGraphTableName = "LineageGraph";
    private static final String tableLineageProcessor = "tableLineageProcessor";
    private static final Criteria findTableGraphCriteria = Criteria.where("value.type").in("table", "field", "tableEdge", "fieldEdge");

    private int  insertBatchSize = 1000;

    private int step = 0;
    private Graph tableGraph;
    private  int currProgress = 0;
    private  int allProgress = 0;
    private  boolean reading = false;
    private  String version = "";

    public void tableGraphInit() {

    }

    public void findAndFlushTableGraph() {
        int currCount = 0;
        Query query = new Query(findTableGraphCriteria);
        long count = lineageGraphService.count(query);
        long batchSize = Math.round(count / 20.0);

        batchSize = batchSize < 1 ? 1 : batchSize;

        reading = true;

        FindIterable<LineageGraphEntity> findIterable = mongoTemplate.getCollection(lineageGraphTableName)
                .find(findTableGraphCriteria.getCriteriaObject(), LineageGraphEntity.class);
        MongoCursor<LineageGraphEntity> cursor = findIterable.cursor();

        log.info("LINEAGE(" + pid + ") -Start to find and flush table graph, all count: " + count);

        List<LineageGraphDto> nodes = new ArrayList<>();
        List<LineageGraphDto> edges = new ArrayList<>();

        while(cursor.hasNext()) {
            LineageGraphEntity next = cursor.next();
            LineageGraphDto lineageGraphDto = new LineageGraphDto();
            BeanUtils.copyProperties(next, lineageGraphDto);
            currCount++;
            if (currCount % batchSize == 0) {
                log.info("LINEAGE(" + pid + ") -Read table graph progress:"+ currCount + "/" + count);
            }

            if (lineageGraphDto.getValue() ==  null || StringUtils.isBlank(lineageGraphDto.getValue().getType())) {
                continue;
            }

            switch (lineageGraphDto.getValue().getType()) {
                case "table":
                case "field":
                    nodes.add(lineageGraphDto);
                    break;
                case "tableEdge":
                case "fieldEdge":
                    edges.add(lineageGraphDto);
                    break;
                default:
                    break;
            }
        }

        log.info("LINEAGE(" + pid + ") -Read table lineage, node count:" + nodes.size() + ", edge count:", + edges.size());

        Graph graph = GraphUtil.createGraph(true, false, true);
        graph.setEdges(edges);
        graph.setNodes(nodes);
        tableGraph = graph;

        Query query1 = new Query(Criteria.where("type").is(tableLineageProcessor));
        query1.fields().include(version);
        LineageGraphDto lineageGraphDto = lineageGraphService.findOne(query1);

        if (lineageGraphDto != null) {
            version = lineageGraphDto.getVersion() == null ? lineageGraphDto.getVersion() : "";
        }
        log.info("LINEAGE(" + pid + ") -Table graph is ready, node count: " + nodes.size() + ", edge count: " + edges.size() +
                ", current version: " + version);
    }

    /**
     * 分析所有的任务，生成表的全链路图
     * @param firstTimeInit 是否第一次运行
     */
    public boolean initialTableLineage(boolean firstTimeInit) {
        int step = 0;

        if (!firstTimeInit && canStartInitTableLineage()) {
            Query query = new Query(Criteria.where("type").is(tableLineageProcessor));
            LineageGraphDto result = lineageGraphService.findOne(query);
            String msg = "Another process is handle the table lineage graph, ipv4: " + result.getIpv4() + ", pid: " + result.getPid();
            throw new BizException(msg);
        }

        //更新 updateTableLineageStats

        //创建graph对象
        Graph<LineageGraphDto> graph = GraphUtil.createGraph(true, false, true);

        try {
            long allProgress = dataFlowService.count(new Query());
            currProgress = 0;
            long batchSize = Math.round(allProgress / 20.0);
            batchSize = batchSize < 1 ? 1 : batchSize;

            log.info("LINEAGE(" + pid + ") -step" + ++step, "Start to analyze and create lineage graph based on data flows one by one, count: " + allProgress);
            FindIterable<DataFlow> findIterable = mongoTemplate.getCollection("DataFlows").find(DataFlow.class);
            MongoCursor<DataFlow> cursor = findIterable.cursor();

            while (cursor.hasNext()) {
                DataFlow next = cursor.next();
                DataFlowDto dataFlowDto = new DataFlowDto();
                BeanUtils.copyProperties(next, dataFlowDto);
                graph = createTableGraphByDataFlow(dataFlowDto, graph);
                currProgress++;

                if (currProgress % batchSize == 0) {
                    log.info("LINEAGE(" + pid + ") -Analyze table lineage progress:" + currProgress + '/' + allProgress);
                }
            }
        } catch (Exception e) {
            String msg = "Analyze data flow(s) and build graph failed, message: " + e.getMessage() + "\n  " + Arrays.toString(e.getStackTrace());
            throw new BizException(msg);
        }

        //删除旧数据，写入新数据
        try {
            deleteAndWirteGraphData(graph);
            log.info("LINEAGE(" + pid + ") -step" + ++step + "Handle table lineage graph data finished");
        } catch (Exception e) {
            String msg = "Delete and upsert graph data failed, message: " + e.getMessage() + "\n  " + Arrays.toString(e.getStackTrace());
            throw new BizException(msg);
        }
        finishInitTableLineageGraph();

        tableGraph = graph;
        return true;


    }

    private void finishInitTableLineageGraph() {
        version = UUIDUtil.getUUID();

        Criteria criteria = Criteria.where("type").is(tableLineageProcessor);
        Update update = Update.update("finish_date", new Date()).set("status", "finish").set("currProgress", currProgress)
                .set("allProgress", allProgress).set("version", version).unset("message").unset("stack");
        mongoTemplate.getCollection(lineageGraphTableName).updateOne(criteria.getCriteriaObject(), update.getUpdateObject());

        //TODO clear interval

    }

    private void deleteAndWirteGraphData(Graph<LineageGraphDto> graph) {
        long ts = System.currentTimeMillis();

        Criteria criteria = Criteria.where("value.type").in("table", "field", "tableEdge", "fieldEdge");
        long deletedCount = lineageGraphService.deleteAll(new Query(criteria));
        log.info("LINEAGE(" + pid + ") -step" +  ++step + "Delete old table lineage graph data, delete result:" +
                deletedCount + ", spend: " + ((System.currentTimeMillis() - ts) / 1000) + " second");

        // 插入新的图形数据
        List<LineageGraphDto> bulkWrites = new ArrayList();

        log.info("LINEAGE(" + pid + ") -step" + ++step + "Start to write table lineage graph in LineageGraph, node count:" +
               graph.getNodes().size() + ", edge count:" + graph.getEdges().size());

        if (CollectionUtils.isNotEmpty(graph.getNodes())) {
            for (Node<LineageGraphDto> node : graph.getNodes()) {
                LineageGraphDto data = node.getData();
                //TODO 这一块代码原来就还有很多问题，暂时不实现了
            }
        }



    }

    private Graph createTableGraphByDataFlow(DataFlowDto dataFlowDto, Graph graph) {
        if (dataFlowDto == null || dataFlowDto.getId() == null || CollectionUtils.isEmpty(dataFlowDto.getStages())) {
            return graph;
        }

        List<Map<String, Object>> newStages = new ArrayList<>();

        List<Map<String, Object>> stages = dataFlowDto.getStages();
        for (Map<String, Object> stage : stages) {
            Map<String, Object> newStage = StageUtil.projectStageField(stage);
            newStages.add(newStage);
        }

        fillInConn(newStages);
        dataFlowDto.setStages(newStages);
        lineageTable.buildGraph(dataFlowDto, graph);
        return graph;
    }

    private void fillInConn(List<Map<String, Object>> stages) {
        Set<String> connectionIds = stages.stream().filter(s -> StringUtils.isNotBlank((String) s.get("connectionId")))
                .map(s -> (String) s.get("connectionId"))
                .collect(Collectors.toSet());
        Criteria criteria = Criteria.where("_id").in(connectionIds).and("schema").is(false);
        List<DataSourceEntity> connections = mongoTemplate.find(new Query(criteria), DataSourceEntity.class, "Connections");
        for (DataSourceEntity connection : connections) {
            String id = connection.getId().toHexString();
            for (Map<String, Object> stage : stages) {
                String connectionId = (String) stage.get("connectionId");
                if (connectionId.equals(id)) {
                    stage.put("connection", projectConnection(connection));
                }
            }
        }

    }

    private SourceDto projectConnection(DataSourceEntity connection) {
        if (connection == null) {
            return null;
        }

        SourceDto sourceDto = new SourceDto();
        sourceDto.setDatabase_type(connection.getDatabase_type());
        sourceDto.setDatabase_uri(connection.getDatabase_uri());
        sourceDto.setDatabase_host(connection.getDatabase_host());
        sourceDto.setDatabase_port(connection.getDatabase_port());
        sourceDto.setDatabase_name(connection.getDatabase_name());
        sourceDto.setBasePath(connection.getBasePath());
        sourceDto.setPath(connection.getPath());
        sourceDto.setApiVersion(connection.getApiVersion());
        sourceDto.setName(connection.getName());
        sourceDto.setId(connection.getId().toHexString());
        return sourceDto;
    }


    public boolean canStartInitTableLineage() {
        Criteria typeCriteria = Criteria.where("type").is(tableLineageProcessor);
        Criteria statusCriteria = Criteria.where("status").ne("running");
        Criteria pingTime1Criteria = Criteria.where("ping_time").lt(System.currentTimeMillis() - (60 * 1000));
        Criteria pingTime2Criteria = Criteria.where("ping_time").is(null);
        Criteria pingTime3Criteria = Criteria.where("ping_time").exists(false);
        Criteria criteria = typeCriteria.orOperator(statusCriteria, pingTime1Criteria, pingTime2Criteria, pingTime3Criteria);

        Update update = Update.update("status", "running").set("start_date", new Date()).set("ping_time", System.currentTimeMillis())
                .set("pid", pid).set("ipv4", getRealIP()).set("currProgress", 0).unset("finish_date");

        Document updateObject = update.getUpdateObject();


        UpdateResult updateResult = mongoTemplate.getCollection(lineageGraphTableName).updateOne(criteria.getCriteriaObject(), updateObject);
        return updateResult.getModifiedCount() > 0;
    }

    public static String getRealIP() {
        try {
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface
                    .getNetworkInterfaces();
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = (NetworkInterface) allNetInterfaces
                        .nextElement();

                // 去除回环接口，子接口，未运行和接口
                if (netInterface.isLoopback() || netInterface.isVirtual()
                        || !netInterface.isUp()) {
                    continue;
                }

                if (!netInterface.getDisplayName().contains("Intel")
                        && !netInterface.getDisplayName().contains("Realtek")) {
                    continue;
                }
                Enumeration<InetAddress> addresses = netInterface
                        .getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress ip = addresses.nextElement();
                    if (ip != null) {
                        // ipv4
                        if (ip instanceof Inet4Address) {
                            return ip.getHostAddress();
                        }
                    }
                }
                break;
            }
        } catch (SocketException e) {
            return null;
        }
        return null;
    }

    public void updateTableLineageStats() {
        Criteria criteria = Criteria.where("type").is(tableLineageProcessor);
        Update set = Update.update("ping_time", System.currentTimeMillis()).set("currProgress", currProgress).set("allProgress", allProgress);
        mongoTemplate.getCollection(lineageGraphTableName).updateOne(criteria.getCriteriaObject(), set.getUpdateObject());
    }


    public void errorHandler(Exception e) {
        String stack = Arrays.toString(e.getStackTrace());
        log.error(stack);
        Criteria criteria = Criteria.where("type").is(tableLineageProcessor);
        Update set = Update.update("finish_date", System.currentTimeMillis()).set("status", "error").set("message", e.getMessage()).set("stack", stack);
        mongoTemplate.getCollection(lineageGraphTableName).updateOne(criteria.getCriteriaObject(), set.getUpdateObject());
    }

    public Graph getTableGraph(String graphId) {
        if (graphId == null) {
            log.info("Graph id cannot be empty");
            throw new BizException("Graph id cannot be empty");
        }

        MetadataInstancesDto metaData = metadataInstancesService.findOne(new Query(Criteria.where("qualified_name").is(graphId)));
        return GraphUtil.getGraphByNode(tableGraph, graphId, metaData);
    }


    public Graph getFieldGraph(String graphId, List<String> fieldGraphIds) {
        if (graphId == null || CollectionUtils.isEmpty(fieldGraphIds)) {
            log.info("Graph id or field graph id cannot be empty");
            throw new BizException("Graph id or field graph id cannot be empty");
        }

        MetadataInstancesDto metaData = metadataInstancesService.findOne(new Query(Criteria.where("qualified_name").is(graphId)));
        return GraphUtil.getGraphByNode(tableGraph, graphId, metaData, fieldGraphIds);
    }

}
