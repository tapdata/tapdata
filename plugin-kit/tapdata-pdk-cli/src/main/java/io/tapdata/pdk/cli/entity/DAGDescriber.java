package io.tapdata.pdk.cli.entity;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.core.workflow.engine.JobOptions;
import io.tapdata.pdk.core.workflow.engine.TapDAGNodeEx;
import io.tapdata.pdk.core.workflow.engine.TapDAG;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class DAGDescriber {
    private static final String TAG = DAGDescriber.class.getSimpleName();

    private String id;
    private List<TapDAGNodeEx> nodes;
    private JobOptions jobOptions;
    private List<List<String>> dag;

    public JobOptions getJobOptions() {
        return jobOptions;
    }

    public void setJobOptions(JobOptions jobOptions) {
        this.jobOptions = jobOptions;
    }

    public List<TapDAGNodeEx> getNodes() {
        return nodes;
    }

    public void setNodes(List<TapDAGNodeEx> nodes) {
        this.nodes = nodes;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<List<String>> getDag() {
        return dag;
    }

    public void setDag(List<List<String>> dag) {
        this.dag = dag;
    }

    public TapDAG toDag() {
        if(nodes != null) {
            TapDAG dagWithWorker = new TapDAG();
            if(id == null) {
                TapLogger.error(TAG, "Missing dag id while generating DAG");
                return null;
            }
            dagWithWorker.setId(id);
            List<String> headNodeIds = new CopyOnWriteArrayList<>();
            dagWithWorker.setHeadNodeIds(headNodeIds);
            Map<String, TapDAGNodeEx> nodeMap = new ConcurrentHashMap<>();
            dagWithWorker.setNodeMap(nodeMap);
            //Put into a map
            for(TapDAGNodeEx node : nodes) {
                if(node == null) continue;
                String result = node.verify();
                if(result != null) {
                    TapLogger.warn(TAG, "Node verify failed, {} node json {}", result, JSON.toJSONString(node));
                    continue;
                }
                TapDAGNodeEx old = nodeMap.put(node.getId(), node);
                if(old != null) {
                    TapLogger.warn(TAG, "Node id {} is duplicated, node is replaced, removed node json {}", node.getId(), JSON.toJSONString(old));
                }
            }

            //Repair child and parent
            if(this.dag != null) {
                for(List<String> dagLine : this.dag) {
                    if(dagLine.size() != 2) {
                        TapLogger.warn(TAG, "DagLine is illegal {}", Arrays.toString(dagLine.toArray()));
                        continue;
                    }
                    TapDAGNodeEx startNode = nodeMap.getOrDefault(dagLine.get(0), null);
                    TapDAGNodeEx endNode = nodeMap.getOrDefault(dagLine.get(1), null);
                    if (startNode == null || endNode == null) {
                        String startNodeId = startNode == null ? "null" : startNode.getId();
                        String endNodeId = endNode == null ? "null" : endNode.getId();
                        TapLogger.error(TAG, "Node in DAG is not found from {} to {}. [ {} -> {}]", dagLine.get(0), dagLine.get(1), startNodeId, endNodeId);

                        return null;
                    }
                    List<String> childNodeIds = startNode.getChildNodeIds();
                    if(childNodeIds == null) {
                        childNodeIds = new ArrayList<>();
                        startNode.setChildNodeIds(childNodeIds);
                    }
                    if(!childNodeIds.contains(endNode.getId())) {
                        childNodeIds.add(endNode.getId());
                    }

                    List<String> parentNodeIds = endNode.getParentNodeIds();
                    if(parentNodeIds == null) {
                        parentNodeIds = new ArrayList<>();
                        endNode.setParentNodeIds(parentNodeIds);
                    }
                    if(!parentNodeIds.contains(startNode.getId())) {
                        parentNodeIds.add(startNode.getId());
                    }
                }
            }

            //Prepare head node id list
            for(TapDAGNodeEx node : nodes) {
                if(node.getParentNodeIds() == null || node.getParentNodeIds().isEmpty()) {
                    if(node.getChildNodeIds() != null && !node.getChildNodeIds().isEmpty()) {
                        if(!headNodeIds.contains(node.getId()))
                            headNodeIds.add(node.getId());
                    } else {
                        TapLogger.warn(TAG, "Node in DAG don't have parent node ids or child node ids, the node {} will be ignored", node.getId());
                        nodeMap.remove(node.getId());
                    }
                }
            }
            if(dagWithWorker.getNodeMap().isEmpty() || dagWithWorker.getHeadNodeIds().isEmpty()) {
                TapLogger.error(TAG, "Generated dag don't have node map or head node ids, dag json {}", JSON.toJSONString(dagWithWorker));
            } else {
                TapLogger.info(TAG, "DAG tree: {}", dagWithWorker.dagString());
                return dagWithWorker;
            }
        }
        return null;
    }
}
