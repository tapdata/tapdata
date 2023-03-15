package io.tapdata.supervisor;

import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.core.api.Node;
import io.tapdata.proxy.client.SubscriptionAspectTask;
import io.tapdata.supervisor.report.Report;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class TaskNodeInfo implements MemoryFetcher {
	SupervisorAspectTask supervisorAspectTask;
	String taskId;
	Map<String, List<Node>> typeConnectionIdPDKNodeMap = new ConcurrentHashMap<>();
	Map<String, Node> nodeIdPDKNodeMap = new ConcurrentHashMap<>();
	Map<String, String> nodeIdTypeConnectionIdMap = new ConcurrentHashMap<>();

	@Override
	public DataMap memory(String keyRegex, String memoryLevel) {
		DataMap dataMap = DataMap.create().keyRegex(keyRegex)/*.prefix(TaskSubscribeInfo.class.getSimpleName())*/
				.kv("taskId", taskId)
				;
		DataMap typeConnectionIdPDKNodeMap = DataMap.create().keyRegex(keyRegex)/*.prefix(TaskSubscribeInfo.class.getSimpleName())*/;
		dataMap.kv("typeConnectionIdPDKNodeMap", typeConnectionIdPDKNodeMap);
		for(Map.Entry<String, List<Node>> entry : this.typeConnectionIdPDKNodeMap.entrySet()) {
			List<String> nodeList = new ArrayList<>();
			typeConnectionIdPDKNodeMap.kv(entry.getKey(), nodeList);
			List<Node> nodes = entry.getValue();
			if(nodes != null && !nodes.isEmpty()) {
				for(Node node : nodes) {
					nodeList.add(node.toString());
				}
			}
		}

		//TODO not finished
		return dataMap;
	}
}



