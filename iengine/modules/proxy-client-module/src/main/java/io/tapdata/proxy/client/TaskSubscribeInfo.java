package io.tapdata.proxy.client;

import io.tapdata.pdk.core.api.Node;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class TaskSubscribeInfo {
	SubscriptionAspectTask subscriptionAspectTask;
	String taskId;
	Map<String, List<Node>> typeConnectionIdPDKNodeMap = new ConcurrentHashMap<>();
	Map<String, Node> nodeIdPDKNodeMap = new ConcurrentHashMap<>();
	Map<String, String> nodeIdTypeConnectionIdMap = new ConcurrentHashMap<>();
}



