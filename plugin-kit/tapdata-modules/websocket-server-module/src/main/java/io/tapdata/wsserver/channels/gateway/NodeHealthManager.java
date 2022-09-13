package io.tapdata.wsserver.channels.gateway;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.error.CoreException;
import io.tapdata.modules.api.net.entity.NodeHealth;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.service.NodeHealthService;
import io.tapdata.pdk.core.utils.CommonUtils;

@Bean
public class NodeHealthManager {
	private int checkHealthPeriodSeconds = 10;
	private String nodeId;
	@Bean
	private NodeHealthService nodeHealthService;

	public void start() {
		checkHealthPeriodSeconds = CommonUtils.getPropertyInt("tapdata_check_health_period_seconds", 10);
		nodeId = CommonUtils.getProperty("tapdata_node_id");
		if(nodeId == null)
			throw new CoreException(NetErrors.CURRENT_NODE_ID_NOT_FOUND, "Current nodeId for NodeHealthManager#start not found");

		nodeHealthService.save(new NodeHealth().id(nodeId).time(System.currentTimeMillis()).health(0));
	}
}
