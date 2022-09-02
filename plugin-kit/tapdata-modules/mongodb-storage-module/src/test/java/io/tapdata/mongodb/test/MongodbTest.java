package io.tapdata.mongodb.test;

import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.modules.api.net.entity.NodeHealth;
import io.tapdata.modules.api.net.entity.NodeRegistry;
import io.tapdata.modules.api.net.service.NodeHealthService;
import io.tapdata.modules.api.net.service.NodeRegistryService;
import io.tapdata.pdk.core.runtime.TapRuntime;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.*;

public class MongodbTest {
	@BeforeEach
	public void before() {
		CommonUtils.setProperty("TAPDATA_MONGO_URI", "mongodb://127.0.0.1:27017/tapdata?authSource=admin");
		TapRuntime.getInstance();
	}

	@Test
	@Disabled
	public void test() {
		NodeRegistryService nodeRegistryService = InstanceFactory.bean(NodeRegistryService.class);
		NodeRegistry nodeRegistry = new NodeRegistry().ip("localhost").httpPort(8080).wsPort(5000).type("proxy").time(System.currentTimeMillis());
		nodeRegistryService.save(nodeRegistry);

		NodeRegistry newNodeRegistry = nodeRegistryService.get("localhost:8080");
		assertNotNull(newNodeRegistry);
		assertEquals(8080, newNodeRegistry.getHttpPort());
		assertEquals(5000, newNodeRegistry.getWsPort());
		assertEquals("localhost", newNodeRegistry.getIp());

		nodeRegistryService.delete("localhost:8080");
		newNodeRegistry = nodeRegistryService.get("localhost:8080");
		assertNull(newNodeRegistry);

		NodeHealthService nodeHealthService = InstanceFactory.bean(NodeHealthService.class);
		NodeHealth nodeHealth0 = new NodeHealth().id("id1").health(100).time(System.currentTimeMillis());
		NodeHealth nodeHealth1 = new NodeHealth().id("id2").health(10).time(System.currentTimeMillis());
		NodeHealth nodeHealth6 = new NodeHealth().id("id6").health(10).time(System.currentTimeMillis());
		NodeHealth nodeHealth2 = new NodeHealth().id("id3").health(-1).time(System.currentTimeMillis());

		nodeHealthService.save(nodeHealth0);

		NodeHealth nodeHealth = nodeHealthService.get("id1");
		assertNotNull(nodeHealth);
		assertEquals("id1", nodeHealth.getId());
		assertEquals(100, nodeHealth.getHealth());
		assertNotNull(nodeHealth.getTime());

		nodeHealthService.save(nodeHealth1);
		nodeHealthService.save(nodeHealth2);
		nodeHealthService.save(nodeHealth6);

		Collection<NodeHealth> nodeHealths = nodeHealthService.getHealthNodes();
		assertNotNull(nodeHealths);
		assertEquals(3, nodeHealths.size());
		assertEquals(100, nodeHealths.stream().findFirst().get().getHealth());

		nodeHealthService.delete("id1");
		nodeHealthService.delete("id2");
		nodeHealthService.delete("id3");
		nodeHealthService.delete("id6");

		nodeHealth = nodeHealthService.get("id1");
		assertNull(nodeHealth);
		nodeHealth = nodeHealthService.get("id2");
		assertNull(nodeHealth);
		nodeHealth = nodeHealthService.get("id3");
		assertNull(nodeHealth);
	}




}
