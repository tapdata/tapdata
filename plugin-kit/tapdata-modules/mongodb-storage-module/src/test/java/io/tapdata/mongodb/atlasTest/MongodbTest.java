package io.tapdata.mongodb.atlasTest;

import com.google.common.collect.Sets;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.modules.api.net.entity.NodeHealth;
import io.tapdata.modules.api.net.entity.NodeRegistry;
import io.tapdata.modules.api.net.entity.ProxySubscription;
import io.tapdata.modules.api.net.message.MessageEntity;
import io.tapdata.modules.api.net.service.MessageEntityService;
import io.tapdata.modules.api.net.service.ProxySubscriptionService;
import io.tapdata.modules.api.net.service.node.NodeHealthService;
import io.tapdata.modules.api.net.service.node.NodeRegistryService;
import io.tapdata.modules.api.proxy.data.FetchNewDataResult;
import io.tapdata.pdk.core.runtime.TapRuntime;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;
import static org.junit.jupiter.api.Assertions.*;

public class MongodbTest {
	@BeforeEach
	public void before() {
		CommonUtils.setProperty("tapdata_proxy_mongodb_uri", "mongodb://127.0.0.1:27017/tapdata?authSource=admin");
//		CommonUtils.setProperty("tapdata_proxy_mongodb_uri", "mongodb://139.198.127.204:30506/tapdata-tests?authSource=admin");
		TapRuntime.getInstance();
	}

	@Test
	@Disabled
	public void test() {
		NodeRegistryService nodeRegistryService = InstanceFactory.bean(NodeRegistryService.class);
		NodeRegistry nodeRegistry = new NodeRegistry().ip("localhost").httpPort(8080).wsPort(5000).type("proxy").time(System.currentTimeMillis());
		nodeRegistryService.save(nodeRegistry);

		NodeRegistry newNodeRegistry = nodeRegistryService.get(nodeRegistry.id());
		assertNotNull(newNodeRegistry);
		assertEquals(8080, newNodeRegistry.getHttpPort());
		assertEquals(5000, newNodeRegistry.getWsPort());
		assertEquals("localhost", newNodeRegistry.getIps().get(0));

		nodeRegistry = new NodeRegistry().ip("localhost").httpPort(8080).wsPort(8246).type("proxy").time(System.currentTimeMillis());
		nodeRegistryService.save(nodeRegistry);

		newNodeRegistry = nodeRegistryService.get(nodeRegistry.id());
		assertEquals(8246, newNodeRegistry.getWsPort());

		nodeRegistryService.delete(nodeRegistry.id());
		newNodeRegistry = nodeRegistryService.get(nodeRegistry.id());
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

		MessageEntityService messageEntityService = InstanceFactory.bean(MessageEntityService.class);
		messageEntityService.remove("abc", "aaaa");

		MessageEntity message = new MessageEntity().service("abc").subscribeId("aaaa").content(map(entry("aa", 1))).time(new Date(12343254234L));
		MessageEntity message1 = new MessageEntity().service("abc").subscribeId("aaaa").content(map(entry("aa", 2))).time(new Date());
		MessageEntity message2 = new MessageEntity().service("abc").subscribeId("aaaa").content(map(entry("aa", 3))).time(new Date());
		MessageEntity message3 = new MessageEntity().service("abc").subscribeId("aaaa").content(map(entry("aa", 4))).time(new Date());
		MessageEntity message4 = new MessageEntity().service("abc").subscribeId("aaaa").content(map(entry("aa", 5))).time(new Date());
		messageEntityService.save(message);
		messageEntityService.save(message1);
		messageEntityService.save(message2);
		messageEntityService.save(message3);
		messageEntityService.save(message4);

		String offset = messageEntityService.getOffsetByTimestamp(0L);
		assertNull(offset);
		String offset1 = messageEntityService.getOffsetByTimestamp(12343254234L);
		assertNotNull(offset1);
		FetchNewDataResult result = messageEntityService.getMessageEntityList("abc", "aaaa", offset1, 2);
		assertNotNull(result.getOffset());
		assertNotNull(result.getMessages());
		assertEquals(2, result.getMessages().size());
		assertEquals(2, result.getMessages().get(0).getContent().get("aa"));
		assertEquals(3, result.getMessages().get(1).getContent().get("aa"));
		result = messageEntityService.getMessageEntityList("abc", "aaaa", result.getOffset(), 10);
		assertNotNull(result.getOffset());
		assertNotNull(result.getMessages());
		assertEquals(2, result.getMessages().size());
		assertEquals(4, result.getMessages().get(0).getContent().get("aa"));
		assertEquals(5, result.getMessages().get(1).getContent().get("aa"));

		ProxySubscriptionService proxySubscriptionService = InstanceFactory.instance(ProxySubscriptionService.class);
		assertNotNull(proxySubscriptionService);
		ProxySubscription proxySubscription = new ProxySubscription().service("engine").subscribeIds(Sets.newHashSet("a", "b", "c")).nodeId("n1").time(123L);
		ProxySubscription proxySubscription1 = new ProxySubscription().service("engine").subscribeIds(Sets.newHashSet("b", "d")).nodeId("n2").time(234L);
		ProxySubscription proxySubscription2 = new ProxySubscription().service("engine").subscribeIds(Sets.newHashSet("e", "f")).nodeId("n3").time(345L);
		proxySubscriptionService.syncProxySubscription(proxySubscription);
		proxySubscriptionService.syncProxySubscription(proxySubscription1);
		proxySubscriptionService.syncProxySubscription(proxySubscription2);

		ProxySubscription proxySubscriptionToVerify = proxySubscriptionService.get("n1");
		assertNotNull(proxySubscriptionToVerify);
		assertEquals("n1", proxySubscriptionToVerify.getNodeId());
		assertEquals(123L, proxySubscriptionToVerify.getTime());
		ProxySubscription proxySubscriptionToVerify1 = proxySubscriptionService.get("n2");
		assertNotNull(proxySubscriptionToVerify1);
		assertEquals("n2", proxySubscriptionToVerify1.getNodeId());
		assertEquals(234L, proxySubscriptionToVerify1.getTime());
		ProxySubscription proxySubscriptionToVerify2 = proxySubscriptionService.get("n3");
		assertNotNull(proxySubscriptionToVerify2);
		assertEquals("n3", proxySubscriptionToVerify2.getNodeId());
		assertEquals(345L, proxySubscriptionToVerify2.getTime());

		List<String> nodeIds = proxySubscriptionService.subscribedNodeIdsByAnyOne("engine", Collections.singleton("b"));
		assertNotNull(nodeIds);
		assertEquals(2, nodeIds.size());
		nodeIds.removeAll(Sets.newHashSet("n1", "n2"));
		assertEquals(0, nodeIds.size());

		nodeIds = proxySubscriptionService.subscribedNodeIdsByAnyOne("engine", Collections.singleton("g"));
		assertNotNull(nodeIds);
		assertEquals(0, nodeIds.size());

		nodeIds = proxySubscriptionService.subscribedNodeIdsByAnyOne("engine", Collections.singleton("f"));
		assertNotNull(nodeIds);
		assertEquals(1, nodeIds.size());
		nodeIds.removeAll(Sets.newHashSet("n3"));
		assertEquals(0, nodeIds.size());

		nodeIds = proxySubscriptionService.subscribedNodeIdsByAnyOne("engine", Sets.newHashSet("b", "a"));
		assertNotNull(nodeIds);
		assertEquals(2, nodeIds.size());
		nodeIds.removeAll(Sets.newHashSet( "n1", "n2"));
		assertEquals(0, nodeIds.size());

		nodeIds = proxySubscriptionService.subscribedNodeIdsByAll("engine", Sets.newHashSet("b", "a"));
		assertNotNull(nodeIds);
		assertEquals(1, nodeIds.size());
		nodeIds.removeAll(Sets.newHashSet( "n1"));
		assertEquals(0, nodeIds.size());

		nodeIds = proxySubscriptionService.subscribedNodeIdsByAll("engine", Sets.newHashSet("b", "a", "c"));
		assertNotNull(nodeIds);
		assertEquals(1, nodeIds.size());
		nodeIds.removeAll(Sets.newHashSet( "n1"));
		assertEquals(0, nodeIds.size());

		nodeIds = proxySubscriptionService.subscribedNodeIdsByAll("engine", Sets.newHashSet("b", "f"));
		assertNotNull(nodeIds);
		assertEquals(0, nodeIds.size());

		nodeIds = proxySubscriptionService.subscribedNodeIdsByAll("engine", Sets.newHashSet("c", "f"));
		assertNotNull(nodeIds);
		assertEquals(0, nodeIds.size());

		nodeIds = proxySubscriptionService.subscribedNodeIdsByAll("engine", Sets.newHashSet("a", "f1323"));
		assertNotNull(nodeIds);
		assertEquals(0, nodeIds.size());

		nodeIds = proxySubscriptionService.subscribedNodeIdsByAll("engine", Sets.newHashSet("f1323"));
		assertNotNull(nodeIds);
		assertEquals(0, nodeIds.size());

		boolean bool = proxySubscriptionService.delete("n1", 123123L);
		assertFalse(bool);
		assertNotNull(proxySubscriptionService.get("n1"));

		bool = proxySubscriptionService.delete("n1", 123L);
		assertTrue(bool);
		assertNull(proxySubscriptionService.get("n1"));

		bool = proxySubscriptionService.delete("n2");
		assertTrue(bool);
		assertNull(proxySubscriptionService.get("n2"));

		bool = proxySubscriptionService.delete("n3");
		assertTrue(bool);
		assertNull(proxySubscriptionService.get("n3"));
	}

}
