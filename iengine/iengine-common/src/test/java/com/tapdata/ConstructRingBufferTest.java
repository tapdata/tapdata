package com.tapdata;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.entity.hazelcast.HZLoggingType;
import io.tapdata.construct.ConstructIterator;
import io.tapdata.construct.HazelcastConstruct;
import io.tapdata.construct.constructImpl.ConstructRingBuffer;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2022-02-18 20:48
 **/
public class ConstructRingBufferTest {

	private static Config config;
	private static HazelcastInstance hazelcastInstance;
	private static HazelcastConstruct<Document> hazelcastConstruct;

	@Before
	public void init() {
		String instanceName = "unit-test-" + System.currentTimeMillis();
		config = HazelcastUtil.getConfig(instanceName, HZLoggingType.NONE);
		hazelcastInstance = Hazelcast.newHazelcastInstance(config);
		hazelcastConstruct = new ConstructRingBuffer<>(hazelcastInstance, "unit-test-ringbuffer");
	}

	@After
	public void after() throws Exception {
		if (hazelcastConstruct != null) {
			hazelcastConstruct.destroy();
		}
		if (hazelcastInstance != null) {
			hazelcastInstance.shutdown();
		}
	}

	@Test
	public void testInsert() throws Exception {
		int num = 100;
		for (int i = 0; i < num; i++) {
			hazelcastConstruct.insert(mockDocument());
		}
		long headSequence = ((ConstructRingBuffer) hazelcastConstruct).getRingbuffer().headSequence();
		long tailSequence = ((ConstructRingBuffer<Document>) hazelcastConstruct).getRingbuffer().tailSequence();
		Assert.assertEquals(tailSequence - headSequence + 1, num);
	}

	@Test
	public void testFindNext() throws Exception {
		int num = 100;
		for (int i = 0; i < num; i++) {
			hazelcastConstruct.insert(mockDocument());
		}
		ConstructIterator<Document> documentConstructIterator = hazelcastConstruct.find(new HashMap<String, Object>() {{
			put(ConstructRingBuffer.SEQUENCE_KEY, 0);
		}});
		List<Document> list = new ArrayList<>();
		while (documentConstructIterator.hasNext()) {
			Document row = documentConstructIterator.next();
			list.add(row);
		}
		Assert.assertEquals(num, list.size());
		Assert.assertEquals(num, documentConstructIterator.getSequence());
	}

	@Test
	public void testFindPeek() throws Exception {
		int num = 5;
		for (int i = 0; i < num; i++) {
			hazelcastConstruct.insert(mockDocument());
		}
		ConstructIterator<Document> iterator = hazelcastConstruct.find();
		Document row = null;
		if (iterator.hasNext()) {
			row = iterator.peek();
		}
		Assert.assertTrue(row != null);
		Assert.assertEquals(0, iterator.getSequence());
	}

	private Document mockDocument() {
		return new Document("name", RandomStringUtils.randomAlphabetic(10))
				.append("insertTs", System.currentTimeMillis())
				.append("status", RandomUtils.nextInt(0, 3))
				.append("isActive", RandomUtils.nextBoolean());
	}
}
