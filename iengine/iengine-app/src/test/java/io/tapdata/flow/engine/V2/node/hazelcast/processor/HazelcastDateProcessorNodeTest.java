package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

/**
 * @author samuel
 * @Description
 * @create 2024-05-18 17:45
 **/
@DisplayName("Class HazelcastDateProcessorNode Test")
class HazelcastDateProcessorNodeTest {

	private HazelcastDateProcessorNode hazelcastDateProcessorNode;

	@BeforeEach
	void setUp() {
		hazelcastDateProcessorNode = mock(HazelcastDateProcessorNode.class);
	}
}