package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.entity.TapdataShareLogEvent;
import com.tapdata.entity.sharecdc.LogContent;
import io.tapdata.common.sharecdc.ShareCdcUtil;
import io.tapdata.construct.HazelcastConstruct;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import lombok.SneakyThrows;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-01-31 12:07
 **/
@DisplayName("HazelcastTargetPdkShareCDCNode Class Test")
class HazelcastTargetPdkShareCDCNodeTest {

	private HazelcastTargetPdkShareCDCNode hazelcastTargetPdkShareCDCNode;

	@BeforeEach
	void setUp() {
		hazelcastTargetPdkShareCDCNode = mock(HazelcastTargetPdkShareCDCNode.class);
	}

}
