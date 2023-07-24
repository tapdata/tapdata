package dynamicadjustmemory;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory.DynamicAdjustMemoryBaseImpl;
import io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory.DynamicAdjustMemoryContext;
import io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory.DynamicAdjustMemoryService;
import io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory.impl.DynamicAdjustMemoryImpl;
import org.apache.lucene.util.RamUsageEstimator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * @author samuel
 * @Description
 * @create 2023-07-20 14:47
 **/
public class DynamicAdjustMemoryTest {
	private static DynamicAdjustMemoryService dynamicAdjustMemoryService;

	@BeforeClass
	public static void init() {
		dynamicAdjustMemoryService = new DynamicAdjustMemoryImpl();
	}

	@Test
	public void randomSampleListTest() throws Exception {
		List<TapdataEvent> events = new ArrayList<>();
		IntStream.range(0, 300).forEach(i -> events.add(new TapdataEvent()));
		Method method = dynamicAdjustMemoryService.getClass().getSuperclass().getDeclaredMethod("randomSampleList", List.class, Double.class);
		Assert.assertNotNull(method);
		method.setAccessible(true);
		double sampleRate = 0.1D;
		Object invokeResult = method.invoke(dynamicAdjustMemoryService, events, sampleRate);
		assert invokeResult instanceof List;
		List<?> sampleList = (List<?>) invokeResult;
		int expectSize = (int) (events.size() * sampleRate);
		Assert.assertEquals(expectSize, sampleList.size());
		events.clear();
		IntStream.range(0, 10000).forEach(i -> events.add(new TapdataEvent()));
		invokeResult = method.invoke(dynamicAdjustMemoryService, events, sampleRate);
		assert invokeResult instanceof List;
		sampleList = (List<?>) invokeResult;
		expectSize = DynamicAdjustMemoryBaseImpl.MAX_SAMPLE_SIZE;
		Assert.assertEquals(expectSize, sampleList.size());
		events.clear();
		events.add(new TapdataEvent());
		invokeResult = method.invoke(dynamicAdjustMemoryService, events, sampleRate);
		assert invokeResult instanceof List;
		sampleList = (List<?>) invokeResult;
		expectSize = events.size();
		Assert.assertEquals(expectSize, sampleList.size());
	}

	@Test
	public void calcQueueSizeTest() {
		DynamicAdjustMemoryContext dynamicAdjustMemoryContext = DynamicAdjustMemoryContext.create().sampleRate(0.1D);
		List<TapdataEvent> events = new ArrayList<>();
		IntStream.range(0,300).forEach(i->events.add(new TapdataEvent()));
		long ramThreshold = 30 * 1024L;
		int originalQueueSize = 2000;
		int newQueueSize = dynamicAdjustMemoryService.calcQueueSize(dynamicAdjustMemoryContext, events, ramThreshold, originalQueueSize);
		Assert.assertEquals(originalQueueSize, newQueueSize);
		events.clear();
		IntStream.range(0, 300).forEach(i->{
			Map<String, Object> after = new HashMap<>();
			IntStream.range(0, 100).forEach(i1 -> after.put("key" + i1, new TapStringValue("value" + i1)));
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
			tapInsertRecordEvent.setAfter(after);
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			events.add(tapdataEvent);
		});
		newQueueSize = dynamicAdjustMemoryService.calcQueueSize(dynamicAdjustMemoryContext, events, ramThreshold, originalQueueSize);
		Assert.assertEquals(1000, newQueueSize);
	}
}
