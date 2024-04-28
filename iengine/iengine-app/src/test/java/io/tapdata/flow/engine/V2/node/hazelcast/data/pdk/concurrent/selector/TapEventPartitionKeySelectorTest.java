package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.selector;

import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.schema.value.TapValue;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author samuel
 * @Description
 * @create 2024-04-28 18:32
 **/
@DisplayName("Class TapEventPartitionKeySelector Test")
class TapEventPartitionKeySelectorTest {
	@Nested
	@DisplayName("Method select Test")
	class selectTest {
		@Test
		@DisplayName("test main process")
		void testMainProcess() {
			TapEventPartitionKeySelector keySelector = new TapEventPartitionKeySelector(tapEvent -> {
				List<String> list = new ArrayList<>();
				list.add("_id");
				return list;
			});

			ObjectId oid = new ObjectId();
			TapValue<String, TapString> _idTapStringValue = new TapStringValue(oid.toHexString())
					.originValue(oid)
					.tapType(new TapString());

			Map<String, Object> before = new HashMap<>();
			before.put("_id", _idTapStringValue);
			before.put("name", "test");

			Map<String, Object> after = new HashMap<>();
			after.put("_id", _idTapStringValue);
			after.put("name", "test1");

			TapUpdateRecordEvent tapEvent = TapUpdateRecordEvent.create().init()
					.before(before)
					.after(after);

			List<Object> result = keySelector.select(tapEvent, after);

			assertEquals(1, result.size());
			assertEquals(oid.toHexString(), result.get(0));
		}
	}
}