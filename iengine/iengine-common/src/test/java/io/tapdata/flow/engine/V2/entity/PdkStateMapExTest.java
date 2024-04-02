package io.tapdata.flow.engine.V2.entity;

import com.hazelcast.map.IMap;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import io.tapdata.construct.constructImpl.DocumentIMap;
import lombok.SneakyThrows;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-04-02 10:44
 **/
class PdkStateMapExTest {

	private PdkStateMapEx pdkStateMapEx;
	private TableNode tableNode;
	private Map<String, Document> map;
	private Document value;

	@BeforeEach
	@SneakyThrows
	void setUp() {
		tableNode = new TableNode();
		tableNode.setId("table_node");
		pdkStateMapEx = mock(PdkStateMapEx.class);
		map = new HashMap<>();
		DocumentIMap<Document> constructIMap = mock(DocumentIMap.class);
		when(constructIMap.find(anyString())).thenAnswer(invocationOnMock -> map.get(invocationOnMock.getArgument(0)));
		when(constructIMap.insert(anyString(), any(Document.class))).thenAnswer(invocationOnMock -> {
			map.put(invocationOnMock.getArgument(0), invocationOnMock.getArgument(1));
			return 1;
		});
		IMap iMap = mock(IMap.class);
		when(iMap.putIfAbsent(anyString(), any(Document.class))).thenAnswer(invocationOnMock -> map.putIfAbsent(invocationOnMock.getArgument(0), invocationOnMock.getArgument(1)));
		when(constructIMap.getiMap()).thenReturn(iMap);

		ReflectionTestUtils.setField(pdkStateMapEx, "constructIMap", constructIMap);
		doCallRealMethod().when(pdkStateMapEx).put(anyString(), any(Document.class));
		doCallRealMethod().when(pdkStateMapEx).putIfAbsent(anyString(), any(Document.class));
		doCallRealMethod().when(pdkStateMapEx).get(anyString());

		List<Object> list = new ArrayList<>();
		list.add(new Document("$c.csv", 1).append("d", new Document("$dd.csv", 1)));
		list.add("test");
		value = new Document("$a.csv", 1).append("b", new Document("$bb.csv", 1)).append("l", list).append("e", 1);
	}

	@Test
	@DisplayName("Method put test")
	void testPut() {
		pdkStateMapEx.put("key", value);

		Document actual = (Document) map.get("key").get(PdkStateMap.KEY);
		assertNotNull(actual);
		assertFalse(actual.containsKey("$a.csv"));
		assertFalse(((Document) actual.get("b")).containsKey("$bb.csv"));
		Object actualList = actual.get("l");
		assertInstanceOf(ArrayList.class, actualList);
		assertEquals(2, ((List) actualList).size());
		Object actualListElem1 = ((List) actualList).get(0);
		assertInstanceOf(Document.class, actualListElem1);
		assertFalse(((Document) actualListElem1).containsKey("$c.csv"));
		assertFalse(((Document) ((Document) actualListElem1).get("d")).containsKey("$dd.csv"));
	}

	@Test
	@DisplayName("Method putIfAbsent test")
	void testPutIfAbsent() {
		pdkStateMapEx.putIfAbsent("key", value);

		Document actual = (Document) map.get("key").get(PdkStateMap.KEY);
		assertNotNull(actual);
		assertFalse(actual.containsKey("$a.csv"));
		assertFalse(((Document) actual.get("b")).containsKey("$bb.csv"));
		Object actualList = actual.get("l");
		assertInstanceOf(ArrayList.class, actualList);
		assertEquals(2, ((List) actualList).size());
		Object actualListElem1 = ((List) actualList).get(0);
		assertInstanceOf(Document.class, actualListElem1);
		assertFalse(((Document) actualListElem1).containsKey("$c.csv"));
		assertFalse(((Document) ((Document) actualListElem1).get("d")).containsKey("$dd.csv"));
	}

	@Test
	@DisplayName("Method get test")
	void testGet() {
		pdkStateMapEx.put("key", value);
		Object actualValue = pdkStateMapEx.get("key");

		assertInstanceOf(Document.class, actualValue);
		Document actualDoc = (Document) actualValue;
		assertEquals(1, actualDoc.get("$a.csv"));
		assertEquals(1, ((Document) actualDoc.get("b")).get("$bb.csv"));
		Object actualList = actualDoc.get("l");
		assertInstanceOf(ArrayList.class, actualList);
		assertEquals(2, ((List) actualList).size());
		Object actualListElem1 = ((List) actualList).get(0);
		assertInstanceOf(Document.class, actualListElem1);
		assertEquals(1, ((Document) actualListElem1).get("$c.csv"));
		assertEquals(1, ((Document) ((Document) actualListElem1).get("d")).get("$dd.csv"));
	}
}
