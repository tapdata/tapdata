package partition;

import io.tapdata.entity.schema.TapIndexEx;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.partition.ReadPartition;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentSkipListMap;

import static io.tapdata.entity.simplify.TapSimplify.index;
import static io.tapdata.entity.simplify.TapSimplify.indexField;

/**
 * @author aplomb
 */
public class ReadPartitionSkipListMapTest {
	@Test
	public void testEmptyPartitionFilter() {
		TapIndexEx partitionIndex = new TapIndexEx(index("a").indexField(indexField("a").fieldAsc(true)));
		ReadPartition readPartition = new ReadPartition().partitionIndex(partitionIndex).partitionFilter(TapPartitionFilter.create());
		ConcurrentSkipListMap<ReadPartition, Object> skipListMap = new ConcurrentSkipListMap<>(ReadPartition::compareTo);
		skipListMap.put(readPartition, new Object());

		ReadPartition theRP0 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create()));
		Assertions.assertNotNull(theRP0);
		Object rp = skipListMap.get(readPartition);
		Assertions.assertNotNull(rp);

		ReadPartition readPartition1 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", 1)));
		Assertions.assertNotNull(readPartition1);
	}



	@Test
	public void testReadPartitionSimpleInt() {
		TapIndexEx partitionIndex = new TapIndexEx(index("a").indexField(indexField("a").fieldAsc(true)));
		ReadPartition readPartition = new ReadPartition().partitionIndex(partitionIndex).partitionFilter(TapPartitionFilter.create().rightBoundary(QueryOperator.lt("a", 3)));
		ReadPartition readPartition1 = new ReadPartition().partitionIndex(partitionIndex).partitionFilter(TapPartitionFilter.create().leftBoundary(QueryOperator.gte("a", 3)).rightBoundary(QueryOperator.lt("a", 5)));
		ReadPartition readPartition2 = new ReadPartition().partitionIndex(partitionIndex).partitionFilter(TapPartitionFilter.create().leftBoundary(QueryOperator.gt("a", 5)));
		ConcurrentSkipListMap<ReadPartition, Object> skipListMap = new ConcurrentSkipListMap<>(ReadPartition::compareTo);
		skipListMap.put(readPartition, new Object());
		skipListMap.put(readPartition1, new Object());
		skipListMap.put(readPartition2, new Object());

		ReadPartition theRP0 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", -10)));
		Assertions.assertEquals(QueryOperator.LT, theRP0.getPartitionFilter().getRightBoundary().getOperator());
		Assertions.assertEquals(3, theRP0.getPartitionFilter().getRightBoundary().getValue());

		ReadPartition theRP = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", 0)));
		Assertions.assertEquals(QueryOperator.LT, theRP.getPartitionFilter().getRightBoundary().getOperator());
		Assertions.assertEquals(3, theRP.getPartitionFilter().getRightBoundary().getValue());

		ReadPartition theRP1 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", 3)));
		Assertions.assertEquals(QueryOperator.LT, theRP1.getPartitionFilter().getRightBoundary().getOperator());
		Assertions.assertEquals(5, theRP1.getPartitionFilter().getRightBoundary().getValue());
		Assertions.assertEquals(QueryOperator.GTE, theRP1.getPartitionFilter().getLeftBoundary().getOperator());
		Assertions.assertEquals(3, theRP1.getPartitionFilter().getLeftBoundary().getValue());

		ReadPartition theRP2 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", 4)));
		Assertions.assertEquals(QueryOperator.LT, theRP2.getPartitionFilter().getRightBoundary().getOperator());
		Assertions.assertEquals(5, theRP2.getPartitionFilter().getRightBoundary().getValue());
		Assertions.assertEquals(QueryOperator.GTE, theRP2.getPartitionFilter().getLeftBoundary().getOperator());
		Assertions.assertEquals(3, theRP2.getPartitionFilter().getLeftBoundary().getValue());

		ReadPartition theRP3 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", 5)));
		Assertions.assertEquals(QueryOperator.GT, theRP3.getPartitionFilter().getLeftBoundary().getOperator());
		Assertions.assertEquals(5, theRP3.getPartitionFilter().getLeftBoundary().getValue());

		ReadPartition theRP4 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", 100)));
		Assertions.assertEquals(QueryOperator.GT, theRP4.getPartitionFilter().getLeftBoundary().getOperator());
		Assertions.assertEquals(5, theRP4.getPartitionFilter().getLeftBoundary().getValue());
	}

	@Test
	public void testReadPartitionWithString() {
		TapIndexEx partitionIndex = new TapIndexEx(index("a").indexField(indexField("a").fieldAsc(true)));
		ReadPartition readPartition = new ReadPartition().partitionIndex(partitionIndex).partitionFilter(TapPartitionFilter.create().rightBoundary(QueryOperator.lt("a", "3")));
		ReadPartition readPartition1 = new ReadPartition().partitionIndex(partitionIndex).partitionFilter(TapPartitionFilter.create().leftBoundary(QueryOperator.gte("a", "3")).rightBoundary(QueryOperator.lt("a", "5")));
		ReadPartition readPartition2 = new ReadPartition().partitionIndex(partitionIndex).partitionFilter(TapPartitionFilter.create().leftBoundary(QueryOperator.gt("a", "5")));

		ConcurrentSkipListMap<ReadPartition, Object> skipListMap = new ConcurrentSkipListMap<>(ReadPartition::compareTo);
		skipListMap.put(readPartition, new Object());
		skipListMap.put(readPartition1, new Object());
		skipListMap.put(readPartition2, new Object());

		ReadPartition theRP0 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", "-10")));
		Assertions.assertEquals(QueryOperator.LT, theRP0.getPartitionFilter().getRightBoundary().getOperator());
		Assertions.assertEquals("3", theRP0.getPartitionFilter().getRightBoundary().getValue());

		ReadPartition theRP1 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", "00123")));
		Assertions.assertEquals(QueryOperator.LT, theRP1.getPartitionFilter().getRightBoundary().getOperator());
		Assertions.assertEquals("3", theRP1.getPartitionFilter().getRightBoundary().getValue());

		ReadPartition theRP2 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", "阿爸afasdf")));
		Assertions.assertEquals(QueryOperator.GT, theRP2.getPartitionFilter().getLeftBoundary().getOperator());
		Assertions.assertEquals("5", theRP2.getPartitionFilter().getLeftBoundary().getValue());

		ReadPartition theRP3 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", "aabadfsdf")));
		Assertions.assertEquals(QueryOperator.GT, theRP3.getPartitionFilter().getLeftBoundary().getOperator());
		Assertions.assertEquals("5", theRP3.getPartitionFilter().getLeftBoundary().getValue());

		ReadPartition theRP4 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", "4")));
		Assertions.assertEquals(QueryOperator.LT, theRP4.getPartitionFilter().getRightBoundary().getOperator());
		Assertions.assertEquals("5", theRP4.getPartitionFilter().getRightBoundary().getValue());
		Assertions.assertEquals(QueryOperator.GTE, theRP4.getPartitionFilter().getLeftBoundary().getOperator());
		Assertions.assertEquals("3", theRP4.getPartitionFilter().getLeftBoundary().getValue());

		ReadPartition theRP5 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", "adfadf")));
		Assertions.assertEquals(QueryOperator.GT, theRP5.getPartitionFilter().getLeftBoundary().getOperator());
		Assertions.assertEquals("5", theRP5.getPartitionFilter().getLeftBoundary().getValue());

		ReadPartition theRP6 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", "89")));
		Assertions.assertEquals(QueryOperator.GT, theRP6.getPartitionFilter().getLeftBoundary().getOperator());
		Assertions.assertEquals("5", theRP6.getPartitionFilter().getLeftBoundary().getValue());

	}

	@Test
	public void testMultiplePartitionKeys() {
		TapIndexEx partitionIndex = new TapIndexEx(index("a").indexField(indexField("a").fieldAsc(true)).indexField(indexField("b").fieldAsc(true)).indexField(indexField("c").fieldAsc(true)));
		ReadPartition readPartition = new ReadPartition().partitionIndex(partitionIndex).partitionFilter(TapPartitionFilter.create().rightBoundary(QueryOperator.lt("a", "3")));
		ReadPartition readPartition1 = new ReadPartition().partitionIndex(partitionIndex).partitionFilter(TapPartitionFilter.create().match("a", "3").rightBoundary(QueryOperator.lt("b", "5")));
		ReadPartition readPartition2 = new ReadPartition().partitionIndex(partitionIndex).partitionFilter(TapPartitionFilter.create().match("a", "3").leftBoundary(QueryOperator.gte("b", "5")).rightBoundary(QueryOperator.lt("b", "发啊打扫房间")));
		ReadPartition readPartition3 = new ReadPartition().partitionIndex(partitionIndex).partitionFilter(TapPartitionFilter.create().match("a", "3").match("b", "发啊打扫房间").rightBoundary(QueryOperator.lt("c", "aaaa")));
		ReadPartition readPartition4 = new ReadPartition().partitionIndex(partitionIndex).partitionFilter(TapPartitionFilter.create().match("a", "3").match("b", "发啊打扫房间").leftBoundary(QueryOperator.gte("c", "aaaa")).rightBoundary(QueryOperator.lt("c", "bbbb")));
		ReadPartition readPartition5 = new ReadPartition().partitionIndex(partitionIndex).partitionFilter(TapPartitionFilter.create().match("a", "3").match("b", "发啊打扫房间").leftBoundary(QueryOperator.gte("c", "bbbb")));
		ReadPartition readPartition6 = new ReadPartition().partitionIndex(partitionIndex).partitionFilter(TapPartitionFilter.create().match("a", "3").leftBoundary(QueryOperator.gt("b", "发啊打扫房间")));
		ReadPartition readPartition7 = new ReadPartition().partitionIndex(partitionIndex).partitionFilter(TapPartitionFilter.create().leftBoundary(QueryOperator.gt("a", "3")));

		ConcurrentSkipListMap<ReadPartition, Object> skipListMap = new ConcurrentSkipListMap<>(ReadPartition::compareTo);
		skipListMap.put(readPartition, new Object());
		skipListMap.put(readPartition1, new Object());
		skipListMap.put(readPartition2, new Object());
		skipListMap.put(readPartition3, new Object());
		skipListMap.put(readPartition4, new Object());
		skipListMap.put(readPartition5, new Object());
		skipListMap.put(readPartition6, new Object());
		skipListMap.put(readPartition7, new Object());

		Object o1 = skipListMap.get(new ReadPartition().partitionIndex(partitionIndex).partitionFilter(TapPartitionFilter.create().match("a", "3").match("b", "发啊打扫房间").leftBoundary(QueryOperator.gte("c", "aaaa")).rightBoundary(QueryOperator.lt("c", "bbbb"))));
		Assertions.assertNotNull(o1);

		ReadPartition thePR0 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", "012313").match("b", "b1232").match("c", "aaaaa")));
		Assertions.assertTrue(thePR0.getPartitionFilter().toString().contains("rightBoundary a<'3';"));

		ReadPartition thePR1 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", "012313").match("b", "b1232")));
		Assertions.assertTrue(thePR1.getPartitionFilter().toString().contains("rightBoundary a<'3';"));

		ReadPartition thePR2 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", "012313")));
		Assertions.assertTrue(thePR2.getPartitionFilter().toString().contains("rightBoundary a<'3';"));

		ReadPartition thePR3 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", "3")));
		Assertions.assertTrue(thePR3.getPartitionFilter().toString().contains("rightBoundary b<'5'; match {a: 3, }"));

		ReadPartition thePR4 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", "3").match("b", "65656")));
		Assertions.assertTrue(thePR4.getPartitionFilter().toString().contains("leftBoundary b>='5'; rightBoundary b<'发啊打扫房间'; match {a: 3, }"));

		ReadPartition thePR5 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", "3").match("b", "发啊打扫房间")));
		Assertions.assertTrue(thePR5.getPartitionFilter().toString().contains("rightBoundary c<'aaaa'; match {a: 3, b: 发啊打扫房间, }"));

		ReadPartition thePR6 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", "3").match("b", "发啊打扫房间").match("c", "1111")));
		Assertions.assertTrue(thePR6.getPartitionFilter().toString().contains("rightBoundary c<'aaaa'; match {a: 3, b: 发啊打扫房间, }"));

		ReadPartition thePR7 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", "3").match("b", "发啊打扫房间").match("c", "aaddd")));
		Assertions.assertTrue(thePR7.getPartitionFilter().toString().contains("leftBoundary c>='aaaa'; rightBoundary c<'bbbb'; match {a: 3, b: 发啊打扫房间, }"));

		ReadPartition thePR8 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", "3").match("b", "发啊打扫房间").match("c", "dfdfa")));
		Assertions.assertTrue(thePR8.getPartitionFilter().toString().contains("leftBoundary c>='bbbb'; match {a: 3, b: 发啊打扫房间, }"));

		ReadPartition thePR9 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", "3").match("b", "咋答复咖啡阿基德付款了").match("c", "dfdfa")));
		Assertions.assertTrue(thePR9.getPartitionFilter().toString().contains("leftBoundary b>'发啊打扫房间'; match {a: 3, }"));

		ReadPartition thePR10 = skipListMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match("a", "vF尽快了解").match("b", "咋答复咖啡阿基德付款了").match("c", "dfdfa")));
		Assertions.assertTrue(thePR10.getPartitionFilter().toString().contains("leftBoundary a>'3'; "));
	}

}
