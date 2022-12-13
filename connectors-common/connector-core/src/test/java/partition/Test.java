package partition;

import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.partition.ReadPartition;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Date;

/**
 * @author aplomb
 */
public class Test {
	public static void main(String[] args) {
		ReadPartition readPartition = ReadPartition.create().id("id").partitionFilter(TapPartitionFilter.create().rightBoundary(QueryOperator.lt("a", 1)).leftBoundary(QueryOperator.gt("a", 123)));
		System.out.println(readPartition);

		Instant instant = Instant.now();
		long value = instant.getEpochSecond() * 1000_000_000 + instant.getNano();
		BigDecimal value1 = BigDecimal.valueOf(instant.getEpochSecond() * 1000_000_000).add(BigDecimal.valueOf(instant.getNano()));
		System.out.println("seconds " + instant.getEpochSecond());
		System.out.println("nano " + instant.getNano());
		System.out.println("value " + value);
		System.out.println("value1 " + value1);
		System.out.println("value1 " + Long.MAX_VALUE);

		DateTime dateTime = new DateTime(new Date());
		System.out.println("dateTime " + dateTime);

		BigDecimal nano = dateTime.toNanoSeconds();
		System.out.println("nano " + nano);

		System.out.println("theDateTime " + new DateTime(nano, 9));

		System.out.println("compared " + (BigDecimal.TEN.compareTo(BigDecimal.ONE)));
		System.out.println("Math.pow " + ((Double)Math.pow(10, 1)).longValue());
		System.out.println("Math.pow " + ((Double)Math.pow(10, 10)).longValue());
		System.out.println("Math.pow " + ((Double)Math.pow(10, 7)).longValue());
		System.out.println("Math.pow " + ((Double)Math.pow(10, 8)).longValue());
		System.out.println("Math.pow " + ((Double)Math.pow(10, 9)).longValue());
		System.out.println("Math.pow " + ((Double)Math.pow(10, 15)).longValue());
	}
}
