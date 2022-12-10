package io.tapdata.pdk.tdd.tests;

import com.google.common.io.LineReader;
import io.tapdata.entity.logger.TapLogger;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;

public class test {
	static Thread thread;

	public static void main(String... args) throws Throwable {
//		CompressorStreamFactory factory = new CompressorStreamFactory();
//		try (CompressorOutputStream outputStream = factory.createCompressorOutputStream(CompressorStreamFactory.ZSTANDARD, null)) {
//
//		}

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		sdf.setTimeZone(TimeZone.getTimeZone("gmt-8"));
		Date date1 = sdf.parse("1111-12-03 03:26:02");
		System.out.println("date1 " + date1);

		Instant newInstant = date1.toInstant();
		System.out.println("newInstant " + newInstant);

		Date date2 = sdf.parse("1911-12-03 03:26:02");
		Instant newInstant1 = date2.toInstant();
		System.out.println("newInstant1 " + newInstant1);

		Instant instant = Instant.ofEpochMilli(-27077862838000L);
		System.out.println("instant " + instant);

		Date date = new Date(-27077862838000L);
		System.out.println("date " + date);

		Timestamp timestamp = new Timestamp(-27077862838000L);
		System.out.println("timestamp " + timestamp.toString() + " long " + timestamp.getTime());

		Timestamp timestamp1 = new Timestamp(-27078467638000L);
		System.out.println("timestamp1 " + timestamp1.toString() + " long " + timestamp1.getTime());

		Instant instant1 = Instant.ofEpochMilli(-27078467638000L);
		System.out.println("instant1 " + instant1);

//		StringBuilder builder =new StringBuilder();
//		for(int i = 0; i < 50000; i++) {
//			builder.append((char)i);
//		}
//		String str = builder.toString();
//		System.out.println(str);
//		String  a= "阿电风扇afadsf";
//		a.chars().forEachOrdered(new IntConsumer() {
//			@Override
//			public void accept(int value) {
//				System.out.println("value " + (char) value + " value " + value);
//			}
//		});
    }
}
