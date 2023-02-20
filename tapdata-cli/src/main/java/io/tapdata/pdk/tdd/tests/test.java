package io.tapdata.pdk.tdd.tests;

import com.google.common.io.LineReader;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FileUtils;
import org.omg.CORBA.Object;

import java.io.File;
import java.io.OutputStream;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;

import static io.tapdata.entity.simplify.TapSimplify.fromJson;
import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class test {
	static Thread thread;

	public static void main(String... args) throws Throwable {
		int count = 10_000_000;
//		int count = 1;

		Map<String, Runnable> stringMap = new ConcurrentHashMap<>();
		stringMap.put("aaaaaa", new Runnable() {
			@Override
			public void run() {

			}
		});
		Map<Class<?>, Runnable> classMap = new ConcurrentHashMap<>();
		classMap.put(TapTable.class, new Runnable() {
			@Override
			public void run() {

			}
		});

		TapTable obj = new TapTable("a");
		long time;
		time = System.currentTimeMillis();
		for(int i = 0; i < count; i++) {
			if(obj instanceof TapTable) {}
			else if(obj instanceof TapTable) {}
			else if(obj instanceof TapTable) {}
			else if(obj instanceof TapTable) {}
			else if(obj instanceof TapTable) {}
			else if(obj instanceof TapTable) {}
			else if(obj instanceof TapTable) {}
			else if(obj instanceof TapTable) {}
			else if(obj instanceof TapTable) {}
			else if(obj instanceof TapTable) {}
			else if(obj instanceof TapTable) {}
		}
		System.out.println("instanceof takes " + (System.currentTimeMillis() - time));

		time = System.currentTimeMillis();
		for(int i = 0; i < count; i++) {
			if(stringMap.containsKey(obj.getClass().getName())) {}
		}
		System.out.println("getName takes " + (System.currentTimeMillis() - time));

		time = System.currentTimeMillis();
		for(int i = 0; i < count; i++) {
			if(classMap.containsKey(obj.getClass())) {} {}
		}
		System.out.println("getClass takes " + (System.currentTimeMillis() - time));

    }
}
