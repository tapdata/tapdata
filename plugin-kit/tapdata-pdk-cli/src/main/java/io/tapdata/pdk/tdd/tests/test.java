package io.tapdata.pdk.tdd.tests;

import com.google.common.io.LineReader;
import io.tapdata.entity.logger.TapLogger;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.text.SimpleDateFormat;
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
