package io.tapdata.pdk.tdd.tests;

import io.tapdata.entity.logger.TapLogger;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.text.SimpleDateFormat;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class test {
	static Thread thread;
	public static void main(String... args) throws Throwable {
		CompressorStreamFactory factory = new CompressorStreamFactory();
		try (CompressorOutputStream outputStream = factory.createCompressorOutputStream(CompressorStreamFactory.ZSTANDARD, null)) {

		}
    }
}
