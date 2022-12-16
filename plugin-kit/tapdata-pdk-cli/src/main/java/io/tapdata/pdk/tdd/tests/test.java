package io.tapdata.pdk.tdd.tests;

import com.google.common.io.LineReader;
import io.tapdata.entity.logger.TapLogger;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.OutputStream;
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
		CompressorStreamFactory factory = new CompressorStreamFactory();
		OutputStream os = FileUtils.openOutputStream(new File("./aaaa"));
		try (CompressorOutputStream outputStream = factory.createCompressorOutputStream(CompressorStreamFactory.ZSTANDARD, os)) {

		}
    }
}
