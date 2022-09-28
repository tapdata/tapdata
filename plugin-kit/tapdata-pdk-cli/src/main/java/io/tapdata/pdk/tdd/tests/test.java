package io.tapdata.pdk.tdd.tests;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TypeUtils;
import io.tapdata.pdk.core.tapnode.TapNodeContainer;
import org.apache.commons.io.FileUtils;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFCreationHelper;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileOutputStream;
import java.math.BigDecimal;
import java.sql.Time;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static io.tapdata.entity.simplify.TapSimplify.*;

public class test {
	public static void main(String... args) throws Throwable {
		Map<String, String> map1 = new ConcurrentHashMap<>();
		map1.put("TapMapValue.class", "aaa");

		Map<Class<?>, String> map = new ConcurrentHashMap<>();
		map.put(TapMapValue.class, "aaa");

		TapCodecsRegistry codecsRegistry = TapCodecsRegistry.create();
		codecsRegistry.registerFromTapValue(TapMapValue.class, tapValue -> toJson(tapValue.getValue()));
		TapValue<?,?> tapMapValue = new TapMapValue();

		long time = System.currentTimeMillis();
		for(int i = 0; i < 10000000; i++) {
			FromTapValueCodec<TapValue<?, ?>> fromTapValueCodec = codecsRegistry.getCustomFromTapValueCodec((Class<TapValue<?, ?>>) tapMapValue.getClass());
		}
		System.out.println("takes " + (System.currentTimeMillis() - time));

		time = System.currentTimeMillis();
		for(int i = 0; i < 10000000; i++) {
			String str = map.get(tapMapValue.getClass());
		}
		System.out.println("takes " + (System.currentTimeMillis() - time));


		time = System.currentTimeMillis();
		for(int i = 0; i < 10000000; i++) {
			String str = map1.get("TapMapValue.class");
		}
		System.out.println("takes " + (System.currentTimeMillis() - time));
    }
}
