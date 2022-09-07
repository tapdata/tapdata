package io.tapdata.pdk.tdd.tests;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.value.DateTime;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

public class test {
	public static void main(String... args) throws Throwable {
		AtomicLong longAdder = new AtomicLong();
        System.out.println("hello " + longAdder.get());
		longAdder.addAndGet(Long.MAX_VALUE);

		System.out.println("hello " + longAdder.get());

		longAdder.incrementAndGet();
		System.out.println("hello " + longAdder.get());
		longAdder.incrementAndGet();
		System.out.println("hello " + longAdder.get());
		longAdder.incrementAndGet();
		System.out.println("hello " + longAdder.get());

    }
}
