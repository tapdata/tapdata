package io.tapdata.pdk.tdd.tests;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TypeHolder;
import io.tapdata.entity.utils.TypeUtils;
import io.tapdata.pdk.core.tapnode.TapNodeContainer;
import io.tapdata.pdk.core.utils.TapConstants;
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
		JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);

		String json1 = "{\"id\":\"adfs\",\"nameFieldMap\":{\"a\":{\"name\":\"a\",\"dataType\":\"varchar\"}}}";
		long time = System.currentTimeMillis();
		for(int i = 0; i< 1000000; i++) {
			Object value1 = jsonParser.fromJson(json1, TapTable.class);
		}
		System.out.println("2takes " + (System.currentTimeMillis() - time));


		String json = "{\"id\":\"adfs\",\"nameFieldMap\":{\"a\":{\"name\":\"a\",\"dataType\":\"varchar\",\"tapType\":{\"type\":8,\"bit\":32}}}}";
		time = System.currentTimeMillis();
		for(int i = 0; i< 1000000; i++) {
			Object value1 = jsonParser.fromJson(json, TapTable.class);
		}
		System.out.println("3takes " + (System.currentTimeMillis() - time));
    }
}
