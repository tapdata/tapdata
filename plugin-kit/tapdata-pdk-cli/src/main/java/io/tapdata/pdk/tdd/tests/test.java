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

public class test {
	public static void main(String... args) throws Throwable {
        System.out.println("hello");
//        System.out.println(convertJarFileName("mongodb-connector-v1.0-SNAPSHOT__628daf0716419763bbdce3f1__.jar"));
    }
}
