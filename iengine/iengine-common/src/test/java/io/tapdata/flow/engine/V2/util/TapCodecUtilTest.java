package io.tapdata.flow.engine.V2.util;

import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.type.TapBinary;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapYear;
import io.tapdata.entity.schema.value.*;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author samuel
 * @Description
 * @create 2024-05-16 14:10
 **/
@DisplayName("Class TapCodecUtil Test")
class TapCodecUtilTest {

	@Nested
	@DisplayName("Method addTag test")
	class addTagTest {
		@Test
		@DisplayName("Test main process")
		void testMainProcess() {
			String value = "test";
			byte[] result = TapCodecUtil.addTag(value.getBytes(StandardCharsets.UTF_8), TapCodecUtil.TAP_YEAR_BYTE_TAG_V2);
			assertEquals("TapCodecUtil\u0001test", new String(result, StandardCharsets.UTF_8));
		}

		@Test
		@DisplayName("Test input null")
		void testInputNull() {
			byte[] result = assertDoesNotThrow(() -> TapCodecUtil.addTag(null, TapCodecUtil.TAP_YEAR_BYTE_TAG_V2));
			assertNull(result);
		}
	}

	@Test
	@DisplayName("Method createEngineCodecsFilterManger test")
	void testCreateEngineCodecsFilterManger() {
		TapCodecsFilterManager engineCodecsFilterManger = TapCodecUtil.createEngineCodecsFilterManger();
		assertNotNull(engineCodecsFilterManger);
		TapCodecsRegistry codecsRegistry = engineCodecsFilterManger.getCodecsRegistry();
		assertNotNull(codecsRegistry);
		assertNotNull(codecsRegistry.getCustomFromTapValueCodec(TapDateValue.class));
		assertNotNull(codecsRegistry.getCustomFromTapValueCodec(TapDateTimeValue.class));
		assertNotNull(codecsRegistry.getCustomFromTapValueCodec(TapYearValue.class));
		assertNotNull(codecsRegistry.getCustomFromTapValueCodec(TapTimeValue.class));
		assertNotNull(codecsRegistry.getCustomToTapValueCodec(byte[].class));
	}

	@Nested
	@DisplayName("Test from and to TapValue v2")
	class fromAndToTapValueV2Test {

		private TapCodecsFilterManager engineCodecsFilterManger;

		@BeforeEach
		void setUp() {
			engineCodecsFilterManger = TapCodecUtil.createEngineCodecsFilterManger();
		}

		@Test
		@DisplayName("test from and to TapDateValue")
		void testFromAndToTapDateValue() {
			LocalDateTime localDateTime = LocalDateTime.of(2024, 5, 16, 0, 0);
			DateTime dateTime = new DateTime(localDateTime);
			TapDateValue tapDateValue = new TapDateValue(dateTime);
			FromTapValueCodec<TapDateValue> customFromTapValueCodec = engineCodecsFilterManger.getCodecsRegistry().getCustomFromTapValueCodec(TapDateValue.class);
			Object result = customFromTapValueCodec.fromTapValue(tapDateValue);
			ToTapValueCodec<?> customToTapValueCodec = engineCodecsFilterManger.getCodecsRegistry().getCustomToTapValueCodec(result.getClass());
			TapValue<?, ?> tapValue = customToTapValueCodec.toTapValue(result, new TapDate());
			assertInstanceOf(TapDateValue.class, tapValue);
			assertNotSame(tapDateValue, tapValue);
			assertEquals(localDateTime.toEpochSecond(ZoneOffset.UTC), ((TapDateValue) tapValue).getValue().getSeconds());
		}

		@Test
		@DisplayName("test from and to illegal TapDateValue")
		void testFromAndToIllegalTapDateValue() {
			String illegalDate = "0001-00-00";
			DateTime dateTime = new DateTime(illegalDate, DateTime.DATE_TYPE);
			TapDateValue tapDateValue = new TapDateValue(dateTime);
			FromTapValueCodec<TapDateValue> customFromTapValueCodec = engineCodecsFilterManger.getCodecsRegistry().getCustomFromTapValueCodec(TapDateValue.class);
			Object result = customFromTapValueCodec.fromTapValue(tapDateValue);
			ToTapValueCodec<?> customToTapValueCodec = engineCodecsFilterManger.getCodecsRegistry().getCustomToTapValueCodec(result.getClass());
			TapValue<?, ?> tapValue = customToTapValueCodec.toTapValue(result, new TapDate());
			assertInstanceOf(TapDateValue.class, tapValue);
			assertNotSame(tapDateValue, tapValue);
			assertTrue(((TapDateValue) tapValue).getValue().isContainsIllegal());
			assertEquals(illegalDate, ((TapDateValue) tapValue).getValue().getIllegalDate());
		}

		@Test
		@DisplayName("test from and to TapDateTimeValue")
		void testFromAndToTapDateTimeValue() {
			LocalDateTime localDateTime = LocalDateTime.of(2024, 5, 16, 17, 5, 12, 123);
			DateTime dateTime = new DateTime(localDateTime);
			TapDateTimeValue tapDateTimeValue = new TapDateTimeValue(dateTime);
			FromTapValueCodec<TapDateTimeValue> customFromTapValueCodec = engineCodecsFilterManger.getCodecsRegistry().getCustomFromTapValueCodec(TapDateTimeValue.class);
			Object result = customFromTapValueCodec.fromTapValue(tapDateTimeValue);
			assertInstanceOf(Instant.class, result);
			assertEquals(localDateTime.toEpochSecond(ZoneOffset.UTC), ((Instant) result).getEpochSecond());
			assertEquals(localDateTime.getNano(), ((Instant) result).getNano());
		}

		@Test
		@DisplayName("test from and to illegal TapDateTimeValue")
		void testFromAndToIllegalTapDatetimeValue() {
			String illegalDatetime = "0001-0-01-12-01-02";
			String expect = "0001-00-01 12:01:02";
			DateTime dateTime = new DateTime(illegalDatetime, DateTime.DATETIME_TYPE);
			TapDateTimeValue tapDateTimeValue = new TapDateTimeValue(dateTime);
			FromTapValueCodec<TapDateTimeValue> customFromTapValueCodec = engineCodecsFilterManger.getCodecsRegistry().getCustomFromTapValueCodec(TapDateTimeValue.class);
			Object result = customFromTapValueCodec.fromTapValue(tapDateTimeValue);
			ToTapValueCodec<?> customToTapValueCodec = engineCodecsFilterManger.getCodecsRegistry().getCustomToTapValueCodec(result.getClass());
			TapValue<?, ?> tapValue = customToTapValueCodec.toTapValue(result, new TapDateTime());
			assertInstanceOf(TapDateTimeValue.class, tapValue);
			assertTrue(((TapDateTimeValue) tapValue).getValue().isContainsIllegal());
			assertEquals(expect, ((TapDateTimeValue) tapValue).getValue().getIllegalDate());
		}

		@Test
		@DisplayName("test from and to TapYearValue")
		void testFromAndToTapYearValue() {
			LocalDateTime localDateTime = LocalDateTime.of(2024, 1, 1, 0, 0);
			DateTime dateTime = new DateTime(localDateTime);
			TapYearValue tapYearValue = new TapYearValue(dateTime);
			FromTapValueCodec<TapYearValue> customFromTapValueCodec = engineCodecsFilterManger.getCodecsRegistry().getCustomFromTapValueCodec(TapYearValue.class);
			Object result = customFromTapValueCodec.fromTapValue(tapYearValue);
			ToTapValueCodec<?> customToTapValueCodec = engineCodecsFilterManger.getCodecsRegistry().getCustomToTapValueCodec(result.getClass());
			TapValue<?, ?> tapValue = customToTapValueCodec.toTapValue(result, new TapYear());
			assertInstanceOf(TapYearValue.class, tapValue);
			assertEquals(localDateTime.toEpochSecond(ZoneOffset.UTC), ((TapYearValue) tapValue).getValue().getSeconds());
		}

		@Test
		@DisplayName("test from and to TapTimeValue")
		void testFromAndToTapTimeValue() {
			LocalDateTime localDateTime = LocalDateTime.of(1970, 1, 1, 17, 18, 47);
			DateTime dateTime = new DateTime(localDateTime);
			TapTimeValue tapTimeValue = new TapTimeValue(dateTime);
			FromTapValueCodec<TapTimeValue> customFromTapValueCodec = engineCodecsFilterManger.getCodecsRegistry().getCustomFromTapValueCodec(TapTimeValue.class);
			Object result = customFromTapValueCodec.fromTapValue(tapTimeValue);
			ToTapValueCodec<?> customToTapValueCodec = engineCodecsFilterManger.getCodecsRegistry().getCustomToTapValueCodec(result.getClass());
			TapValue<?, ?> tapValue = customToTapValueCodec.toTapValue(result, new TapYear());
			assertInstanceOf(TapTimeValue.class, tapValue);
			assertEquals(tapTimeValue.getValue().toTimeStr(), ((TapTimeValue) tapValue).getValue().toTimeStr());
		}

		@Test
		@DisplayName("test input null")
		void testInputNull() {
			ToTapValueCodec<?> customToTapValueCodec = engineCodecsFilterManger.getCodecsRegistry().getCustomToTapValueCodec(byte[].class);
			TapValue<?, ?> tapValue = customToTapValueCodec.toTapValue(null, new TapDateTime());
			assertInstanceOf(TapBinaryValue.class, tapValue);
			assertNull(((TapBinaryValue) tapValue).getValue());
		}

		@Test
		@DisplayName("test input other normal bytes value")
		void testInputNormalBytes() {
			String str = "test";
			byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
			ToTapValueCodec<?> customToTapValueCodec = engineCodecsFilterManger.getCodecsRegistry().getCustomToTapValueCodec(byte[].class);
			TapValue<?, ?> tapValue = customToTapValueCodec.toTapValue(bytes, new TapBinary());
			assertInstanceOf(TapBinaryValue.class, tapValue);
			assertArrayEquals(bytes, ((TapBinaryValue) tapValue).getValue().getValue());

			str = "testtesttesttesttesttest";
			bytes = str.getBytes(StandardCharsets.UTF_8);
			tapValue = customToTapValueCodec.toTapValue(bytes, new TapBinary());
			assertInstanceOf(TapBinaryValue.class, tapValue);
			assertArrayEquals(bytes, ((TapBinaryValue) tapValue).getValue().getValue());
		}
	}

	@Nested
	@DisplayName("Test from and to TapValue v1")
	class fromAndToTapValueV1 {

		private TapCodecsFilterManager engineCodecsFilterManger;

		@BeforeEach
		void setUp() {
			engineCodecsFilterManger = TapCodecUtil.createEngineCodecsFilterManger();
		}

		@Test
		@DisplayName("test from and to TapDateValue")
		void testFromAndToTapDateValue() {
			LocalDateTime localDateTime = LocalDateTime.of(2024, 5, 16, 0, 0);
			DateTime dateTime = new DateTime(localDateTime);
			TapDateValue tapDateValue = new TapDateValue(dateTime);
			Object result = TapCodecUtil.fromTapValueV1(tapDateValue);
			ToTapValueCodec<?> customToTapValueCodec = engineCodecsFilterManger.getCodecsRegistry().getCustomToTapValueCodec(result.getClass());
			TapValue<?, ?> tapValue = customToTapValueCodec.toTapValue(result, new TapDate());
			assertInstanceOf(TapDateValue.class, tapValue);
			assertNotSame(tapDateValue, tapValue);
			assertEquals(localDateTime.toEpochSecond(ZoneOffset.ofHours(8)), ((TapDateValue) tapValue).getValue().getSeconds());
		}

		@Test
		@DisplayName("test from and to TapDateTimeValue")
		void testFromAndToTapDateTimeValue() {
			LocalDateTime localDateTime = LocalDateTime.of(2024, 5, 16, 17, 5, 12, 123);
			DateTime dateTime = new DateTime(localDateTime);
			TapDateTimeValue tapDateTimeValue = new TapDateTimeValue(dateTime);
			Object result = TapCodecUtil.fromTapValueV1(tapDateTimeValue);
			assertInstanceOf(Instant.class, result);
			assertEquals(localDateTime.toEpochSecond(ZoneOffset.UTC), ((Instant) result).getEpochSecond());
			assertEquals(localDateTime.getNano(), ((Instant) result).getNano());
		}

		@Test
		@DisplayName("test from and to TapYearValue")
		void testFromAndToTapYearValue() {
			LocalDateTime localDateTime = LocalDateTime.of(2024, 1, 1, 0, 0);
			DateTime dateTime = new DateTime(localDateTime);
			TapYearValue tapYearValue = new TapYearValue(dateTime);
			Object result = TapCodecUtil.fromTapValueV1(tapYearValue);
			ToTapValueCodec<?> customToTapValueCodec = engineCodecsFilterManger.getCodecsRegistry().getCustomToTapValueCodec(result.getClass());
			TapValue<?, ?> tapValue = customToTapValueCodec.toTapValue(result, new TapYear());
			assertInstanceOf(TapYearValue.class, tapValue);
			assertEquals(localDateTime.toEpochSecond(ZoneOffset.ofHours(8)), ((TapYearValue) tapValue).getValue().getSeconds());
		}

		@Test
		@DisplayName("test from and to TapTimeValue")
		void testFromAndToTapTimeValue() {
			LocalDateTime localDateTime = LocalDateTime.of(1970, 1, 1, 17, 18, 47);
			DateTime dateTime = new DateTime(localDateTime);
			TapTimeValue tapTimeValue = new TapTimeValue(dateTime);
			Object result = TapCodecUtil.fromTapValueV1(tapTimeValue);
			ToTapValueCodec<?> customToTapValueCodec = engineCodecsFilterManger.getCodecsRegistry().getCustomToTapValueCodec(result.getClass());
			TapValue<?, ?> tapValue = customToTapValueCodec.toTapValue(result, new TapYear());
			assertInstanceOf(TapTimeValue.class, tapValue);
			assertEquals(tapTimeValue.getValue().toTimeStr(), ((TapTimeValue) tapValue).getValue().toTimeStr());
		}
	}
}