package io.tapdata.flow.engine.V2.util;

import com.tapdata.constant.DateUtil;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.value.*;
import org.jetbrains.annotations.NotNull;

import java.text.ParseException;
import java.util.Arrays;
import java.util.TimeZone;

/**
 * @author jackin
 * @Description
 * @date 2023/3/10 17:42
 **/
public class TapCodecUtil {

	private static final String TAP_YEAR_BYTE_TAG = "$tapYear$";
	private static final String TAP_TIME_BYTE_TAG = "$tapTime$";
	private static final String TAP_DATE_BYTE_TAG = "$tapDate$";

	public static TapCodecsFilterManager genericCodecsFilterManager() {
		TapCodecsRegistry tapCodecsRegistry = TapCodecsRegistry.create();
		tapCodecsRegistry.registerFromTapValue(TapDateTimeValue.class, tapValue -> tapValue.getValue().toInstant());
		tapCodecsRegistry.registerFromTapValue(TapYearValue.class, tapValue -> {
			tapValue.getValue().setTimeZone(TimeZone.getDefault());
			final String yyyy = tapValue.getValue().toFormatString("yyyy");
			StringBuilder sb = new StringBuilder(TAP_YEAR_BYTE_TAG);
			sb.append(yyyy);
			return sb.toString().getBytes();
		});
		tapCodecsRegistry.registerFromTapValue(TapDateValue.class, tapValue -> {
			tapValue.getValue().setTimeZone(TimeZone.getDefault());
			final String dateString = tapValue.getValue().toFormatString("yyyy-MM-dd");
			StringBuilder sb = new StringBuilder(TAP_DATE_BYTE_TAG);
			sb.append(dateString);
			return sb.toString().getBytes();
		});
		tapCodecsRegistry.registerFromTapValue(TapTimeValue.class, tapValue -> {
			final String timeString = tapValue.getValue().toTimeStr();
			StringBuilder sb = new StringBuilder(TAP_TIME_BYTE_TAG);
			sb.append(timeString);
			return sb.toString().getBytes();
		});


		return getCodecsFilterManager(tapCodecsRegistry);
	}

	@NotNull
	public static TapCodecsFilterManager getCodecsFilterManager(TapCodecsRegistry tapCodecsRegistry) {
		tapCodecsRegistry.registerToTapValue(byte[].class, (o, tapType) -> {
			byte[] bytes = (byte[]) o;

			// TapYear judgment
			if (bytes.length > TAP_YEAR_BYTE_TAG.length()) {
				byte[] yearTagBytes = new byte[TAP_YEAR_BYTE_TAG.length()];
				System.arraycopy(bytes, 0, yearTagBytes, 0, TAP_YEAR_BYTE_TAG.length());
				if (Arrays.equals(yearTagBytes, TAP_YEAR_BYTE_TAG.getBytes())) {

					byte[] yearStringBytes = new byte[bytes.length - TAP_YEAR_BYTE_TAG.length()];
					System.arraycopy(bytes, TAP_YEAR_BYTE_TAG.length(), yearStringBytes, 0, yearStringBytes.length);
					String yearString = new String(yearStringBytes);
					try {
						return new TapYearValue(new DateTime(DateUtil.parse(yearString, "yyyy", null)));
					} catch (ParseException e) {
						throw new RuntimeException(e);
					}
				}
			}

			// TapDate judgment
			if (bytes.length > TAP_DATE_BYTE_TAG.length()) {
				byte[] dateTagBytes = new byte[TAP_DATE_BYTE_TAG.length()];
				System.arraycopy(bytes, 0, dateTagBytes, 0, TAP_DATE_BYTE_TAG.length());
				if (Arrays.equals(dateTagBytes, TAP_DATE_BYTE_TAG.getBytes())) {

					byte[] dateStringBytes = new byte[bytes.length - TAP_DATE_BYTE_TAG.length()];
					System.arraycopy(bytes, TAP_DATE_BYTE_TAG.length(), dateStringBytes, 0, dateStringBytes.length);
					String dateString = new String(dateStringBytes);
					try {
						return new TapDateValue(new DateTime(DateUtil.parse(dateString, "yyyy-MM-dd", null)));
					} catch (ParseException e) {
						throw new RuntimeException(e);
					}
				}
			}

			// TapTime judgment
			if (bytes.length > TAP_TIME_BYTE_TAG.length()) {
				byte[] timeTagBytes = new byte[TAP_TIME_BYTE_TAG.length()];
				System.arraycopy(bytes, 0, timeTagBytes, 0, TAP_TIME_BYTE_TAG.length());
				if (Arrays.equals(timeTagBytes, TAP_TIME_BYTE_TAG.getBytes())) {

					byte[] timeStringBytes = new byte[bytes.length - TAP_TIME_BYTE_TAG.length()];
					System.arraycopy(bytes, TAP_TIME_BYTE_TAG.length(), timeStringBytes, 0, timeStringBytes.length);
					String timeString = new String(timeStringBytes);
					return new TapTimeValue(DateTime.withTimeStr(timeString));
				}
			}
			return new TapBinaryValue((byte[]) o);
		});

		return TapCodecsFilterManager.create(tapCodecsRegistry);
	}

}
