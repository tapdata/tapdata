package io.tapdata.inspect.sql.autoUpdate;

import com.tapdata.constant.DateUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.inspect.sql.CustomSQLObject;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

public class AutoUpdateDateFilterTime implements CustomSQLObject<Object, Map<String, Object>> {
    public static final String FUNCTION_NAME = "$autoUpdateDate";
    public static final String SUB_FUNCTION = "$subtract";

    @Override
    public Object execute(Object functionObj, Map<String, Object> curMap) {
        Long timestamp = null;
        String functionValue = String.valueOf(functionObj);
        long subtract = getSubtract(curMap);

        if (functionObj instanceof Number) {
            timestamp = ((Number) functionObj).longValue();
        } else if (functionValue.matches("^[0-9]*$")) {
            timestamp = Long.parseLong(String.valueOf(functionObj));
        }

        if (null == timestamp) {
            return covertTime(functionValue, subtract);
        }
        return covertTimestamp(timestamp, subtract);
    }

    @Override
    public String getFunctionName() {
        return FUNCTION_NAME;
    }

    protected long getSubtract(Map<String, Object> map) {
        if (null == map || map.isEmpty() || !map.containsKey(SUB_FUNCTION)) {
            return 0L;
        }
        Object subtractObject = map.get(SUB_FUNCTION);
        if (subtractObject instanceof Number) {
            return ((Number)subtractObject).longValue();
        }
        try {
            return Long.parseLong(String.valueOf(subtractObject));
        } catch (Exception e) {
            return 0L;
        }
    }

    protected Long covertTimestamp(Long dateTime, long subtract) {
        if (dateTime < 0) {
            throw new CoreException("Illegal argument in function: {}, wrong value: {}, the correct key value pairs should be as follows: \"\": {an timestamp which more than zero or a data time string}",
                    getFunctionName(), dateTime, getFunctionName());
        }
        Date covert = covert(new Date(dateTime), subtract);
        return covert.getTime();
    }

    protected String covertTime(String dateTime, long subtract) {
        dateTime = replaceDate(dateTime);
        String dateFormat = DateUtil.determineDateFormat(dateTime);
        if (null == dateFormat) {
            throw new CoreException("Illegal argument in function: {}, wrong value: {}, the correct key value pairs should be as follows: \"\": {\"yyyy-dd-MM hh:mm:ss[.SSSSSS]\"}",
                    getFunctionName(), dateTime, getFunctionName());
        }
        Object parse = DateUtil.parse(dateTime);
        Date covert = covert(parse, subtract);
        return DateUtil.timeStamp2Date(covert.getTime(), dateFormat);
    }

    protected String replaceDate(String format) {
        Calendar calendar = Calendar.getInstance();
        format = format(format, "%y", String.valueOf(calendar.get(Calendar.YEAR)));
        format = format(format, "%M", String.valueOf(calendar.get(Calendar.MONTH)));
        format = format(format, "%d", String.valueOf(calendar.get(Calendar.DATE)));
        format = format(format, "%h", String.valueOf(calendar.get(Calendar.HOUR)));
        format = format(format, "%m", String.valueOf(calendar.get(Calendar.YEAR)));
        format = format(format, "%s", String.valueOf(calendar.get(Calendar.SECOND)));
        format = format(format, "%S", String.valueOf(calendar.get(Calendar.MILLISECOND)));
        return format;
    }

    protected String format(String str, String format, String value) {
        if (str.contains(format)) {
            str = str.replace(format, value);
        }
        return str;
    }

    protected Date covert(Object parse, long subtract) {
        if (parse instanceof Date) {
            return calculateDate((Date)parse, subtract);
        } else {
            throw new CoreException("UNKnow date time format string: " + parse);
        }
    }

    protected Date calculateDate(Date parse, long subtract) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(parse);
        calendar.add(Calendar.MILLISECOND, -1 * ((Long)subtract).intValue());
        return calendar.getTime();
    }
}
