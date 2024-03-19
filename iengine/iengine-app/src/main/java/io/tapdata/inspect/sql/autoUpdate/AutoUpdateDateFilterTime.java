package io.tapdata.inspect.sql.autoUpdate;

import com.tapdata.constant.DateUtil;
import com.tapdata.entity.inspect.Inspect;
import com.tapdata.entity.inspect.InspectCron;
import io.tapdata.entity.error.CoreException;
import io.tapdata.inspect.sql.CustomSQLObject;

import java.util.Calendar;
import java.util.Date;

public class AutoUpdateDateFilterTime implements CustomSQLObject<Inspect, Object> {
    public static final String FUNCTION_NAME = "$autoUpdateDate";

    @Override
    public Object execute(Inspect inspect, Object functionObj) {
        InspectCron inspectCron = inspect.getInspectCron();
        if (!verify(inspectCron)) {
            return functionObj;
        }
        Long timestamp = null;
        String functionValue = String.valueOf(functionObj);
        if (functionObj instanceof Number) {
            timestamp = ((Number) functionObj).longValue();
        } else if (functionValue.matches("^[0-9]*$")) {
            timestamp = Long.parseLong(String.valueOf(functionObj));
        }

        if (null == timestamp) {
            return covertTime(functionValue, inspectCron);
        }
        return covertTimestamp(timestamp, inspectCron);
    }

    @Override
    public String getFunctionName() {
        return FUNCTION_NAME;
    }

    protected Long covertTimestamp(Long dateTime, InspectCron cronInfo) {
        if (dateTime < 0) {
            throw new CoreException("Illegal argument in function: {}, wrong value: {}, the correct key value pairs should be as follows: \"\": {an timestamp which more than zero or a data time string}",
                    getFunctionName(), dateTime, getFunctionName());
        }
        Date covert = covert(new Date(dateTime), cronInfo);
        return covert.getTime();
    }

    protected String covertTime(String dateTime, InspectCron cronInfo) {
        String dateFormat = DateUtil.determineDateFormat(dateTime);
        if (null == dateFormat) {
            throw new CoreException("Illegal argument in function: {}, wrong value: {}, the correct key value pairs should be as follows: \"\": {\"yyyy-dd-MM hh:mm:ss[.SSSSSS]\"}",
                    getFunctionName(), dateTime, getFunctionName());
        }
        Object parse = DateUtil.parse(dateTime);
        Date covert = covert(parse, cronInfo);
        return DateUtil.timeStamp2Date(covert.getTime(), dateFormat);
    }

    protected Date covert(Object parse, InspectCron cronInfo) {
        int times = cronInfo.getScheduleTimes();
        int intervals = cronInfo.getIntervals() * times;
        if (parse instanceof Date) {
            return calculateDate((Date)parse, unit(cronInfo), intervals);
        } else {
            throw new CoreException("UNKnow date time format string: " + parse);
        }
    }

    protected Date calculateDate(Date parse, String unit, int intervals) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(parse);
        calculateCalendar(calendar, unit, intervals);
        return calendar.getTime();
    }

    protected void calculateCalendar(Calendar calendar, String unit, int intervals) {
        int u = 0;
        switch (unit) {
            case "second":
                u = Calendar.SECOND;
                break;
            case "hour":
                u = Calendar.HOUR;
                break;
            case "day":
                u = Calendar.DATE;
                break;
            case "week":
                u = Calendar.DATE;
                intervals = intervals * 7;
                break;
            case "month":
                u = Calendar.MONTH;
                break;
            case "minute":
            default:
                u = Calendar.MINUTE;
        }
        calendar.add(u, intervals);
    }

    protected String unit(InspectCron cronInfo) {
        String unit = cronInfo.getIntervalsUnit();
        if (null == unit) {
            unit = "minute";
        } else {
            unit = unit.toLowerCase();
        }
        return unit;
    }

    protected boolean verify(InspectCron cronInfo) {
        if (null == cronInfo) {
            return false;
        }
        int times = cronInfo.getScheduleTimes();
        return times > 0;
    }
}
