package com.tapdata.tm.monitor.param;

import com.tapdata.tm.monitor.constant.Granularity;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Dexter
 */
@Data
public class AggregateMeasurementParam {
    Long start;
    Long end;
    Map<String, String> tags;
    List<String> granularity;

    public boolean isStartEndValid() {
        return null != start && null != end;
    }

    private static List<String> validGranularity = new ArrayList<String>(){{
        add(Granularity.GRANULARITY_MINUTE);
        add(Granularity.GRANULARITY_HOUR);
        add(Granularity.GRANULARITY_DAY);
        add(Granularity.GRANULARITY_MONTH);
    }};

    public boolean isGranularityValid() {
        boolean valid = true;
        for (String item : granularity) {
            if (!validGranularity.contains(item)) {
                valid = false;
                break;
            }
        }

        return valid;
    }
}
