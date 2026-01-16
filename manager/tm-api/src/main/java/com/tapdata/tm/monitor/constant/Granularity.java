package com.tapdata.tm.monitor.constant;

import com.tapdata.tm.commons.metrics.MetricCons;
import com.tapdata.tm.monitor.entity.TDigestEntity;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.utils.TimeUtil;
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import io.tapdata.common.sample.request.Sample;
import org.apache.commons.collections4.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Granularity {
    public static final String GRANULARITY_MINUTE = "minute";
    public static final String GRANULARITY_HOUR = "hour";
    public static final String GRANULARITY_DAY = "day";
    public static final String GRANULARITY_MONTH = "month";

    private static final String INVALID_GRANULARITY_ERR_FORMAT = String.format("Invalid granularity value %%s, " +
            "granularity value should be in %s, %s, %s and %s", GRANULARITY_MINUTE, GRANULARITY_HOUR, GRANULARITY_DAY,
            GRANULARITY_MONTH);

    public static String getNextLevelGranularity(String granularity) {
        switch (granularity) {
            case Granularity.GRANULARITY_MINUTE:
                return GRANULARITY_HOUR;
            case Granularity.GRANULARITY_HOUR:
                return GRANULARITY_DAY;
            case Granularity.GRANULARITY_DAY:
                return GRANULARITY_MONTH;
            case Granularity.GRANULARITY_MONTH:
                throw new RuntimeException("You should not get next level granularity when granularity is month.");
            default:
                throw new RuntimeException(String.format(INVALID_GRANULARITY_ERR_FORMAT, granularity));
        }

    }

    public static String getPreviousLevelGranularity(String granularity) {
        switch (granularity) {
            case Granularity.GRANULARITY_MINUTE:
                throw new RuntimeException("You should not get previous level granularity when granularity is minute.");
            case Granularity.GRANULARITY_HOUR:
                return GRANULARITY_MINUTE;
            case Granularity.GRANULARITY_DAY:
                return GRANULARITY_HOUR;
            case Granularity.GRANULARITY_MONTH:
                return GRANULARITY_MONTH;
            default:
                throw new RuntimeException(String.format(INVALID_GRANULARITY_ERR_FORMAT, granularity));
        }

    }

    private static final long GRANULARITY_MILLIS_INTERVAL_MINUTE = 60 * 1000;
    private static final long GRANULARITY_MILLIS_INTERVAL_HOUR = 60 * 60 * 1000;
    private static final long GRANULARITY_MILLIS_INTERVAL_DAY = 24 * 60 * 60 * 1000;
    public static long getGranularityMillisInterval(String granularity) {
        switch (granularity) {
            case Granularity.GRANULARITY_MINUTE:
                return GRANULARITY_MILLIS_INTERVAL_MINUTE;
            case Granularity.GRANULARITY_HOUR:
                return GRANULARITY_MILLIS_INTERVAL_HOUR;
            case Granularity.GRANULARITY_DAY:
                return GRANULARITY_MILLIS_INTERVAL_DAY;
            case Granularity.GRANULARITY_MONTH:
                throw new RuntimeException("You should not get millis interval when granularity is month.");
            default:
                throw new RuntimeException(String.format(INVALID_GRANULARITY_ERR_FORMAT, granularity));
        }

    }


    private static final long TIMELINE_MILLIS_INTERVAL_MINUTE = 5 * 1000;
    private static final long TIMELINE_MILLIS_INTERVAL_HOUR = 60 * 1000;
    private static final long TIMELINE_MILLIS_INTERVAL_DAY = 60 * 60 * 1000;
    private static final long TIMELINE_MILLIS_INTERVAL_MONTH = 24 * 60 * 60 * 1000;
    public static long getTimelineMillisInterval(String granularity) {
        switch (granularity) {
            case Granularity.GRANULARITY_MINUTE:
                return TIMELINE_MILLIS_INTERVAL_MINUTE;
            case Granularity.GRANULARITY_HOUR:
                return TIMELINE_MILLIS_INTERVAL_HOUR;
            case Granularity.GRANULARITY_DAY:
                return TIMELINE_MILLIS_INTERVAL_DAY;
            case Granularity.GRANULARITY_MONTH:
                return TIMELINE_MILLIS_INTERVAL_MONTH;
            default:
                throw new RuntimeException(String.format(INVALID_GRANULARITY_ERR_FORMAT, granularity));
        }

    }

    private static final long MAX_MILLIS_FOR_MINUTE_GRANULARITY = 60 * 60 * 1000L;
    private static final long MAX_MILLIS_FOR_HOUR_GRANULARITY = 24 * 60 * 60 * 1000L;
    private static final long MAX_MILLIS_FOR_DAY_GRANULARITY = 30 * 24 * 60 * 60 * 1000L;

    public static String calculateReasonableGranularity(long start, long end) {
        long diff = end - start;
        if (diff < MAX_MILLIS_FOR_MINUTE_GRANULARITY) {
            return GRANULARITY_MINUTE;
        }
        if (diff < MAX_MILLIS_FOR_HOUR_GRANULARITY) {
            return GRANULARITY_HOUR;
        }
        if (diff < MAX_MILLIS_FOR_DAY_GRANULARITY) {
            return GRANULARITY_DAY;
        }

        return GRANULARITY_MONTH;
    }


    public static Date calculateGranularityDate(String granularity, Date date) {
        switch (granularity) {
            case Granularity.GRANULARITY_MINUTE:
                return TimeUtil.cleanTimeAfterMinute(date);
            case Granularity.GRANULARITY_HOUR:
                return TimeUtil.cleanTimeAfterHour(date);
            case Granularity.GRANULARITY_DAY:
                return TimeUtil.cleanTimeAfterDay(date);
            case Granularity.GRANULARITY_MONTH:
                return TimeUtil.cleanTimeAfterMonth(date);
            default:
                throw new RuntimeException(String.format(INVALID_GRANULARITY_ERR_FORMAT, granularity));
        }
    }

	public static long calculateGranularityStart(String granularity, long times) {
		switch (granularity) {
			case Granularity.GRANULARITY_MINUTE:
				return times - times % GRANULARITY_MILLIS_INTERVAL_MINUTE;
			case Granularity.GRANULARITY_HOUR:
				return times - times % GRANULARITY_MILLIS_INTERVAL_HOUR;
			case Granularity.GRANULARITY_DAY:
				return times - times % GRANULARITY_MILLIS_INTERVAL_DAY;
			case Granularity.GRANULARITY_MONTH:
				throw new RuntimeException("Not support start times calculate when granularity is month.");
			default:
				throw new RuntimeException(String.format(INVALID_GRANULARITY_ERR_FORMAT, granularity));
		}
	}

    public static boolean isCalculateQuantiles(String granularity) {
        return !Granularity.GRANULARITY_MINUTE.equals(granularity);
    }


    public static void quantileCalculation(String granularity,List<Sample> continuousSamples, List<TDigestEntity> digests) {
        if (CollectionUtils.isEmpty(continuousSamples) || (!GRANULARITY_MINUTE.equals(granularity) && CollectionUtils.isEmpty(digests))) {
            return;
        }
        Map<Date,TDigestEntity> tDigestEntityMap = digests.stream().collect(Collectors.toMap(TDigestEntity::getDate, Function.identity()));
        String[] metricKeys = {
                MetricCons.SS.VS.F_REPLICATE_LAG
        };

        String[] digest95Keys = {
                MetricCons.SS.VS.F_95TH_REPLICATE_LAG
        };

        String[] digest99Keys = {
                MetricCons.SS.VS.F_99TH_REPLICATE_LAG
        };
        long start = continuousSamples.get(0).getDate().getTime() / 1000;

        long start95,start99;

        if(GRANULARITY_MINUTE.equals(granularity)) {
            start95 = start + (5 * 60);
            start99 = start + (10 * 60);
        }else{
            start95 = start + (60 * 60) ;
            start99 = start + (120 * 60) ;
        }

        // Process each metric
        for (int metricIdx = 0; metricIdx < digest95Keys.length; metricIdx++) {
            String metricKey = metricKeys[metricIdx];
            String digest95Key = digest95Keys[metricIdx];
            String digest99Key = digest99Keys[metricIdx];

            // Create TDigest for merging
            TDigest digest95 = TDigest.createMergingDigest(100);
            TDigest digest99 = TDigest.createMergingDigest(100);
            for (int i = 0; i < continuousSamples.size(); i++) {
                Sample sample = continuousSamples.get(i);
                long time = sample.getDate().getTime() / 1000;
                Map<String, Number> vs = sample.getVs();
                if(GRANULARITY_MINUTE.equals(granularity)) {
                    if (vs != null && vs.containsKey(metricKey)) {
                        Number value = vs.get(metricKey);
                        if (value != null) {
                            digest95.add(value.doubleValue());
                            digest99.add(value.doubleValue());
                        }
                    }
                }else {
                    TDigestEntity digestEntity = tDigestEntityMap.get(sample.getDate());
                    Map<String, byte[]> digestMap = digestEntity.getDigest();
                    if (digestMap != null) {
                        // Merge 95th percentile digest
                        if (digestMap.containsKey(digest95Key)) {
                            byte[] digestBytes = digestMap.get(digest95Key);
                            if (digestBytes != null && digestBytes.length > 0) {
                                ByteBuffer buffer = ByteBuffer.wrap(digestBytes);
                                MergingDigest digest = MergingDigest.fromBytes(buffer);
                                digest95.add(digest);
                            }
                        }
                        // Merge 99th percentile digest
                        if (digestMap.containsKey(digest99Key)) {
                            byte[] digestBytes = digestMap.get(digest99Key);
                            if (digestBytes != null && digestBytes.length > 0) {
                                ByteBuffer buffer = ByteBuffer.wrap(digestBytes);
                                MergingDigest digest = MergingDigest.fromBytes(buffer);
                                digest99.add(digest);
                            }
                        }
                    }
                }
                // After 1 hour (60 minutes), calculate 95th percentile
                if ((time >= start95 || (i == continuousSamples.size() - 1 && time >= start95 - 10)) && null != vs) {
                    double percentile95Value = digest95.quantile(0.95);
                    vs.put(digest95Key, percentile95Value);

                }

                // After 2 hours (120 minutes), calculate 99th percentile
                if ((time >= start99 || (i == continuousSamples.size() - 1 && time >= start99 - 10))  && null != vs) {
                    double percentile99Value = digest99.quantile(0.99);
                    vs.put(digest99Key, percentile99Value);
                }
            }

        }
    }
}
