package io.tapdata.observable.logging.cache;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.tapdata.constant.JSONUtil;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class MonitoringLogCodec {
    private static final Logger LOGGER = LogManager.getLogger(MonitoringLogCodec.class);
    private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";
    private static final String EMPTY_JSON_ARRAY = "[]";

    public void write(ValueOut valueOut, MonitoringLogsDto log) {
        writeAndMeasure(valueOut, log);
    }

    long writeAndMeasure(ValueOut valueOut, MonitoringLogsDto log) {
        String date = new SimpleDateFormat(DATE_PATTERN).format(log.getDate());
        String dynamicDescriptionParameters = dynamicDescriptionParameters(log);
        String logTags = String.join(",",
                CollectionUtils.isNotEmpty(log.getLogTags()) ? log.getLogTags() : new ArrayList<>(0));
        String data = data(log);

        valueOut.writeString(date);
        valueOut.writeString(log.getLevel());
        valueOut.writeString(log.getErrorStack());
        valueOut.writeString(log.getMessage());
        valueOut.writeString(log.getTaskId());
        valueOut.writeString(log.getTaskRecordId());
        valueOut.writeLong(log.getTimestamp());
        valueOut.writeString(log.getTaskName());
        valueOut.writeString(log.getNodeId());
        valueOut.writeString(log.getNodeName());
        valueOut.writeString(log.getErrorCode());
        valueOut.writeString(log.getFullErrorCode());
        valueOut.writeString(dynamicDescriptionParameters);
        valueOut.writeString(logTags);
        valueOut.writeString(data);

        return measure(log, date, dynamicDescriptionParameters, logTags, data);
    }

    long measure(MonitoringLogsDto log) {
        String date = new SimpleDateFormat(DATE_PATTERN).format(log.getDate());
        String dynamicDescriptionParameters = dynamicDescriptionParameters(log);
        String logTags = String.join(",",
                CollectionUtils.isNotEmpty(log.getLogTags()) ? log.getLogTags() : new ArrayList<>(0));
        String data = data(log);
        return measure(log, date, dynamicDescriptionParameters, logTags, data);
    }

    private long measure(MonitoringLogsDto log,
                         String date,
                         String dynamicDescriptionParameters,
                         String logTags,
                         String data) {
        return Long.BYTES
                + utf8Bytes(date)
                + utf8Bytes(log.getLevel())
                + utf8Bytes(log.getErrorStack())
                + utf8Bytes(log.getMessage())
                + utf8Bytes(log.getTaskId())
                + utf8Bytes(log.getTaskRecordId())
                + utf8Bytes(log.getTaskName())
                + utf8Bytes(log.getNodeId())
                + utf8Bytes(log.getNodeName())
                + utf8Bytes(log.getErrorCode())
                + utf8Bytes(log.getFullErrorCode())
                + utf8Bytes(dynamicDescriptionParameters)
                + utf8Bytes(logTags)
                + utf8Bytes(data);
    }

    public MonitoringLogsDto read(ValueIn valueIn) {
        MonitoringLogsDto.MonitoringLogsDtoBuilder builder = MonitoringLogsDto.builder();
        read(valueIn, builder);
        return builder.build();
    }

    public void read(ValueIn valueIn, MonitoringLogsDto.MonitoringLogsDtoBuilder builder) {
        builder.date(parseDate(valueIn.readString()));
        builder.level(valueIn.readString());
        builder.errorStack(valueIn.readString());
        builder.message(valueIn.readString());
        builder.taskId(valueIn.readString());
        builder.taskRecordId(valueIn.readString());
        builder.timestamp(valueIn.readLong());
        builder.taskName(valueIn.readString());
        builder.nodeId(valueIn.readString());
        builder.nodeName(valueIn.readString());
        builder.errorCode(valueIn.readString());
        builder.fullErrorCode(valueIn.readString());
        builder.dynamicDescriptionParameters(readDynamicDescriptionParameters(valueIn.readString()));

        String logTags = valueIn.readString();
        if (StringUtils.isNotBlank(logTags)) {
            builder.logTags(java.util.Arrays.asList(logTags.split(",")));
        }

        String data = valueIn.readString();
        if (StringUtils.isNotBlank(data)) {
            try {
                builder.data((Collection<? extends Map<String, Object>>) JSON.parseArray(
                        data,
                        (new HashMap<String, Object>()).getClass()));
            } catch (RuntimeException e) {
                LOGGER.error("Read log from cache queue failed, parse data JSON failed: {}", data, e);
            }
        }
    }

    private String dynamicDescriptionParameters(MonitoringLogsDto log) {
        String[] parameters = log.getDynamicDescriptionParameters();
        if (parameters == null || parameters.length == 0) {
            return EMPTY_JSON_ARRAY;
        }
        try {
            return JSONUtil.obj2Json(parameters);
        } catch (JsonProcessingException e) {
            LOGGER.error("Failed to convert dynamicDescriptionParameters to JSON", e);
            return EMPTY_JSON_ARRAY;
        }
    }

    private String[] readDynamicDescriptionParameters(String value) {
        try {
            return JSONUtil.json2POJO(value, String[].class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid dynamicDescriptionParameters JSON", e);
        }
    }

    private String data(MonitoringLogsDto log) {
        if (log.getData() == null) {
            return EMPTY_JSON_ARRAY;
        }
        SerializeConfig config = log.getSerializeConfig() != null
                ? log.getSerializeConfig()
                : SerializeConfig.getGlobalInstance();
        try {
            return JSON.toJSON(log.getData(), config).toString();
        } catch (RuntimeException e) {
            LOGGER.error("Convert monitoring log data to JSON failed", e);
            return EMPTY_JSON_ARRAY;
        }
    }

    private Date parseDate(String value) {
        try {
            return new SimpleDateFormat(DATE_PATTERN).parse(value);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid monitoring log date: " + value, e);
        }
    }

    private long utf8Bytes(String value) {
        return value == null ? 0L : value.getBytes(StandardCharsets.UTF_8).length;
    }
}
