package io.tapdata.connector.selectdb;

import cn.hutool.core.lang.Assert;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.connector.selectdb.config.SelectDbConfig;
import io.tapdata.connector.selectdb.exception.SelectDbRunTimeException;
import io.tapdata.connector.selectdb.exception.StreamLoadException;
import io.tapdata.connector.selectdb.streamload.*;
import io.tapdata.connector.selectdb.streamload.rest.models.RespContent;
import io.tapdata.connector.selectdb.util.CopyIntoUtils;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.tapdata.entity.simplify.TapSimplify.toJson;


/**
 * Author:Skeet
 * Date: 2022/12/13
 **/
public class SelectDbStreamLoader {
    private static final String TAG = SelectDbStreamLoader.class.getSimpleName();
    private static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load";
    private static final String LABEL_PREFIX_PATTERN = "tapdata_%s_%s";
    private static final int MAX_FLUSH_BATCH_SIZE = 10000;
    private int size;
    private AtomicInteger lastEventFlag;
    private RecordStream recordStream;
    private boolean loadBatchFirstRecord;
    private ExecutorService executorService;
    private CloseableHttpClient httpClient;
    private SelectDbConfig selectDbConfig;
    private SelectDbContext selectDbContext;
    private Future<CloseableHttpResponse> pendingLoadFuture;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public SelectDbStreamLoader(SelectDbContext selectDbContext, CloseableHttpClient httpClient) {
        this.selectDbContext = selectDbContext;
        this.httpClient = httpClient;
    }

    public void shutdown() {
        this.stopLoad();
    }

    private void stopLoad() {
        flush();
    }

    public SelectDbStreamLoader(CloseableHttpClient httpClient, SelectDbConfig selectDbConfig) {
        this.httpClient = httpClient;
        this.selectDbConfig = selectDbConfig;
        this.recordStream = new RecordStream(Constants.CACHE_BUFFER_SIZE, Constants.CACHE_BUFFER_COUNT);
        this.size = 0;
        this.lastEventFlag = new AtomicInteger(0);
        this.selectDbContext = selectDbContext;
        this.executorService = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
        this.loadBatchFirstRecord = true;
    }

    public synchronized WriteListResult<TapRecordEvent> writeRecord(final List<TapRecordEvent> tapRecordEvents, final TapTable table) throws IOException {
        TapLogger.info(TAG, "batch events length is: {}", tapRecordEvents.size());
//        WriteListResult<TapRecordEvent> listResult = writeListResult();
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        List<Map<String, Object>> records = new ArrayList<>();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
            records.addAll(MessageSerializer.serializeMap(table, tapRecordEvent));
            if (tapRecordEvent instanceof TapInsertRecordEvent) {
                listResult.incrementInserted(1);
            } else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
                listResult.incrementModified(1);
            } else if (tapRecordEvent instanceof TapDeleteRecordEvent) {
                listResult.incrementRemove(1);
            } else {
                listResult.addError(tapRecordEvent, new Exception("Event type \"" + tapRecordEvent.getClass().getSimpleName() + "\" not support: " + tapRecordEvent));
            }
        }
        String s = toJson(records);
        dataOutputStream.write(s.getBytes(StandardCharsets.UTF_8));
        dataOutputStream.write(Constants.LINE_DELIMITER_DEFAULT.getBytes(StandardCharsets.UTF_8));
        final byte[] finalBytes = byteArrayOutputStream.toByteArray();
        CopyIntoUtils.upload(finalBytes);
        CopyIntoUtils.copyInto(table);
        return listResult;
    }

    private volatile boolean isStop;

    public void stop() {
        this.isStop = true;
    }

    private boolean needFlush(TapRecordEvent recordEvent, int length) {
        int lastEventType = lastEventFlag.get();
        return lastEventType > 0 && lastEventType != OperationType.getOperationFlag(recordEvent)
                || this.size >= MAX_FLUSH_BATCH_SIZE
                || !recordStream.canWrite(length);
    }

    public enum OperationType {
        INSERT(1, "insert"),
        UPDATE(2, "update"),
        DELETE(3, "delete");

        private int code;
        private String desc;

        OperationType(int code, String desc) {
            this.code = code;
            this.desc = desc;
        }

        static int getOperationFlag(TapRecordEvent recordEvent) {
            if (recordEvent instanceof TapInsertRecordEvent) {
                return INSERT.code;
            } else if (recordEvent instanceof TapUpdateRecordEvent) {
                return UPDATE.code;
            } else if (recordEvent instanceof TapDeleteRecordEvent) {
                return DELETE.code;
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }

    public RespContent flush() {
        // the stream is not started yet, no response to get
        if (lastEventFlag.get() == 0) {
            return null;
        }
        try {
            recordStream.endInput();
            TapLogger.info(TAG, "stream load stopped");
            Assert.notNull(pendingLoadFuture, "pendingLoadFuture of SelectDBStreamLoad should never be null");
            lastEventFlag.set(0);
            size = 0;
            RespContent respContent = handlePreCommitResponse(pendingLoadFuture.get());
            pendingLoadFuture = null;
            return respContent;
        } catch (Exception e) {
            recordStream.init();
            throw new SelectDbRunTimeException(e);
        }
    }

    public RespContent handlePreCommitResponse(CloseableHttpResponse response) throws Exception {
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200 || response.getEntity() == null) {
            throw new StreamLoadException("Stream load error: " + response.getStatusLine().toString());
        }
        String loadResult = EntityUtils.toString(response.getEntity());

        TapLogger.debug(TAG, "load Result {}", loadResult);
        RespContent respContent = OBJECT_MAPPER.readValue(loadResult, RespContent.class);
        if (!respContent.isSuccess()) {
            throw new StreamLoadException("Stream load failed | Error: " + loadResult);
        }
        return respContent;
    }

    private String buildLoadUrl(final String selectDbHttp, final String database, final String tableName) {
        return String.format(LOAD_URL_PATTERN, selectDbHttp, database, tableName);
    }

    private String buildPrefix(final String tableName) {
        return String.format(LABEL_PREFIX_PATTERN, Thread.currentThread().getId(), tableName);
    }
}
