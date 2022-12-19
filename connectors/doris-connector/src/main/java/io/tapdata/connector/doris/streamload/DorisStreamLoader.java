package io.tapdata.connector.doris.streamload;

import io.tapdata.connector.doris.DorisContext;
import io.tapdata.connector.doris.bean.DorisConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.connector.doris.streamload.exception.DorisRuntimeException;
import io.tapdata.connector.doris.streamload.exception.StreamLoadException;
import io.tapdata.connector.doris.streamload.rest.models.RespContent;
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
import org.springframework.util.Assert;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.writeListResult;

/**
 * @Author dayun
 * @Date 7/14/22
 */
public class DorisStreamLoader {
    private static final String TAG = DorisStreamLoader.class.getSimpleName();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load";
    private static final String LABEL_PREFIX_PATTERN = "tapdata_%s_%s";
    private static final int MAX_FLUSH_INTERVAL = 10000;
    private static final int MAX_FLUSH_BATCH_SIZE = 5000;

    private DorisContext dorisContext;
    private CloseableHttpClient httpClient;
    private RecordStream recordStream;

    private ExecutorService executorService;

    private Future<CloseableHttpResponse> pendingLoadFuture;

    private boolean loadBatchFirstRecord;
    private int size;
    private AtomicInteger lastEventFlag;

    public DorisStreamLoader(DorisContext dorisContext, CloseableHttpClient httpClient) {
        this.dorisContext = dorisContext;
        this.httpClient = httpClient;

        this.recordStream = new RecordStream(Constants.CACHE_BUFFER_SIZE, Constants.CACHE_BUFFER_COUNT);
        this.executorService = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());

        this.loadBatchFirstRecord = true;
        this.size = 0;

        this.lastEventFlag = new AtomicInteger(0);
    }

    public synchronized void writeRecord(final List<TapRecordEvent> tapRecordEvents, final TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws IOException {
        TapLogger.info(TAG, "batch events length is: {}", tapRecordEvents.size());
        WriteListResult<TapRecordEvent> listResult = writeListResult();
        int index =0;
        boolean before_is_null =false;
        for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
            byte[] bytes = MessageSerializer.serialize(table, tapRecordEvent);
            if (needFlush(tapRecordEvent, bytes.length)) {
                int lastFlag = this.lastEventFlag.get();
                RespContent flushResult = flush();
                incrementFlushResult(flushResult, listResult, lastFlag, before_is_null);
            }

            if(tapRecordEvent instanceof  TapUpdateRecordEvent && index <1){
                final TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) tapRecordEvent;
                final Map<String, Object> before = updateRecordEvent.getBefore();
                if(before == null){
                    before_is_null = true;
                }
                index++;
            }
            if (lastEventFlag.get() == 0) {
                startLoad(table, this.dorisContext.getDorisConfig(), tapRecordEvent);
            }
            writeRecord(bytes);
        }
        int lastFlag = this.lastEventFlag.get();
        RespContent lastFlushResult = flush();
        incrementFlushResult(lastFlushResult, listResult, lastFlag,before_is_null);
        writeListResultConsumer.accept(listResult);
    }

    private void incrementFlushResult(RespContent respContent, WriteListResult<TapRecordEvent> listResult, int lastFlag,boolean before_is_null) {
        long handledRows = respContent.getNumberLoadedRows();
        // todo 这个计数可能需要考虑upsert的情况，前置处理upsert
        if (OperationType.INSERT.code == lastFlag) {
            listResult.incrementInserted(handledRows);
        } else if (OperationType.UPDATE.code == lastFlag) {
            if (respContent.isSuccess()) {
                if(!before_is_null) {
                    listResult.incrementModified(handledRows / 2);
                }else {
                    listResult.incrementModified(handledRows);
                }
            } else {
                listResult.incrementModified(0);
            }
        } else if (OperationType.DELETE.code == lastFlag) {
            listResult.incrementRemove(handledRows);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public void writeRecord(byte[] record) throws IOException {
        if (loadBatchFirstRecord) {
            loadBatchFirstRecord = false;
        } else {
            recordStream.write(Constants.LINE_DELIMITER_DEFAULT.getBytes(StandardCharsets.UTF_8));
        }
        recordStream.write(record);
        size += 1;
    }

    public void startLoad(final TapTable table, final DorisConfig config, final TapRecordEvent recordEvent) {
        try {
            loadBatchFirstRecord = true;
            final String loadUrl = buildLoadUrl(config.getDorisHttp(), config.getDatabase(), table.getId());
            final String prefix = buildPrefix(table.getId());

            String label = prefix + "_" + recordEvent.getTime();
            List<String> columns = new ArrayList<>();
            for (Map.Entry<String, TapField> entry : table.getNameFieldMap().entrySet()) {
                columns.add(entry.getKey());
            }
            // add the DORIS_DELETE_SIGN at the end of the column
            columns.add(Constants.DORIS_DELETE_SIGN);
            HttpPutBuilder putBuilder = new HttpPutBuilder();
            recordStream.startInput();
            TapLogger.info(TAG, "stream load started for {}", label);
            InputStreamEntity entity = new InputStreamEntity(recordStream);
            putBuilder.setUrl(loadUrl)
                    // 前端表单传出来的值和tdd json加载的值可能有差别，如前端传的pwd可能是null，tdd的是空字符串
                    .baseAuth(config.getUser(), config.getPassword())
                    .addCommonHeader()
                    .addColumns(columns)
                    .enableDelete()
                    .setEntity(entity);
            pendingLoadFuture = executorService.submit(() -> {
                TapLogger.info(TAG, "start execute load, headers: {}", putBuilder.header);
                return httpClient.execute(putBuilder.build());
            });
            lastEventFlag.set(OperationType.getOperationFlag(recordEvent));
        } catch (Exception e) {
            TapLogger.error(TAG, e.getMessage());
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

    public RespContent flush() {
        // the stream is not started yet, no response to get
        if (lastEventFlag.get() == 0) {
            return null;
        }
        try {
            recordStream.endInput();
            TapLogger.info(TAG, "stream load stopped");
            Assert.notNull(pendingLoadFuture, "pendingLoadFuture of DorisStreamLoad should never be null");
            lastEventFlag.set(0);
            size = 0;
            RespContent respContent = handlePreCommitResponse(pendingLoadFuture.get());
            pendingLoadFuture = null;
            return respContent;
        } catch (Exception e) {
            recordStream.init();
            throw new DorisRuntimeException(e);
        }
    }

    public void shutdown() {
        this.stopLoad();
    }

    private String buildLoadUrl(final String dorisHttp, final String database, final String tableName) {
        return String.format(LOAD_URL_PATTERN, dorisHttp, database, tableName);
    }

    private String buildPrefix(final String tableName) {
        return String.format(LABEL_PREFIX_PATTERN, Thread.currentThread().getId(), tableName);
    }

    private boolean needFlush(TapRecordEvent recordEvent, int length) {
        int lastEventType = lastEventFlag.get();
        return lastEventType > 0 && lastEventType != OperationType.getOperationFlag(recordEvent)
                || this.size >= MAX_FLUSH_BATCH_SIZE
                || !recordStream.canWrite(length);
    }

    private void stopLoad() {
        flush();
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
}
