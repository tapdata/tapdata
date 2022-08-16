package io.tapdata.autoinspect.tester.samples;

import io.tapdata.autoinspect.connector.IConnector;
import io.tapdata.autoinspect.connector.IDataCursor;
import io.tapdata.autoinspect.entity.CompareEvent;
import io.tapdata.autoinspect.entity.CompareRecord;
import io.tapdata.autoinspect.tester.AutoInspectTester;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/9 17:22 Create
 */
public class SampleConnector implements IConnector {
    private static final Logger logger = LogManager.getLogger(SampleConnector.class);

    private final @NonNull IDataCursor<CompareRecord> dataCursor;
    private final @NonNull List<CompareRecord> recordList;

    public SampleConnector(@NonNull List<CompareRecord> recordList) {
        this.recordList = recordList;
        this.dataCursor = new SampleDataCursor(recordList);
    }

    @Override
    public ObjectId getConnId() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public IDataCursor<CompareRecord> queryAll(String tableName) {
        return dataCursor;
    }

    @Override
    public void increment(Function<List<CompareEvent>, Boolean> compareEventConsumer) {
        List<CompareEvent> records;
        while (!Thread.interrupted()) {
            records = new ArrayList<>();
//            for (int i = (int) (Math.random() * 10); i > 0; i--) {
            records.add(AutoInspectTester.createCompareEvent("test", String.valueOf((int) (Math.random() * 10)), new Date().toString()));
//            }
            if (!compareEventConsumer.apply(records)) {
                break;
            }
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException ignore) {
                break;
            }
        }
    }

    @Override
    public CompareRecord queryByKey(String tableName, Map<String, Object> keymap) {
//        boolean allTrue;
//        Object v1, v2;
//        for (CompareRecord record : recordList) {
//            allTrue = true;
//            for (Map.Entry<String, Object> en : keymap.entrySet()) {
//                v1 = record.getDataValue(k);
//                v2 = data.get(k);
//                if (null == v1) {
//                    if (null == v2) continue;
//                } else if (v1.equals(v2)) {
//                    continue;
//                }
//                allTrue = false;
//                break;
//            }
//
//            if (allTrue) {
//                return record;
//            }
//        }

        return null;
    }

    @Override
    public void close() throws Exception {
        logger.info("close source connector");
    }
}
