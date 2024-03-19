package io.tapdata.inspect;

import com.tapdata.entity.Connections;
import com.tapdata.entity.inspect.*;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.SettingService;
import io.tapdata.error.EngineExCode_33;
import io.tapdata.exception.TapOssNonsupportFunctionException;

import java.util.List;

public interface InspectService {

    default void startInspect(Inspect inspect) {
        throw new TapOssNonsupportFunctionException();
    }


    void init(ClientMongoOperator clientMongoOperator, SettingService settingService);
    void updateStatus(String id, InspectStatus status, String msg);
    void upsertInspectResult(InspectResult inspectResult, boolean excludeInspect);
    void upsertInspectResult(InspectResult inspectResult);
    InspectResult getLastDifferenceInspectResult(String firstCheckId);
    InspectResult getLastInspectResult(String inspectId);
    InspectResult getInspectResultById(String inspectResultId);
    List<Connections> getInspectConnectionsById(Inspect inspect);
    void insertInspectDetails(List<InspectDetail> details);
    void onInspectStopped(Inspect inspect);
    void doInspectStop(String inspectId);
    void inspectHeartBeat(String id);
    Inspect getInspectById(String id);

}
