package io.tapdata.inspect;

import com.tapdata.entity.Connections;
import com.tapdata.entity.inspect.*;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.SettingService;
import io.tapdata.exception.TapOssNonsupportFunctionException;

import java.util.List;

public class InspectServiceImpl implements InspectService{

    @Override
    public void init(ClientMongoOperator clientMongoOperator, SettingService settingService) {
        throw new TapOssNonsupportFunctionException();
    }

    @Override
    public void updateStatus(String id, InspectStatus status, String msg) {
        throw new TapOssNonsupportFunctionException();
    }

    @Override
    public void upsertInspectResult(InspectResult inspectResult, boolean excludeInspect) {
        throw new TapOssNonsupportFunctionException();
    }

    @Override
    public void upsertInspectResult(InspectResult inspectResult) {
        throw new TapOssNonsupportFunctionException();
    }

    @Override
    public InspectResult getLastDifferenceInspectResult(String inspectId, String firstCheckId) {
        throw new TapOssNonsupportFunctionException();
    }

    @Override
    public InspectResult getLastInspectResult(String inspectId) {
        throw new TapOssNonsupportFunctionException();
    }

    @Override
    public InspectResult getInspectResultById(String inspectResultId) {
        throw new TapOssNonsupportFunctionException();
    }

    @Override
    public List<Connections> getInspectConnectionsById(Inspect inspect) {
        throw new TapOssNonsupportFunctionException();
    }

    @Override
    public void insertInspectDetails(List<InspectDetail> details) {
        throw new TapOssNonsupportFunctionException();
    }

    @Override
    public void onInspectStopped(Inspect inspect) {
        throw new TapOssNonsupportFunctionException();
    }

    @Override
    public void doInspectStop(String inspectId) {
        throw new TapOssNonsupportFunctionException();
    }

    @Override
    public void inspectHeartBeat(String id) {
        throw new TapOssNonsupportFunctionException();
    }

    @Override
    public Inspect getInspectById(String id) {
        throw new TapOssNonsupportFunctionException();
    }

    @Override
    public void handleDiffInspect(Inspect inspect, long syncTaskDelay,InspectResult inspectResult) {
        throw new TapOssNonsupportFunctionException();
    }

    @Override
    public String getConnectionDatabaseType(String connectionId) {
        throw new TapOssNonsupportFunctionException();
    }
}
