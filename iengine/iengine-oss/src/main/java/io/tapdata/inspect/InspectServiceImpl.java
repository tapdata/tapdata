package io.tapdata.inspect;

import com.tapdata.entity.Connections;
import com.tapdata.entity.inspect.*;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.SettingService;

import java.util.List;

public class InspectServiceImpl implements InspectService{

    @Override
    public void init(ClientMongoOperator clientMongoOperator, SettingService settingService) {

    }

    @Override
    public void updateStatus(String id, InspectStatus status, String msg) {

    }

    @Override
    public void upsertInspectResult(InspectResult inspectResult, boolean excludeInspect) {

    }

    @Override
    public void upsertInspectResult(InspectResult inspectResult) {

    }

    @Override
    public InspectResult getLastDifferenceInspectResult(String firstCheckId) {
        return null;
    }

    @Override
    public InspectResult getLastInspectResult(String inspectId) {
        return null;
    }

    @Override
    public InspectResult getInspectResultById(String inspectResultId) {
        return null;
    }

    @Override
    public List<Connections> getInspectConnectionsById(Inspect inspect) {
        return null;
    }

    @Override
    public void insertInspectDetails(List<InspectDetail> details) {

    }

    @Override
    public void onInspectStopped(Inspect inspect) {

    }

    @Override
    public void doInspectStop(String inspectId) {

    }

    @Override
    public void inspectHeartBeat(String id) {

    }

    @Override
    public Inspect getInspectById(String id) {
        return null;
    }
}
