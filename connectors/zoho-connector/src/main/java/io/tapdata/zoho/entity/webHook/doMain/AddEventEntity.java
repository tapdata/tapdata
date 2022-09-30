package io.tapdata.zoho.entity.webHook.doMain;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.zoho.entity.webHook.EventBaseEntity;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.connectionMode.impl.CSVMode;

import java.util.Map;

public class AddEventEntity extends EventBaseEntity<AddEventEntity> {
    public AddEventEntity create(){
        return new AddEventEntity();
    }

    @Override
    protected AddEventEntity event(Map<String, Object> issueEventData) {
        return BeanUtil.mapToBean(issueEventData,AddEventEntity.class,true, CopyOptions.create().ignoreError());
    }

    @Override
    public TapEvent outputTapEvent(String table, ConnectionMode instance) {
        return TapSimplify.insertRecordEvent(
                instance instanceof CSVMode ? instance.attributeAssignmentSelf(this.payload()):this.payload()
                ,table)
                .referenceTime(this.eventTime());
    }
}
