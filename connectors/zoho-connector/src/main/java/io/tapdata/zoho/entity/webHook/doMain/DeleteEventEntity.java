package io.tapdata.zoho.entity.webHook.doMain;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.zoho.entity.webHook.EventBaseEntity;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.connectionMode.impl.CSVMode;

import java.util.Map;

public class DeleteEventEntity extends EventBaseEntity<DeleteEventEntity> {
    public static DeleteEventEntity create(){
        return new DeleteEventEntity();
    }

    @Override
    protected DeleteEventEntity event(Map<String, Object> issueEventData) {
        return BeanUtil.mapToBean(issueEventData,DeleteEventEntity.class,true, CopyOptions.create().ignoreError());
    }

    @Override
    public TapEvent outputTapEvent(String table, ConnectionMode instance) {
        return TapSimplify.deleteDMLEvent(
                instance instanceof CSVMode ? instance.attributeAssignmentSelf(this.payload()):this.payload()
                , table)
                .referenceTime(System.currentTimeMillis());
    }
}
