package io.tapdata.zoho.entity.webHook.doMain;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.zoho.entity.webHook.EventBaseEntity;

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
    public TapEvent outputTapEvent(String table) {
        return TapSimplify.insertRecordEvent(this.payload(),table).referenceTime(System.currentTimeMillis());
    }
}
