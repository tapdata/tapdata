package io.tapdata.zoho.entity.webHook;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.BeanUtils;

import java.util.Map;

public class AddEventEntity extends EventBaseEntity<AddEventEntity> {
    public AddEventEntity create(){
        return new AddEventEntity();
    }

    @Override
    protected AddEventEntity event(Map<String, Object> issueEventData) {
//        BeanUtils utils = new BeanUtilsImpl();
        return BeanUtil.mapToBean(issueEventData,AddEventEntity.class,true, CopyOptions.create().ignoreError());
    }

    @Override
    public TapEvent outputTapEvent(String table) {
        return TapSimplify.insertRecordEvent(this.payload(),table).referenceTime(System.currentTimeMillis());
    }
}
