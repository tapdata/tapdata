package io.tapdata.flow.engine.V2.filter;

import com.tapdata.tm.commons.schema.Field;
import io.tapdata.entity.codec.detector.TapSkipper;
import io.tapdata.entity.schema.TapField;

public class TapRecordSkipDetector implements TapSkipper {
    protected boolean isomorphism;

    public TapRecordSkipDetector(boolean isomorphism) {
        this.isomorphism = isomorphism;
    }

    public TapRecordSkipDetector() {
        this(true);
    }

    @Override
    public boolean skip(TapField field) {
        return false;
        //是否同构，不是同构就就返回false继续执行codec, 是同构就继续下面的判断
//        if (!isomorphism) return false;
        //是同构，如果这个字段是手动修改改过表结构的结果就返回false需要走codec, 否则不走codec
//        return !(null != field && null != field.getCreateSource() && Field.SOURCE_MANUAL.equalsIgnoreCase(field.getCreateSource()));
    }

    protected void setIsomorphism(boolean isomorphism) {
        this.isomorphism = isomorphism;
    }
}