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
        return !(isomorphism && null != field && null != field.getCreateSource() && Field.SOURCE_MANUAL.equalsIgnoreCase(field.getCreateSource()));
    }

    protected void setIsomorphism(boolean isomorphism) {
        this.isomorphism = isomorphism;
    }
}