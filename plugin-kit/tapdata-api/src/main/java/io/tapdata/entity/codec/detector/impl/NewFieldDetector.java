package io.tapdata.entity.codec.detector.impl;

import io.tapdata.entity.codec.detector.TapDetector;
import io.tapdata.entity.schema.TapField;

public interface NewFieldDetector extends TapDetector {
    void detected(TapField newField);
}
