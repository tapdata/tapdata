package io.tapdata.flow.engine.V2.filter;

import com.tapdata.tm.commons.schema.Field;
import io.tapdata.entity.schema.TapField;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TapRecordSkipDetectorTest {
    TapRecordSkipDetector skipDetector;
    @BeforeEach
    void init() {
        new TapRecordSkipDetector();
        skipDetector = mock(TapRecordSkipDetector.class);
        doCallRealMethod().when(skipDetector).setIsomorphism(anyBoolean());
        skipDetector.setIsomorphism(true);
        when(skipDetector.skip(any(TapField.class))).thenCallRealMethod();
        when(skipDetector.skip(null)).thenCallRealMethod();
    }

    @Nested
    class SkipTest {
        TapField field;
        @BeforeEach
        void init() {
            field = mock(TapField.class);
        }
        @Test
        void testSkip() {
            when(field.getCreateSource()).thenReturn(Field.SOURCE_MANUAL);
            boolean skip = skipDetector.skip(field);
            Assertions.assertFalse(skip);
            verify(field, times(2)).getCreateSource();
        }

        @Test
        void testSkipNullCreateSourceInField() {
            when(field.getCreateSource()).thenReturn(null);
            boolean skip = skipDetector.skip(field);
            Assertions.assertTrue(skip);
            verify(field, times(1)).getCreateSource();
        }

        @Test
        void testSkipNotManualCreateSourceInField() {
            when(field.getCreateSource()).thenReturn(Field.SOURCE_JOB_ANALYZE);
            boolean skip = skipDetector.skip(field);
            Assertions.assertTrue(skip);
            verify(field, times(2)).getCreateSource();
        }

        @Test
        void testSkipNotIsomorphism() {
            skipDetector.setIsomorphism(false);
            //when(skipDetector.isomorphism).thenReturn(false);
            when(field.getCreateSource()).thenReturn(Field.SOURCE_JOB_ANALYZE);
            boolean skip = skipDetector.skip(field);
            Assertions.assertTrue(skip);
            verify(field, times(0)).getCreateSource();
        }

        @Test
        void testSkipNullField() {
            when(field.getCreateSource()).thenReturn(Field.SOURCE_JOB_ANALYZE);
            boolean skip = skipDetector.skip(null);
            Assertions.assertTrue(skip);
            verify(field, times(0)).getCreateSource();
        }
    }
}