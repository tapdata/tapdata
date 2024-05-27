package com.tapdata.entity.dataflow.batch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class BatchOffsetTest {
    @Nested
    class CloneTest {
        @Test
        void cloneNormal() throws CloneNotSupportedException {
            BatchOffset batchOffset = new BatchOffset();
            Object clone = batchOffset.clone();
            Assertions.assertEquals(BatchOffset.class.getName(), clone.getClass().getName());
            BatchOffset offset= (BatchOffset)clone;
            Assertions.assertNull(offset.getStatus());
            Assertions.assertNull(offset.getOffset());
        }
        @Test
        void cloneOffsetIsMap() throws CloneNotSupportedException {
            BatchOffset batchOffset = new BatchOffset();
            Map<String, Object> o = new HashMap<>();
            batchOffset.setOffset(o);
            Object clone = batchOffset.clone();
            Assertions.assertEquals(BatchOffset.class.getName(), clone.getClass().getName());
            BatchOffset offset= (BatchOffset)clone;
            Assertions.assertNull(offset.getStatus());
            Assertions.assertNotNull(offset.getOffset());
            Assertions.assertEquals(HashMap.class.getName(), offset.getOffset().getClass().getName());
        }

        @Test
        void cloneOffsetNotMapAndNotSerializable() throws CloneNotSupportedException {
            BatchOffset batchOffset = new BatchOffset();
            batchOffset.setOffset(new TestOffset());
            Object clone = batchOffset.clone();
            Assertions.assertEquals(BatchOffset.class.getName(), clone.getClass().getName());
            BatchOffset offset= (BatchOffset)clone;
            Assertions.assertNull(offset.getStatus());
            Assertions.assertNotNull(offset.getOffset());
            Assertions.assertEquals(TestOffset.class.getName(), offset.getOffset().getClass().getName());
        }

        class TestOffset {

        }
    }
}