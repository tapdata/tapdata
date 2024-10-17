package com.tapdata.tm.commons.util;

import com.tapdata.tm.commons.schema.Schema;
import io.tapdata.entity.schema.partition.TapPartition;
import io.tapdata.entity.schema.partition.TapPartitionField;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/10/10 16:00
 */
public class PartitionTableFieldRenameOperatorTest {

    @Test
    public void test() {

        PartitionTableFieldRenameOperator operator = new PartitionTableFieldRenameOperator();
        operator.rename("test", "test_1");

        Schema schema = new Schema();
        schema.setPartitionInfo(new TapPartition());

        List<TapPartitionField> partitionFields = Collections.singletonList(new TapPartitionField());
        partitionFields.get(0).setName("test");

        schema.getPartitionInfo().setPartitionFields(partitionFields);

        operator.endOf(schema);

        Assertions.assertEquals("test_1", partitionFields.get(0).getName());

    }

}
