package com.tapdata.tm.commons.schema;

import io.tapdata.entity.schema.partition.TapPartition;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/10/16 08:00
 */
public class TableTest {

    @Test
    public void test() {
        Table table = new Table();
        table.setId(new ObjectId());
        table.setFileMeta(new FileMeta());
        table.setPartitionMasterTableId("test");
        table.setPartitionInfo(new TapPartition());

        Assertions.assertNotNull(table.getPartitionMasterTableId());
        Assertions.assertEquals("test", table.getPartitionMasterTableId());
        Assertions.assertNotNull(table.getPartitionInfo());
        Assertions.assertNotNull(table.getId());
    }
}
