package com.tapdata.tm.utils;

import com.tapdata.tm.commons.schema.Schema;
import io.tapdata.entity.schema.TapTable;

import java.util.Objects;

public class PartitionTableUtil {
    private PartitionTableUtil() {}

    public static boolean checkIsMasterPartitionTable(TapTable table) {
        return Objects.nonNull(table.getPartitionInfo())
                && (Objects.isNull(table.getPartitionMasterTableId()) || table.getId().equals(table.getPartitionMasterTableId()));
    }

    public static boolean checkIsSubPartitionTable(TapTable table) {
        return Objects.nonNull(table.getPartitionInfo())
                && Objects.nonNull(table.getPartitionMasterTableId())
                && !table.getId().equals(table.getPartitionMasterTableId());
    }

    public static boolean checkIsMasterPartitionTable(Schema table) {
        return Objects.nonNull(table.getPartitionInfo())
                && (Objects.isNull(table.getPartitionMasterTableId()) || table.getId().equals(table.getPartitionMasterTableId()));
    }

    public static boolean checkIsSubPartitionTable(Schema table) {
        return Objects.nonNull(table.getPartitionInfo())
                && Objects.nonNull(table.getPartitionMasterTableId())
                && !table.getId().equals(table.getPartitionMasterTableId());
    }
}
