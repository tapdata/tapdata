//package io.tapdata.connector.doris;
//
//import com.google.common.collect.Lists;
//import com.google.common.collect.Sets;
//import io.tapdata.entity.schema.TapField;
//import io.tapdata.entity.schema.TapTable;
//import io.tapdata.kit.EmptyKit;
//import org.apache.commons.lang3.StringUtils;
//
//import java.util.Collection;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
///**
// * @Author dayun
// * @Date 7/14/22
// */
//public class DorisDDLInstance {
//    private static final DorisDDLInstance DDLInstance = new DorisDDLInstance();
//
//    public static DorisDDLInstance getInstance() {
//        return DDLInstance;
//    }
//
//    public String buildDistributedKey(Collection<String> primaryKeyNames) {
//        StringBuilder builder = new StringBuilder();
//        for (String fieldName : primaryKeyNames) {
//            builder.append(fieldName);
//            builder.append(',');
//        }
//        builder.delete(builder.length() - 1, builder.length());
//        return builder.toString();
//    }
//
//    public String buildColumnDefinition(final TapTable tapTable) {
//        final Map<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
//        Collection<String> pks = tapTable.primaryKeys(true);
//        if (EmptyKit.isEmpty(pks)) {
//            pks = tapTable.getNameFieldMap().keySet();
//        }
//        Set<String> pkSet = Sets.newHashSet();
//        final List<String> fieldStrs = Lists.newArrayList();
//        for (String pk : pks) {
//            final String fieldStr = concatFieldInCreateSql(nameFieldMap.get(pk));
//            fieldStrs.add(fieldStr);
//            pkSet.add(pk);
//        }
//        for (final String columnName : nameFieldMap.keySet()) {
//            if (pkSet.contains(columnName)) {
//                continue;
//            }
//            final String fieldStr = concatFieldInCreateSql(nameFieldMap.get(columnName));
//            if (StringUtils.isBlank(fieldStr)) {
//                continue;
//            }
//            fieldStrs.add(fieldStr);
//        }
//        return StringUtils.join(fieldStrs, ",");
//    }
//
//    private String concatFieldInCreateSql(final TapField tapField) {
//        List<String> fieldStrs = Lists.newArrayList();
//        if (tapField.getDataType() == null) {
//            return null;
//        }
//        fieldStrs.add(tapField.getName());
//        fieldStrs.add(tapField.getDataType());
//        Boolean nullable = tapField.getNullable();
//        if (nullable != null && !nullable) {
//            fieldStrs.add("NOT NULL");
//        } else {
//            fieldStrs.add("NULL");
//        }
//        if (tapField.getDefaultValue() != null) {
//            fieldStrs.add("DEFAULT");
//            fieldStrs.add("'" + tapField.getDefaultValue().toString() + "'");
//        }
//        return StringUtils.join(fieldStrs, " ");
//    }
//}
