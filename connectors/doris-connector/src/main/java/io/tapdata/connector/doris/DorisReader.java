//package io.tapdata.connector.doris;
//
//import io.tapdata.entity.schema.TapTable;
//import io.tapdata.entity.utils.DataMap;
//import io.tapdata.pdk.apis.context.TapConnectionContext;
//import io.tapdata.pdk.apis.entity.FilterResult;
//import io.tapdata.pdk.apis.entity.TapFilter;
//
//import java.sql.ResultSet;
//import java.sql.Statement;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Set;
//
///**
// * @Author dayun
// * @Date 7/14/22
// */
//public class DorisReader {
//    private TapConnectionContext tapConnectionContext;
//    private DorisJdbcContext dorisJdbcContext;
//    private static final DorisDMLInstance DMLInstance = DorisDMLInstance.getInstance();
//
//    public DorisReader(DorisJdbcContext dorisJdbcContext) {
//        this.dorisJdbcContext = dorisJdbcContext;
//        this.tapConnectionContext = dorisJdbcContext.getTapConnectionContext();
//    }
//
//    public List<FilterResult> queryByFilter(List<TapFilter> filters, TapTable tapTable) {
//        Set<String> columnNames = tapTable.getNameFieldMap().keySet();
//        List<FilterResult> filterResults = new LinkedList<>();
//        for (TapFilter filter : filters) {
//            String sql = "SELECT * FROM " + tapTable.getName() + " WHERE " + DMLInstance.buildKeyAndValue(tapTable, filter.getMatch(), "AND");
//            FilterResult filterResult = new FilterResult();
//            try (Statement statement = dorisJdbcContext.getConnection().createStatement();
//                 ResultSet resultSet = dorisJdbcContext.executeQuery(statement, sql)){
//                DataMap resultMap = new DataMap();
//                if (resultSet.next()) {
//                    for (String columnName : columnNames) {
//                        resultMap.put(columnName, resultSet.getObject(columnName));
//                    }
//                    filterResult.setResult(resultMap);
//                    break;
//                }
//            } catch (Exception e) {
//                filterResult.setError(e);
//            } finally {
//                filterResults.add(filterResult);
//            }
//        }
//        return filterResults;
//    }
//}
