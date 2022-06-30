package io.tapdata.kit;

import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * tools for ResultSet
 *
 * @author Jarad
 * @date 2022/5/29
 */
public class DbKit {

    /**
     * get all data from ResultSet starting with current row
     *
     * @param resultSet ResultSet
     * @return list<Map>
     */
    public static List<DataMap> getDataFromResultSet(ResultSet resultSet) {
        List<DataMap> list = TapSimplify.list();
        try {
            if (EmptyKit.isNotNull(resultSet)) {
                List<String> columnNames = getColumnsFromResultSet(resultSet);
                //cannot replace with while resultSet.next()
                while (!resultSet.isAfterLast() && resultSet.getRow() > 0) {
                    list.add(getRowFromResultSet(resultSet, columnNames));
                    resultSet.next();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * get current row
     *
     * @param resultSet   ResultSet
     * @param columnNames column names of ResultSet
     * @return Map
     */
    public static DataMap getRowFromResultSet(ResultSet resultSet, Collection<String> columnNames) {
        DataMap map = DataMap.create();
        try {
            if (EmptyKit.isNotNull(resultSet) && resultSet.getRow() > 0) {
                for (String col : columnNames) {
                    map.put(col, resultSet.getObject(col));
                }
                return map;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * get column names from ResultSet
     *
     * @param resultSet ResultSet
     * @return List<String>
     */
    public static List<String> getColumnsFromResultSet(ResultSet resultSet) {
        //get all column names
        List<String> columnNames = new ArrayList<>();
        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                columnNames.add(resultSetMetaData.getColumnName(i));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columnNames;
    }

    public static byte[] BlobToBytes(Blob blob) {
        BufferedInputStream bis = null;
        try {
            bis = new BufferedInputStream(blob.getBinaryStream());
            byte[] bytes = new byte[(int) blob.length()];
            int len = bytes.length;
            int offset = 0;
            int read;
            while (offset < len && (read = bis.read(bytes, offset, len - offset)) > 0) {
                offset += read;
            }
            return bytes;
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            if (bis != null) {
                try {
                    bis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
