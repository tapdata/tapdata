package io.tapdata.kit;

import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

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
                while (resultSet.next()) {
                    list.add(getRowFromResultSet(resultSet, columnNames));
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
                String[] columnNameArr = resultSetMetaData.getColumnName(i).split("\\.");
                String substring = columnNameArr[columnNameArr.length - 1];
                columnNames.add(substring);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columnNames;
    }

    public static List<String> getColumnTypesFromResultSet(ResultSet resultSet) {
        //get all column typeNames
        List<String> columnTypeNames = new ArrayList<>();
        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                columnTypeNames.add(resultSetMetaData.getColumnTypeName(i));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columnTypeNames;
    }

    public static byte[] blobToBytes(Blob blob) {
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

    public static String clobToString(Clob clob) {
        String re = "";
        try (Reader is = clob.getCharacterStream(); BufferedReader br = new BufferedReader(is)) {
            String s = br.readLine();
            StringBuilder sb = new StringBuilder();
            while (s != null) {
                sb.append(s);
                s = br.readLine();
            }
            re = sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return re;
    }

    public static boolean ignoreCreateIndex(TapIndex exists, TapIndex created) {
        if (!exists.isUnique() && created.isUnique()) {
            return false;
        }
        return exists.getIndexFields().stream().map(TapIndexField::getName).collect(Collectors.toList())
                .equals(created.getIndexFields().stream().map(TapIndexField::getName).collect(Collectors.toList()));
    }

    public static String buildIndexName(String table) {
        return "TAPIDX_" + table.substring(Math.max(table.length() - 10, 0)) + UUID.randomUUID().toString().replaceAll("-", "").substring(20);
    }

    public static <T> List<List<T>> splitToPieces(List<T> data, int eachPieceSize) {
        if (EmptyKit.isEmpty(data)) {
            return new ArrayList<>();
        }
        if (eachPieceSize <= 0) {
            throw new IllegalArgumentException("Param Error");
        }
        List<List<T>> result = new ArrayList<>();
        for (int index = 0; index < data.size(); index += eachPieceSize) {
            result.add(data.stream().skip(index).limit(eachPieceSize).collect(Collectors.toList()));
        }
        return result;
    }
}
