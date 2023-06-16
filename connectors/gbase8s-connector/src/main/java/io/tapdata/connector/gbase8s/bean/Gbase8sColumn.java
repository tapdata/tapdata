package io.tapdata.connector.gbase8s.bean;

import io.tapdata.common.CommonColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;

/**
 * @author Jarad
 * @date 2022/4/20
 */
public class Gbase8sColumn extends CommonColumn {

    public Gbase8sColumn(DataMap dataMap) {
        this.columnName = dataMap.getString("colname");
        this.dataType = getDataType(dataMap); //'dataType' with precision and scale (postgres has its function)
//        this.dataType = dataMap.getString("data_type"); //'data_type' without precision or scale
        this.nullable = dataMap.getString("is_nullable");
        this.remarks = dataMap.getString("comments");
        //create table in target has no need to set default value
        this.columnDefaultValue = null;
//        this.columnDefaultValue = getDefaultValue(dataMap.getString("column_default"));
    }

    @Override
    public TapField getTapField() {
        return new TapField(this.columnName, this.dataType).nullable(this.isNullable()).
                defaultValue(columnDefaultValue).comment(this.remarks);
    }

    @Override
    protected Boolean isNullable() {
        return "YES".equals(this.nullable);
    }

    private String getDataType(DataMap dataMap) {
        String originType = dataMap.getString("coltypename");
        if (originType.startsWith("NVCHAR")) {
            return originType.replace("NVCHAR", "NVARCHAR");
        }
        switch (originType) {
            case "BOOL":
                return "BOOLEAN";
            case "FLOAT":
                return "FLOAT(" + dataMap.getString("collength") + ")";
            case "DATETIME":
                return dataMap.getString("coltypename2");
            case "DECIMAL":
                int colLength = Integer.parseInt(dataMap.getString("collength"));
                String dataType = "DECIMAL(" + (colLength / 256);
                if (colLength % 256 == 255) {
                    dataType += ")";
                } else {
                    dataType += "," + (colLength % 256) + ")";
                }
                return dataType;
            default:
                return originType;
        }
    }

}
