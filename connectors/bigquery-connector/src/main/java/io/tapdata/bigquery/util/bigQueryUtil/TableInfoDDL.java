package io.tapdata.bigquery.util.bigQueryUtil;

public class TableInfoDDL{

        private String ddl;
        /**
         * CREATE TABLE `vibrant-castle-366614.SchemaoOfJoinSet.JoinTestSchema`
         * (
         *   _id STRING,
         *   name STRING,
         *   type STRING,
         *   isTable STRING,
         *   isDel STRING,
         *   status STRING,
         *   times STRING,
         *   avactor STRING,
         *   description STRING,
         *   title STRING,
         *   teamName STRING,
         *   projectName STRING,
         *   priority STRING,
         *   workingHours STRING,
         *   createTime STRING,
         *   updateTime STRING,
         *   createBy STRING,
         *   updateBy STRING,
         *   bytes BYTES,
         *   bytess BYTES(10),
         *   integer INT64,
         *   float FLOAT64,
         *   numeric NUMERIC(15, 8),
         *   bignumeric BIGNUMERIC(55, 25),
         *   big BIGNUMERIC(76, 38),
         *   timestamp TIMESTAMP,
         *   date DATE,
         *   time TIME,
         *   datetime DATETIME,
         *   geography GEOGRAPHY,
         *   straact STRUCT<s STRING>,
         *   A NUMERIC)OPTIONS(description='',labels=[("","")]);
         * */
        public static TableInfoDDL create(String ddl){
            return new TableInfoDDL(ddl);
        }
        public TableInfoDDL(String ddl){
            this.ddl = ddl.replaceAll("\n","");
        }
        private final String tableNameRegex = "CREATE TABLE `";
        private String projectId;
        private String tableSetId;
        private String tableName;
        private String[] table;
        private String[] table(){
            String str = attribute(new String[]{"CREATE TABLE `"},"`(");
            String[] split = str.split("\\.");
            this.table = split;
            return split;
        }
        /**
         * @deprecated
         * */
        public String projectId(){
            if (null == table){
                this.table = this.table();
            }
            if (this.table.length == 3){
                return this.table[0];
            }
            int index = this.table.length - 3;
            return index>=0?this.table[index]:null;
        }
        public String tableSetId(){
            if (null != this.tableSetId) return tableSetId;
            if (null == table){
                this.table = this.table();
            }
            if (this.table.length == 3){
                return this.table[1];
            }
            return this.table[table.length-2];
        }
        public String tableName(){
            if (null != this.tableName) return tableName;
            if (null == table){
                this.table = this.table();
            }
            if (this.table.length == 3){
                return this.table[2];
            }
            return this.table[table.length-1];
        }

        public String description(){
            return attribute(new String[]{")OPTIONS(","description='"},"',");
        }

        private String attribute(String[] prefix,String suffix){
            int startIndex = -1;
            for (String s : prefix) {
                int indexNext = this.ddl.indexOf(s, startIndex);
                if (indexNext > -1) {
                    startIndex = this.ddl.indexOf(s, startIndex) + s.length();
                } else {
                    //can not find subString prefix from ddl string;
                    return null;
                }
            }
            int endIndex = this.ddl.indexOf(suffix,startIndex);
            if (endIndex < 0){
                //can not find subString suffix from ddl string
                return null;
            }
            StringBuilder builder = new StringBuilder();
            for (int i = startIndex; i < endIndex; i++) {
                char ch = this.ddl.charAt(i);
                builder.append(ch);
            }
            return builder.toString();
        }
    }