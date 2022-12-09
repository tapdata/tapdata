package io.tapdata.bigquery.util.bigQueryUtil;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    public static void main(String[] args) {
            String s = "DECLARE exits INT64;SET exits = (select 1 from `vibrant-castle-366614`.`SchemaoOfJoinSet`.`TEST_coding_Issues` WHERE `Code`=187 AND `ProjectName`='tapdata' AND `TeamName`='tapdata' );  if exits = 1 then   update `vibrant-castle-366614`.`SchemaoOfJoinSet`.`TEST_coding_Issues` SET `Code`=187,`ProjectName`='tapdata',`TeamName`='tapdata',`ParentType`='DEFECT',`Type`='DEFECT',`IssueTypeDetail`= JSON '{\"Description\":\"缺陷是指软件不符合最初定义的业务需求的现象，缺陷管理用于跟踪这些问题和错误。\",\"IssueType\":\"DEFECT\",\"Name\":\"缺陷\",\"IsSystem\":true,\"Id\":104983}',`IssueTypeId`=104983,`Name`='Take 1 postgresql到mongo',`Description`='1、创建postgresql源到mongodb目标的连接\\n2、启动任务后，已接收、已插入的统计数据显示为1286，而已处理统计数据显示为640，源库中，customer表的记录数为640，policy表的记录数为646',`IterationId`=317840,`IssueStatusId`=1587681,`IssueStatusName`='已关闭',`IssueStatusType`='COMPLETED',`Priority`='2',`Assignee`= JSON '{\"Status\":1,\"Email\":\"\",\"TeamId\":0,\"GlobalKey\":\"\",\"TeamGlobalKey\":\"\",\"Avatar\":\"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-VCICtwoLaTGkAjteNzmr.jpg\",\"Name\":\"Sam\",\"Phone\":\"\",\"Id\":222040}',`StartDate`=0,`DueDate`=0,`WorkingHours`=0.0,`Creator`= JSON '{\"Status\":1,\"Email\":\"\",\"TeamId\":0,\"GlobalKey\":\"\",\"TeamGlobalKey\":\"\",\"Avatar\":\"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-awaKQHhUuOlaCtwiOkVX.jpg\",\"Name\":\"Steve\",\"Phone\":\"\",\"Id\":222025}',`StoryPoint`='',`CreatedAt`=1576557640000,`UpdatedAt`=1667047480000,`CompletedAt`=0,`ProjectModule`= JSON '{\"Name\":\"\",\"Id\":0}',`Watchers`= JSON '[{\"Status\":1,\"Email\":\"\",\"TeamId\":0,\"GlobalKey\":\"\",\"TeamGlobalKey\":\"\",\"Avatar\":\"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-awaKQHhUuOlaCtwiOkVX.jpg\",\"Name\":\"Steve\",\"Phone\":\"\",\"Id\":222025},{\"Status\":1,\"Email\":\"\",\"TeamId\":0,\"GlobalKey\":\"\",\"TeamGlobalKey\":\"\",\"Avatar\":\"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-VCICtwoLaTGkAjteNzmr.jpg\",\"Name\":\"Sam\",\"Phone\":\"\",\"Id\":222040}]',`Labels`= JSON '[]',`Files`= JSON '[{\"Size\":129164,\"Url\":\"/api/project/342870/files/2647827/download\",\"Name\":\"4-20191217124036.png\",\"Type\":2,\"FileId\":2647827}]',`RequirementType`='{\"Name\":\"\",\"Id\":0}',`DefectType`= JSON '{\"Name\":\"功能缺陷\",\"IconUrl\":\"\",\"Id\":32068367}',`CustomFields`= JSON '[{\"Id\":36843698,\"ValueString\":\"385881\"},{\"Id\":36843699,\"ValueString\":\"1.0\"},{\"Id\":36843715,\"ValueString\":\"[\\\"385944\\\",\\\"385948\\\"]\"},{\"Id\":36843722,\"ValueString\":\"385964\"}]',`ThirdLinks`= JSON '[]',`SubTasks`= JSON '[]',`Parent`= JSON '{\"Assignee\":{\\\"Status\\\":0,\\\"Email\\\":\\\"\\\",\\\"TeamId\\\":0,\\\"GlobalKey\\\":\\\"\\\",\\\"TeamGlobalKey\\\":\\\"\\\",\\\"Avatar\\\":\\\"\\\",\\\"Name\\\":\\\"\\\",\\\"Phone\\\":\\\"\\\",\\\"Id\\\":0},\"Priority\":\"\",\"IssueStatusName\":\"\",\"IssueStatusId\":0,\"Code\":0,\"Name\":\"\",\"IssueStatusType\":\"\",\"Type\":\"\",\"IssueTypeDetail\":{\\\"Description\\\":\\\"\\\",\\\"IssueType\\\":\\\"\\\",\\\"Name\\\":\\\"\\\",\\\"IsSystem\\\":false,\\\"Id\\\":0}}',`Epic`= JSON '{\"Assignee\":{\\\"Status\\\":0,\\\"Email\\\":\\\"\\\",\\\"TeamId\\\":0,\\\"GlobalKey\\\":\\\"\\\",\\\"TeamGlobalKey\\\":\\\"\\\",\\\"Avatar\\\":\\\"\\\",\\\"Name\\\":\\\"\\\",\\\"Phone\\\":\\\"\\\",\\\"Id\\\":0},\"Priority\":\"\",\"IssueStatusName\":\"\",\"IssueStatusId\":0,\"Code\":0,\"Name\":\"\",\"Type\":\"\"}',`Iteration`= JSON '{\"Status\":\"WAIT_PROCESS\",\"Code\":14105,\"Name\":\"SDK Sprint #1\",\"Id\":0}'  WHERE `Code`=187 AND `ProjectName`='tapdata' AND `TeamName`='tapdata' ; ELSE   insert into `vibrant-castle-366614`.`SchemaoOfJoinSet`.`TEST_coding_Issues` (`Code`,`ProjectName`,`TeamName`,`ParentType`,`Type`,`IssueTypeDetail`,`IssueTypeId`,`Name`,`Description`,`IterationId`,`IssueStatusId`,`IssueStatusName`,`IssueStatusType`,`Priority`,`Assignee`,`StartDate`,`DueDate`,`WorkingHours`,`Creator`,`StoryPoint`,`CreatedAt`,`UpdatedAt`,`CompletedAt`,`ProjectModule`,`Watchers`,`Labels`,`Files`,`RequirementType`,`DefectType`,`CustomFields`,`ThirdLinks`,`SubTasks`,`Parent`,`Epic`,`Iteration` ) VALUES ( 187,'tapdata','tapdata','DEFECT','DEFECT', JSON '{\"Description\":\"缺陷是指软件不符合最初定义的业务需求的现象，缺陷管理用于跟踪这些问题和错误。\",\"IssueType\":\"DEFECT\",\"Name\":\"缺陷\",\"IsSystem\":true,\"Id\":104983}',104983,'Take 1 postgresql到mongo','1、创建postgresql源到mongodb目标的连接\\n2、启动任务后，已接收、已插入的统计数据显示为1286，而已处理统计数据显示为640，源库中，customer表的记录数为640，policy表的记录数为646',317840,1587681,'已关闭','COMPLETED','2', JSON '{\"Status\":1,\"Email\":\"\",\"TeamId\":0,\"GlobalKey\":\"\",\"TeamGlobalKey\":\"\",\"Avatar\":\"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-VCICtwoLaTGkAjteNzmr.jpg\",\"Name\":\"Sam\",\"Phone\":\"\",\"Id\":222040}',0,0,0.0, JSON '{\"Status\":1,\"Email\":\"\",\"TeamId\":0,\"GlobalKey\":\"\",\"TeamGlobalKey\":\"\",\"Avatar\":\"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-awaKQHhUuOlaCtwiOkVX.jpg\",\"Name\":\"Steve\",\"Phone\":\"\",\"Id\":222025}','',1576557640000,1667047480000,0, JSON '{\"Name\":\"\",\"Id\":0}', JSON '[{\"Status\":1,\"Email\":\"\",\"TeamId\":0,\"GlobalKey\":\"\",\"TeamGlobalKey\":\"\",\"Avatar\":\"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-awaKQHhUuOlaCtwiOkVX.jpg\",\"Name\":\"Steve\",\"Phone\":\"\",\"Id\":222025},{\"Status\":1,\"Email\":\"\",\"TeamId\":0,\"GlobalKey\":\"\",\"TeamGlobalKey\":\"\",\"Avatar\":\"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-VCICtwoLaTGkAjteNzmr.jpg\",\"Name\":\"Sam\",\"Phone\":\"\",\"Id\":222040}]', JSON '[]', JSON '[{\"Size\":129164,\"Url\":\"/api/project/342870/files/2647827/download\",\"Name\":\"4-20191217124036.png\",\"Type\":2,\"FileId\":2647827}]','{\"Name\":\"\",\"Id\":0}', JSON '{\"Name\":\"功能缺陷\",\"IconUrl\":\"\",\"Id\":32068367}', JSON '[{\"Id\":36843698,\"ValueString\":\"385881\"},{\"Id\":36843699,\"ValueString\":\"1.0\"},{\"Id\":36843715,\"ValueString\":\"[\\\"385944\\\",\\\"385948\\\"]\"},{\"Id\":36843722,\"ValueString\":\"385964\"}]', JSON '[]', JSON '[]', JSON '{\"Assignee\":{\\\"Status\\\":0,\\\"Email\\\":\\\"\\\",\\\"TeamId\\\":0,\\\"GlobalKey\\\":\\\"\\\",\\\"TeamGlobalKey\\\":\\\"\\\",\\\"Avatar\\\":\\\"\\\",\\\"Name\\\":\\\"\\\",\\\"Phone\\\":\\\"\\\",\\\"Id\\\":0},\"Priority\":\"\",\"IssueStatusName\":\"\",\"IssueStatusId\":0,\"Code\":0,\"Name\":\"\",\"IssueStatusType\":\"\",\"Type\":\"\",\"IssueTypeDetail\":{\\\"Description\\\":\\\"\\\",\\\"IssueType\\\":\\\"\\\",\\\"Name\\\":\\\"\\\",\\\"IsSystem\\\":false,\\\"Id\\\":0}}', JSON '{\"Assignee\":{\\\"Status\\\":0,\\\"Email\\\":\\\"\\\",\\\"TeamId\\\":0,\\\"GlobalKey\\\":\\\"\\\",\\\"TeamGlobalKey\\\":\\\"\\\",\\\"Avatar\\\":\\\"\\\",\\\"Name\\\":\\\"\\\",\\\"Phone\\\":\\\"\\\",\\\"Id\\\":0},\"Priority\":\"\",\"IssueStatusName\":\"\",\"IssueStatusId\":0,\"Code\":0,\"Name\":\"\",\"Type\":\"\"}', JSON '{\"Status\":\"WAIT_PROCESS\",\"Code\":14105,\"Name\":\"SDK Sprint #1\",\"Id\":0}' );  END IF ";
        System.out.println(s.replaceAll("\\\\\"","\""));

        //String json = "[\n    {\n        \"Size\": 138167,        \"Url\": \"/api/project/342870/files/2650851/download\",        \"Name\": \"6-20191217174143.png\",        \"Type\": 2,        \"FileId\": 2650851    }]";

        //System.out.println(simpleJSONStr(json));
        String ss = "DECLARE exits INT64;SET exits = (select 1 from `vibrant-castle-366614`.`SchemaoOfJoinSet`.`TEST_coding_Issues` WHERE `Code`=187 AND `ProjectName`='tapdata' AND `TeamName`='tapdata' );  if exits = 1 then   update `vibrant-castle-366614`.`SchemaoOfJoinSet`.`TEST_coding_Issues` SET `Code`=187,`ProjectName`='tapdata',`TeamName`='tapdata',`ParentType`='DEFECT',`Type`='DEFECT',`IssueTypeDetail`= JSON \"{\\\"Description\\\":\\\"缺陷是指软件不符合最初定义的业务需求的现象，缺陷管理用于跟踪这些问题和错误。\\\",\\\"IssueType\\\":\\\"DEFECT\\\",\\\"Name\\\":\\\"缺陷\\\",\\\"IsSystem\\\":true,\\\"Id\\\":104983}',`IssueTypeId`='104983',`Name`='Take 1 postgresql到mongo',`Description`='1、创建postgresql源到mongodb目标的连接\\n2、启动任务后，已接收、已插入的统计数据显示为1286，而已处理统计数据显示为640，源库中，customer表的记录数为640，policy表的记录数为646\",`IterationId`='317840',`IssueStatusId`=1587681,`IssueStatusName`='已关闭',`IssueStatusType`='COMPLETED',`Priority`='2',`Assignee`= JSON \"{\\\"Status\\\":1,\\\"Email\\\":\\\"\\\",\\\"TeamId\\\":0,\\\"GlobalKey\\\":\\\"\\\",\\\"TeamGlobalKey\\\":\\\"\\\",\\\"Avatar\\\":\\\"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-VCICtwoLaTGkAjteNzmr.jpg\\\",\\\"Name\\\":\\\"Sam\\\",\\\"Phone\\\":\\\"\\\",\\\"Id\\\":222040}\",`StartDate`=0,`DueDate`=0,`WorkingHours`=0.0,`Creator`= JSON \"{\\\"Status\\\":1,\\\"Email\\\":\\\"\\\",\\\"TeamId\\\":0,\\\"GlobalKey\\\":\\\"\\\",\\\"TeamGlobalKey\\\":\\\"\\\",\\\"Avatar\\\":\\\"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-awaKQHhUuOlaCtwiOkVX.jpg\\\",\\\"Name\\\":\\\"Steve\\\",\\\"Phone\\\":\\\"\\\",\\\"Id\\\":222025}\",`StoryPoint`='',`CreatedAt`=1576557640000,`UpdatedAt`=1667047480000,`CompletedAt`=0,`ProjectModule`= JSON '{\"Name\":\"\",\"Id\":0}',`Watchers`= JSON \"[{\\\"Status\\\":1,\\\"Email\\\":\\\"\\\",\\\"TeamId\\\":0,\\\"GlobalKey\\\":\\\"\\\",\\\"TeamGlobalKey\\\":\\\"\\\",\\\"Avatar\\\":\\\"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-awaKQHhUuOlaCtwiOkVX.jpg\\\",\\\"Name\\\":\\\"Steve\\\",\\\"Phone\\\":\\\"\\\",\\\"Id\\\":222025},{\\\"Status\\\":1,\\\"Email\\\":\\\"\\\",\\\"TeamId\\\":0,\\\"GlobalKey\\\":\\\"\\\",\\\"TeamGlobalKey\\\":\\\"\\\",\\\"Avatar\\\":\\\"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-VCICtwoLaTGkAjteNzmr.jpg\\\",\\\"Name\\\":\\\"Sam\\\",\\\"Phone\\\":\\\"\\\",\\\"Id\\\":222040}]\",`Labels`= JSON '[]',`Files`= NULL,`DefectType`= JSON '{\"Name\":\"功能缺陷\",\"IconUrl\":\"\",\"Id\":32068367}',`CustomFields`= JSON '[{\"Id\":36843698,\"ValueString\":\"385881\"},{\"Id\":36843699,\"ValueString\":\"1.0\"},{\"Id\":36843715,\"ValueString\":\"[\"385944\",\"385948\"]\"},{\"Id\":36843722,\"ValueString\":\"385964\"}]',`ThirdLinks`= JSON '[]',`SubTasks`= JSON '[]',`Parent`= JSON '{\"Assignee\":{\"Status\":0,\"Email\":\"\",\"TeamId\":0,\"GlobalKey\":\"\",\"TeamGlobalKey\":\"\",\"Avatar\":\"\",\"Name\":\"\",\"Phone\":\"\",\"Id\":0},\"Priority\":\"\",\"IssueStatusName\":\"\",\"IssueStatusId\":0,\"Code\":0,\"Name\":\"\",\"IssueStatusType\":\"\",\"Type\":\"\",\"IssueTypeDetail\":{\"Description\":\"\",\"IssueType\":\"\",\"Name\":\"\",\"IsSystem\":false,\"Id\":0}}',`Epic`= JSON '{\"Assignee\":{\"Status\":0,\"Email\":\"\",\"TeamId\":0,\"GlobalKey\":\"\",\"TeamGlobalKey\":\"\",\"Avatar\":\"\",\"Name\":\"\",\"Phone\":\"\",\"Id\":0},\"Priority\":\"\",\"IssueStatusName\":\"\",\"IssueStatusId\":0,\"Code\":0,\"Name\":\"\",\"Type\":\"\"}',`Iteration`= JSON '{\"Status\":\"WAIT_PROCESS\",\"Code\":14105,\"Name\":\"SDK Sprint #1\",\"Id\":0}'  WHERE `Code`='187' AND `ProjectName`='tapdata' AND `TeamName`='tapdata' ; ELSE   insert into `vibrant-castle-366614`.`SchemaoOfJoinSet`.`TEST_coding_Issues` (`Code`,`ProjectName`,`TeamName`,`ParentType`,`Type`,`IssueTypeDetail`,`IssueTypeId`,`Name`,`Description`,`IterationId`,`IssueStatusId`,`IssueStatusName`,`IssueStatusType`,`Priority`,`Assignee`,`StartDate`,`DueDate`,`WorkingHours`,`Creator`,`StoryPoint`,`CreatedAt`,`UpdatedAt`,`CompletedAt`,`ProjectModule`,`Watchers`,`Labels`,`Files`,`RequirementType`,`DefectType`,`CustomFields`,`ThirdLinks`,`SubTasks`,`Parent`,`Epic`,`Iteration` ) VALUES ( 187,'tapdata','tapdata','DEFECT','DEFECT', JSON '{\"Description\":\"缺陷是指软件不符合最初定义的业务需求的现象，缺陷管理用于跟踪这些问题和错误。\",\"IssueType\":\"DEFECT\",\"Name\":\"缺陷\",\"IsSystem\":true,\"Id\":104983}',104983,'Take 1 postgresql到mongo','1、创建postgresql源到mongodb目标的连接\\n2、启动任务后，已接收、已插入的统计数据显示为1286，而已处理统计数据显示为640，源库中，customer表的记录数为640，policy表的记录数为646',317840,1587681,'已关闭','COMPLETED','2', JSON '{\"Status\":1,\"Email\":\"\",\"TeamId\":0,\"GlobalKey\":\"\",\"TeamGlobalKey\":\"\",\"Avatar\":\"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-VCICtwoLaTGkAjteNzmr.jpg\",\"Name\":\"Sam\",\"Phone\":\"\",\"Id\":222040}',0,0,0.0, JSON '{\"Status\":1,\"Email\":\"\",\"TeamId\":0,\"GlobalKey\":\"\",\"TeamGlobalKey\":\"\",\"Avatar\":\"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-awaKQHhUuOlaCtwiOkVX.jpg\",\"Name\":\"Steve\",\"Phone\":\"\",\"Id\":222025}','',1576557640000,1667047480000,0, JSON '{\"Name\":\"\",\"Id\":0}', JSON '[{\"Status\":1,\"Email\":\"\",\"TeamId\":0,\"GlobalKey\":\"\",\"TeamGlobalKey\":\"\",\"Avatar\":\"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-awaKQHhUuOlaCtwiOkVX.jpg\",\"Name\":\"Steve\",\"Phone\":\"\",\"Id\":222025},{\"Status\":1,\"Email\":\"\",\"TeamId\":0,\"GlobalKey\":\"\",\"TeamGlobalKey\":\"\",\"Avatar\":\"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-VCICtwoLaTGkAjteNzmr.jpg\",\"Name\":\"Sam\",\"Phone\":\"\",\"Id\":222040}]', JSON '[]', JSON '[{\"Size\":129164,\"Url\":\"/api/project/342870/files/2647827/download\",\"Name\":\"4-20191217124036.png\",\"Type\":2,\"FileId\":2647827}]','{\"Name\":\"\",\"Id\":0}', JSON '{\"Name\":\"功能缺陷\",\"IconUrl\":\"\",\"Id\":32068367}', JSON '[{\"Id\":36843698,\"ValueString\":\"385881\"},{\"Id\":36843699,\"ValueString\":\"1.0\"},{\"Id\":36843715,\"ValueString\":\"[\"385944\",\"385948\"]\"},{\"Id\":36843722,\"ValueString\":\"385964\"}]', JSON '[]', JSON '[]', JSON '{\"Assignee\":{\"Status\":0,\"Email\":\"\",\"TeamId\":0,\"GlobalKey\":\"\",\"TeamGlobalKey\":\"\",\"Avatar\":\"\",\"Name\":\"\",\"Phone\":\"\",\"Id\":0},\"Priority\":\"\",\"IssueStatusName\":\"\",\"IssueStatusId\":0,\"Code\":0,\"Name\":\"\",\"IssueStatusType\":\"\",\"Type\":\"\",\"IssueTypeDetail\":{\"Description\":\"\",\"IssueType\":\"\",\"Name\":\"\",\"IsSystem\":false,\"Id\":0}}', JSON '{\"Assignee\":{\"Status\":0,\"Email\":\"\",\"TeamId\":0,\"GlobalKey\":\"\",\"TeamGlobalKey\":\"\",\"Avatar\":\"\",\"Name\":\"\",\"Phone\":\"\",\"Id\":0},\"Priority\":\"\",\"IssueStatusName\":\"\",\"IssueStatusId\":0,\"Code\":0,\"Name\":\"\",\"Type\":\"\"}', JSON '{\"Status\":\"WAIT_PROCESS\",\"Code\":14105,\"Name\":\"SDK Sprint #1\",\"Id\":0}' );  END IF ";
        System.out.println(ss.substring(0,5085));


        System.out.println("\"[]\"".replaceAll("\"\\[","\\[").replaceAll("]\"","]"));

        System.out.println("'{}'".replaceAll("'","\\\\'"));
        }

    public static String simpleJSONStr(String str){
        if (null == str) return str;
        StringBuilder builder = new StringBuilder();
        boolean skip = true;
        boolean escape = false;
        for (int index = 0; index < str.length(); index++) {
            char ch  =str.charAt(index);
            escape = !escape && ch=='\\';
            if (skip && (ch==' '|| ch == '\r' || ch == '\n' || ch == '\t')){
                continue;
            }
            builder.append(ch);
            if (ch == '"' && !escape) skip = !skip;
        }
        return builder.toString();
        //return replace(builder.toString(),"\r\n","\\\\r\\\\n");

    }

    public static String replace(String str,String format,String target){
        if(str!=null && !"".equals(str)) {
            Pattern p = Pattern.compile(format);
            Matcher m = p.matcher(str);
            String strNoBlank = m.replaceAll(target);
            return strNoBlank;
        }else {
            return str;
        }
    }
}
