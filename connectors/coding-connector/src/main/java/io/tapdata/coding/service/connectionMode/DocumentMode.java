package io.tapdata.coding.service.connectionMode;

import cn.hutool.http.HttpRequest;
import io.tapdata.coding.entity.ContextConfig;
import io.tapdata.coding.service.loader.CodingStarter;
import io.tapdata.coding.service.loader.IssuesLoader;
import io.tapdata.coding.service.schema.SchemaStart;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.http.HttpEntity;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * {
 * "label": "${document}",
 * "value": "DocumentMode"
 * }
 */
public class DocumentMode implements ConnectionMode {
    TapConnectionContext connectionContext;
    IssuesLoader loader;
    ContextConfig contextConfig;

    @Override
    public ConnectionMode config(TapConnectionContext connectionContext, AtomicReference<String> accessToken) {
        this.connectionContext = connectionContext;
        this.loader = IssuesLoader.create(connectionContext, accessToken);
        this.contextConfig = loader.veryContextConfigAndNodeConfig();
        return this;
    }

    @Override
    public List<TapTable> discoverSchema(List<String> tables, int tableSize, AtomicReference<String> accessToken) {
        /**
         if(tables == null || tables.isEmpty()) {
         return list(
         table("Issues")
         .add(field("Code", JAVA_Integer).isPrimaryKey(true).primaryKeyPos(3))        //事项 Code
         .add(field("ProjectName", "StringMinor").isPrimaryKey(true).primaryKeyPos(2))   //项目名称
         .add(field("TeamName", "StringMinor").isPrimaryKey(true).primaryKeyPos(1))      //团队名称
         .add(field("ParentType", "StringMinor"))                                       //父事项类型
         .add(field("Type", "StringMinor"))                                         //事项类型：DEFECT - 缺陷;REQUIREMENT - 需求;MISSION - 任务;EPIC - 史诗;SUB_TASK - 子工作项
         .add(field("IssueTypeDetailId", JAVA_Integer))                               //事项类型ID
         .add(field("IssueTypeDetail", JAVA_Map))                                         //事项类型具体信息
         .add(field("Name", "StringMinor"))                                              //名称
         .add(field("Description", "StringLonger"))                                       //描述
         .add(field("IterationId", JAVA_Integer))                                     //迭代 Id
         .add(field("IssueStatusId", JAVA_Integer))                                   //事项状态 Id
         .add(field("IssueStatusName", "StringMinor"))                                   //事项状态名称
         .add(field("IssueStatusType", "StringMinor"))                                   //事项状态类型
         .add(field("Priority", "StringBit"))                                          //优先级:"0" - 低;"1" - 中;"2" - 高;"3" - 紧急;"" - 未指定
         .add(field("AssigneeId", JAVA_Integer))                                      //Assignee.Id 等于 0 时表示未指定
         .add(field("Assignee", JAVA_Map))                                                //处理人
         .add(field("StartDate", JAVA_Long))                                             //开始日期时间戳
         .add(field("DueDate", JAVA_Long))                                               //截止日期时间戳
         .add(field("WorkingHours", "WorkingHours"))                                      //工时（小时）
         .add(field("CreatorId", JAVA_Integer))                                       //创建人Id
         .add(field("Creator", JAVA_Map))                                                 //创建人
         .add(field("StoryPoint", "StringMinor"))                                        //故事点
         .add(field("CreatedAt", JAVA_Long))                                             //创建时间
         .add(field("UpdatedAt", JAVA_Long))                                             //修改时间
         .add(field("CompletedAt", JAVA_Long))                                           //完成时间
         .add(field("ProjectModuleId", JAVA_Integer))                                 //ProjectModule.Id 等于 0 时表示未指定
         .add(field("ProjectModule", JAVA_Map))                                           //项目模块
         .add(field("WatcherIdArr", JAVA_Array))                                        //关注人Id列表
         .add(field("Watchers", JAVA_Array))                                            //关注人
         .add(field("LabelIdArr", JAVA_Array))                                          //标签Id列表
         .add(field("Labels", JAVA_Array))                                              //标签列表
         .add(field("FileIdArr", JAVA_Array))                                           //附件Id列表
         .add(field("Files", JAVA_Array))                                               //附件列表
         .add(field("RequirementType", "StringSmaller"))                                   //需求类型
         .add(field("DefectType", JAVA_Map))                                              //缺陷类型
         .add(field("CustomFields", JAVA_Array))                                        //自定义字段列表
         .add(field("ThirdLinks", JAVA_Array))                                          //第三方链接列表
         .add(field("SubTaskCodeArr", JAVA_Array))                                      //子工作项Code列表
         .add(field("SubTasks", JAVA_Array))                                            //子工作项列表
         .add(field("ParentCode", JAVA_Integer))                                      //父事项Code
         .add(field("Parent", JAVA_Map))                                                  //父事项
         .add(field("EpicCode", JAVA_Integer))                                        //所属史诗Code
         .add(field("Epic", JAVA_Map))                                                    //所属史诗
         .add(field("IterationCode", JAVA_Integer))                                   //所属迭代Code
         .add(field("Iteration", JAVA_Map))                                               //所属迭代
         );
         }
         */
        List<SchemaStart> schemaStart = SchemaStart.getAllSchemas(connectionContext);
        if (tables == null || tables.isEmpty()) {
            List<TapTable> tapTables = list();
            schemaStart.forEach(schema -> {
                TapTable documentTable = schema.document(connectionContext);
                if (Checker.isNotEmpty(documentTable)) {
                    tapTables.add(documentTable);
                }
            });
            return tapTables;
        }
        return null;
    }

    @Override
    public Map<String, Object> attributeAssignment(Map<String, Object> stringObjectMap) {
        Object code = stringObjectMap.get("Code");
        HttpEntity<String, String> header = HttpEntity.create().builder("Authorization", contextConfig.getToken());
        String projectName = contextConfig.getProjectName();
        HttpEntity<String, Object> issueDetialBody = HttpEntity.create()
                .builder("Action", "DescribeIssue")
                .builder("ProjectName", projectName);
        String teamName = contextConfig.getTeamName();
        CodingHttp authorization = CodingHttp.create(header.getEntity(), String.format(CodingStarter.OPEN_API_URL, teamName));
        HttpRequest requestDetail = authorization.createHttpRequest();
        Map<String, Object> issueDetail = loader.readIssueDetail(
                issueDetialBody,
                authorization,
                requestDetail,
                (code instanceof Integer) ? (Integer) code : Integer.parseInt(code.toString()),
                projectName,
                teamName);
        loader.composeIssue(projectName, teamName, issueDetail);
        return issueDetail;
    }
}
