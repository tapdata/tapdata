package io.tapdata.coding.service.openApi;

import io.tapdata.coding.service.openApi.lamada.HttpBodyChecker;
import io.tapdata.coding.service.openApi.lamada.HttpResultGet;

import java.util.Collections;
import java.util.Map;

public class IssuesOpenApi extends OpenApi {

    public IssuesOpenApi(Map<String,String> httpHeard,String openUrl){
        super(httpHeard,openUrl);
    }
    public static IssuesOpenApi create(Map<String,String> httpHeard,String openUrl){
        return new IssuesOpenApi(httpHeard,openUrl);
    }

    /**创建事项
     *
     * 参数名称	             必选	类型	描述
     * Action	             是	String	公共参数，本接口取值：CreateIssue
     * ProjectName	         是	String	项目名称
     * Type	                 是	String	事项类型
     *                                      DEFECT - 缺陷
     *                                      REQUIREMENT - 需求
     *                                      MISSION - 任务
     *                                      EPIC - 史诗
     *                                      SUB_TASK - 子工作项
     *                                      RISK - 风险
     *                                      WORK_ITEM - 工作项（项目集使用）
     * Name	是	String	事项名称
     * Priority	是	String	紧急程度
     *                          "0" - 低
     *                          "1" - 中
     *                          "2" - 高
     *                          "3" - 紧急
     * IssueTypeId	否	Integer	具体事项类型 ID，参考 DescribeTeamIssueTypeList
     * ParentCode	否	Integer	所属事项 Code，Type 为 SUB_TASK 时需指定
     * StatusId	否	Integer	事项状态 Id
     * AssigneeId	否	Integer	指派人 Id
     * Description	否	String	描述
     * DueDate	否	String	截止日期
     * StartDate	否	String	开始日期
     * WorkingHours	否	Double	工时（小时）
     * ProjectModuleId	否	Integer	项目模块 Id
     * WatcherIds	否	Array of Integer	事项关注人 Id 列表
     * DefectTypeId	否	Integer	项目缺陷类型 Id
     * RequirementTypeId	否	Integer	项目需求类型 Id
     * IterationCode	否	Integer	迭代 Code，Type 为 EPIC 或 SUB_TASK 时，忽略该值
     * EpicCode	否	Integer	史诗 Code，Type 为 EPIC 或 SUB_TASK 时，不传该值
     * StoryPoint	否	String	故事点，例如：0.5、1
     * LabelIds	否	Array of Integer	标签 Id 列表
     * FileIds	否	Array of Integer	文件 Id 列表
     * TargetSortCode	否	Integer	排序目标位置的事项 code 可不填，排在底位
     * ThirdLinks	否	Array of CreateThirdLinkForm	第三方链接列表
     * CustomFieldValues	否	Array of IssueCustomFieldForm	自定义属性值列表，具体见创建示例
     * */
    public Map<String,Object> createIssue(Map<String,Object> httpBody){
        return (Map<String, Object>) this.http(()-> HttpBodyChecker.verify(httpBody,Action.CreateIssue,"ProjectName","Type","Name","Priority"),httpBody,null);
    }
    public Map<String,Object> createIssueAsSimpleResultBack(Map<String,Object> httpBody){
        return (Map<String, Object>) this.http(()->
                HttpBodyChecker.verify(httpBody,Action.CreateIssue,"ProjectName","Type","Name","Priority")
                ,httpBody
                ,(response)->{
                    Object byKey = HttpResultGet.getByKey("Response.Issue", response);
                    if (byKey instanceof Map) return (Map<String,Object>)byKey;
                    return Collections.emptyMap();
                });
    }

    /**删除事项*/
    public Map<String,Object> deleteIssue(){
        return null;
    }

    /**查询事项附件的下载地址*/
    public Map<String,Object> describeIssueFileUrl(){
        return null;
    }

    /**查询筛选器列表*/
    public Map<String,Object> describeIssueFilterList(){
        return null;
    }

    /**查询事项列表（旧）*/
    public Map<String,Object> describeIssueList(){
        return null;
    }

    /**事项列表（新）*/
    public Map<String,Object> describeIssueListWithPage(){
        return null;
    }

    /**查询事项详情*/
    public Map<String,Object> describeIssue(){
        return null;
    }

    /**查询属性设置*/
    public Map<String,Object> describeProjectIssueFieldList(){
        return null;
    }

    /**查询状态设置*/
    public Map<String,Object> describeProjectIssueStatusList(){
        return null;
    }

    /**修改事项*/
    public Map<String,Object> modifyIssue(){
        return null;
    }

    /**修改事项描述*/
    public Map<String,Object> modifyIssueDescription(){
        return null;
    }

    /**查询后置事项*/
    public Map<String,Object> describeBlockIssueList(){
        return null;
    }

    /**查询前置事项*/
    public Map<String,Object> describeBlockedByIssueList(){
        return null;
    }

    /**查询子事项列表*/
    public Map<String,Object> describeSubIssueList(){
        return null;
    }

    /**删除前置事项*/
    public Map<String,Object> deleteIssueBlock(){
        return null;
    }

    /**添加前置事项*/
    public Map<String,Object> createIssueBlock(){
        return null;
    }

    /**修改事项父需求*/
    public Map<String,Object> modifyIssueParentRequirement(){
        return null;
    }

    /**查询企业所有事项类型列表*/
    public Map<String,Object> describeTeamIssueTypeList(){
        return null;
    }

    /**查询项目的事项类型列表*/
    public Map<String,Object> describeProjectIssueTypeList(){
        return null;
    }

    /**事项关联的测试用例*/
    public Map<String,Object> describeRelatedCaseList(){
        return null;
    }

    /**创建事项评论*/
    public Map<String,Object> createIssueComment(){
        return null;
    }

    /**修改事项评论*/
    public Map<String,Object> modifyIssueComment(){
        return null;
    }

    /**查询事项评论列表*/
    public Map<String,Object> describeIssueCommentList(){
        return null;
    }

    /**获取事项的状态变更历史*/
    public Map<String,Object> describeIssueStatusChangeLogList(){
        return null;
    }

}
