package io.tapdata.zoho.service.zoho.loader;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.HttpResult;
import io.tapdata.zoho.entity.HttpType;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.ZoHoHttp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Open-Api Document: https://desk.zoho.com.cn/support/APIDocument.do#Departments
 * */
public class DepartmentOpenApi extends ZoHoStarter implements ZoHoBase {
    public static final String GET_URI = "/api/v1/departments/{department_id}";
    public static final String LIST_URI = "/api/v1/departments";
    public static final String LIST_AGENTS_IN_DEPARTMENT = "/api/v1/departments/{department_id}/agents";
    public static final String GET_DEPARTMENT_COUNT = "/api/v1/departments/count";
    public static final String GET_DEPARTMENT_DETAILS_BY_DEPARTMENT_IDS_URI = "/api/v1/departmentsByIds?departmentIds={department_ids}";

    private static final String TAG = DepartmentOpenApi.class.getSimpleName();
    protected DepartmentOpenApi(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }
    public static DepartmentOpenApi create(TapConnectionContext tapConnectionContext){
        return new DepartmentOpenApi(tapConnectionContext);
    }

    public TapConnectionContext getContext(){
        return this.tapConnectionContext;
    }
    /**
     * 根据部门ID获取一个部门详情
     * */
    public Map<String,Object> getOne(String departmentId){
        if (Checker.isEmpty(departmentId)){
            TapLogger.debug(TAG,"Department Id can not be null or not be empty.");
        }
        String accessToken = this.accessTokenFromConfig();
        HttpEntity<String,String> header = HttpEntity.create()
                .build("Authorization",accessToken);
        HttpEntity<String,String> resetFull = HttpEntity.create().build("department_id",departmentId);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,GET_URI), HttpType.GET,header).resetFull(resetFull);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Get Department list succeed.");
        Map<String,Object> data = (Map<String,Object>)httpResult.getResult();
        return Checker.isEmpty(data)? new HashMap<>():data;
    }

    /**
     * 部门分页查询
     * @param from       integer optional,range : >=0        Index number, starting from which the departments must be fetched
     * @param searchStr  string optional max chars : 100     String to search for departments by name, help center name, or description.
     *                                                          The string must contain at least one character.
     *                                                          Three search methods are supported: 1) string*
     *                                                          - Searches for departments whose name,
     *                                                          help center name,
     *                                                          or description start with the string, 2)
     *                                                          *string* - Searches for departments whose name,
     *                                                          help center name, or description contain the string, 3)
     *                                                          string
     *                                                          - Searches for departments whose name, help center name,
     *                                                          or description is an exact match for the string
     * @param limit  integer optional  range  0-200          Number of departments to fetch; default value is 10 and maximum value supported is 200
     * @param chatStatus   string optional max chars : 100   Key that filters departments based on their chat status.
         *                                                          Values allowed are AVAILABLE, DISABLED, NOT_CREATED,
     *                                                          and ${UNAVAILABLE}. ${UNAVAILABLE}
     *                                                          refers to departments which are not available for chat.
     * */
    public static final int MAX_PAGE_LIMIT = 200;
    public static final int MIN_PAGE_LIMIT = 0;
    public static final int DEFAULT_PAGE_LIMIT = 10;
    public static final int MIN_FROM = 0;
    public List<Map<String,Object>> list(String searchStr,String chatStatus,Integer from,Integer limit,Boolean isEnabled){
        if (Checker.isEmpty(from) || from < MIN_FROM) from = MIN_FROM;
        if (Checker.isEmpty(limit) || limit < MIN_PAGE_LIMIT || limit > MAX_PAGE_LIMIT) limit = DEFAULT_PAGE_LIMIT;
        HttpEntity<String,Object> form = HttpEntity.create();
        form.build("from",from).build("limit",limit);
        if (Checker.isNotEmpty(searchStr)){
            form.build("searchStr",searchStr);
        }
        if (Checker.isNotEmpty(isEnabled)){
            form.build("isEnabled",isEnabled);
        }
        if (Checker.isNotEmpty(chatStatus)){
            form.build("chatStatus",chatStatus);
        }
        return this.list(form);
    }
    private List<Map<String,Object>> list(HttpEntity<String,Object> form){
        HttpEntity<String, String> header = requestHeard();
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,LIST_URI), HttpType.GET,header).header(header).form(form);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Get department list succeed.");
        Object data = ((Map<String,Object>)httpResult.getResult()).get("data");
        return Checker.isEmpty(data)?new ArrayList<>():(List<Map<String,Object>>)data;
    }

    public List<Map<String,Object>> listAgentsInDepartment(){
        return null;
    }

    public int getDepartmentCount(){
        HttpEntity<String, String> header = requestHeard();
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,GET_DEPARTMENT_COUNT), HttpType.GET,header);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Get Department list succeed.");
        Map<String,Object> data = (Map<String,Object>)httpResult.getResult();
        Object count = null;
        return !data.isEmpty() && Checker.isNotEmpty(count = data.get("count")) ? (int)count:0;
    }

    public Map<String,Object> getDepartmentDetailsByDepartmentIDs(){
        return null;
    }
}
