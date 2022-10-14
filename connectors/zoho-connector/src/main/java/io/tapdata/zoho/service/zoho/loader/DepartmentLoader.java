package io.tapdata.zoho.service.zoho.loader;

import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.List;
import java.util.Map;

/**
 * Open-Api Document: https://desk.zoho.com.cn/support/APIDocument.do#Departments
 * */
public class DepartmentLoader extends ZoHoStarter implements ZoHoBase {
    public static final String GET_URI = "/api/v1/departments/{department_id}";
    public static final String LIST_URI = "/api/v1/departments";
    public static final String LIST_AGENTS_IN_DEPARTMENT = "/api/v1/departments/{department_id}/agents";
    public static final String GET_DEPARTMENT_COUNT = "/api/v1/departments/{department_id}/agents";
    public static final String GET_DEPARTMENT_DETAILS_BY_DEPARTMENT_IDS_URI = "/api/v1/departmentsByIds?departmentIds={department_ids}";

    private static final String TAG = DepartmentLoader.class.getSimpleName();
    protected DepartmentLoader(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }
    public static DepartmentLoader create(TapConnectionContext tapConnectionContext){
        return new DepartmentLoader(tapConnectionContext);
    }

    public Map<String,Object> getOne(){

        return null;
    }

    public List<Map<String,Object>> list(){
        return null;
    }

    public List<Map<String,Object>> listAgentsInDepartment(){
        return null;
    }

    public int getDepartmentCount(){
        return 0;
    }

    public Map<String,Object> getDepartmentDetailsByDepartmentIDs(){
        return null;
    }
}
