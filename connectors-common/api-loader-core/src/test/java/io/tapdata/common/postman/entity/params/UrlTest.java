package io.tapdata.common.postman.entity.params;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UrlTest {
    @Test
    public void url(){
        String raw = "{{base_url}}/sheets/v3/spreadsheets/:spreadsheet_token/sheets/:sheet_id/float_images";
        List<String> host = new ArrayList<>();
        List<String> path = new ArrayList<>();
        List<Map<String,Object>> variable = new ArrayList<>();
        Map<String,Object> q = new HashMap<>();
        q.put("key","spreadsheet_token");
        q.put("value","shtcnmBA*****yGehy8");
        q.put("description","表格 token");
        variable.add(q);
        q = new HashMap<>();
        q.put("key","sheet_id");
        q.put("value","0b**12");
        q.put("description","子表 id");
        variable.add(q);
        List<Map<String,Object>> query = new ArrayList<>();
        Map<String,Object> v = new HashMap<>();
        v.put("key","tokenId");
        v.put("value","0b**12");
        v.put("description","子表 id");
        query.add(v);

        Url url = Url.create().query(query).variable(variable).raw(raw).host(host).path(path);

        Map<String,Object> map = new HashMap<>();
        map.put("base_url","http://127.0.0.1");
        map.put("sheet_id","520");
        map.put("tokenId","666");
        //System.out.println(url.variableAssignment(map).raw());
        Assert.assertEquals(
               "Url variable assignment to url String error."
                , url.variableAssignment(map).raw()
                , "http://127.0.0.1/sheets/v3/spreadsheets/shtcnmBA*****yGehy8/sheets/520/float_images"
                );

    }
}
