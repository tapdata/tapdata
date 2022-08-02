/*
Copyright [2020] [https://www.stylefeng.cn]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Guns采用APACHE LICENSE 2.0开源协议，您在使用过程中，需要注意以下几点：

1.请不要删除和修改根目录下的LICENSE文件。
2.请不要删除和修改Guns源码头部的版权声明。
3.请保留源码和相关描述文件的项目出处，作者声明等。
4.分发源码时候，请注明软件出处 https://gitee.com/stylefeng/guns-separation
5.在修改包名，模块名称，项目代码等时，请注明软件出处 https://gitee.com/stylefeng/guns-separation
6.若您的项目无法满足以上几点，可申请商业授权，获取Guns商业授权许可，请在官网购买授权，地址为 https://www.stylefeng.cn
 */
package com.tapdata.tm;

import com.alibaba.fastjson.JSON;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import javax.annotation.Resource;
import java.util.Collections;

/**
 * 基础测试类
 *
 */
@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TMApplication.class})
public class BaseJunit {

    @Resource
    private WebApplicationContext webApplicationContext;

    public MockMvc mockMvc;

    @Before
    public void setupMockMvc() {
        mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
    }

    @Before
    public void initDatabase() {
    }

    protected UserDetail getUser() {
        UserDetail userDetail = new UserDetail("62172cfc49b865ee5379d3ed", "", "测试用户", "", Collections.singleton(new SimpleGrantedAuthority("USERS")));
        return userDetail;
    }


    protected UserDetail getUser(String userId) {
        UserDetail userDetail = new UserDetail(userId, "", "测试用户", "", Collections.singleton(new SimpleGrantedAuthority("USERS")));
        return userDetail;
    }

    protected void printResult(Object o) {
        String json = JsonUtil.toJson(o);
        log.info("结果是: " );
        log.info("{}  \t\n ", json);
    }
    public String replaceLoopBack(String json) {
        if (StringUtils.isNotBlank(json)) {
            json = json.replace("\"like\"", "\"$regex\"");
            json = json.replace("\"options\"", "\"$options\"");
            json = json.replace("\"$inq\"", "\"$in\"");
            json = json.replace("\"in\"", "\"$in\"");
        }
        return json;
    }
    public Filter parseFilter(String filterJson) {
        filterJson=replaceLoopBack(filterJson);
        Filter filter = JsonUtil.parseJson(filterJson, Filter.class);
        if (filter == null) {
            return new Filter();
        }
        Where where = filter.getWhere();
        if (where != null) {
            where.remove("user_id");
        }
        return filter;
    }

    public Where parseWhere(String whereJson) {
        replaceLoopBack(whereJson);
        return JsonUtil.parseJson(whereJson, Where.class);
    }

}
