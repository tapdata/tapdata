package io.tapdata.coding.service.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;
public class ProjectMembers implements SchemaStart {
    public final Boolean use = true;

    @Override
    public Boolean use() {
        return use;
    }

    @Override
    public String tableName() {
        return "ProjectMembers";
    }
    @Override
    public boolean connection(TapConnectionContext tapConnectionContext) {
        return false;
    }
    @Override
    public TapTable document(TapConnectionContext connectionContext) {
        /**
         "ProjectMembers": [
         {
         "Id": 6,
         "TeamId": 1,
         "Name": "blockuser",
         "NamePinYin": "blockuser",
         "Avatar": "http://e.coding.net/static/fruit_avatar/Fruit-4.png",
         "Email": "blockuser@gmail.com",
         "Phone": "13800138006",
         "EmailValidation": 1,
         "PhoneValidation": 1,
         "Status": -1,
         "GlobalKey": "GK",
         "Roles": [
         {
         "RoleType": "ProjectMember",
         "RoleId": 1,
         "RoleTypeName": "开发"
         }
         ]
         }
         ]
         }
         * */
        return table(tableName())
                .add(field("Id", JAVA_Integer).isPrimaryKey(true).primaryKeyPos(1))
                .add(field("TeamId", JAVA_Integer))
                .add(field("ProjectId", JAVA_Integer))
                .add(field("Name", "StringMinor"))
                .add(field("NamePinYin", "StringMinor"))
                .add(field("Avatar", "StringMinor"))
                .add(field("Email", "StringMinor"))
                .add(field("Phone", "StringMinor"))
                .add(field("EmailValidation", JAVA_Integer))
                .add(field("PhoneValidation", JAVA_Integer))
                .add(field("Status", JAVA_Integer))
                .add(field("GlobalKey", "StringMinor"))
                .add(field("RolesRoles", JAVA_Array)) ;

    }

    /**
     * "member":{
     *      *         "id": 8647278,
     *      *         "login": "vMBtGCrzEP",
     *      *         "avatar_url": "https://coding-net-production-static-ci.codehub.cn/a0835321-9657-48ce-950a-d196b75e4ed3.png?imageView2/1/w/0/h/0",
     *      *         "url": "https://testhookgavin.coding.net/api/user/key/vMBtGCrzEP",
     *      *         "html_url": "https://testhookgavin.coding.net/u/vMBtGCrzEP",
     *      *         "name": "邱迎豪",
     *      *         "name_pinyin": "qyh|qiuyinghao"
     * }
     *
     * */
    @Override
    public Map<String, Object> autoSchema(Map<String, Object> eventData) {
        return eventMapToSchemaMap(eventData, TapSimplify.map(
                entry("Id", "id"),
                entry("Name", "name"),
                entry("NamePinYin", "name_pinyin"),
                entry("Avatar", "avatar_url"),
                entry("Email", "html_url"),
                entry("Phone", "name"),
                entry("EmailValidation", "email_validation"),
                entry("PhoneValidation", "phone_validation"),
                entry("Status", "status"),
                entry("TeamId", "team_id"),
                entry("ProjectId", "project_id"),
                entry("GlobalKey", "login"),
                entry("RolesRoles", "roles_roles")
        ));
    }
}
