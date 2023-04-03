package io.tapdata.coding.service.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Array;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Integer;

public class ProjectMembers implements SchemaStart {
    public final Boolean use = true;

    @Override
    public Boolean use() {
        return use;
    }

    public ProjectMembers(AtomicReference<String> accessToken) {
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
                .add(field("RolesRoles", JAVA_Array));

    }

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
