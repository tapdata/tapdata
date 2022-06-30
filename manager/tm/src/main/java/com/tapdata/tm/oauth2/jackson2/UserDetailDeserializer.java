package com.tapdata.tm.oauth2.jackson2;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;

import java.io.IOException;
import java.util.Set;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/24 下午8:00
 */
public class UserDetailDeserializer extends JsonDeserializer<UserDetail> {
    @Override
    public UserDetail deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {

        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode root = mapper.readTree(p);
        return deserialize(p, mapper, root);
    }

    private UserDetail deserialize(JsonParser parser, ObjectMapper mapper, JsonNode root) {

        String userId = JsonNodeUtils.findStringValue(root, "userId");
        String customerId = JsonNodeUtils.findStringValue(root, "customerId");
        String username = JsonNodeUtils.findStringValue(root, "username");
        String password = JsonNodeUtils.findStringValue(root, "password");
        String customerType = JsonNodeUtils.findStringValue(root, "customerType");
        String accessCode = JsonNodeUtils.findStringValue(root, "accessCode");
        Set<SimpleGrantedAuthority> authorities = JsonNodeUtils.findValue(
                root, "authorities", new TypeReference<Set<SimpleGrantedAuthority>>(){}, mapper);
        // JsonNodeUtils.findValue(root, "authorities", TypeRef)
        boolean accountNonExpired = JsonNodeUtils.findBooleanValue(root, "accountNonExpired");
        boolean accountNonLocked = JsonNodeUtils.findBooleanValue(root, "accountNonLocked");
        boolean credentialsNonExpired = JsonNodeUtils.findBooleanValue(root, "credentialsNonExpired");
        boolean enabled = JsonNodeUtils.findBooleanValue(root, "enabled");
        String email = JsonNodeUtils.findStringValue(root, "email");
        String phone = JsonNodeUtils.findStringValue(root, "phone");
        //notification
        String externalUserId = JsonNodeUtils.findStringValue(root, "externalUserId");

        UserDetail userDetail = new UserDetail(userId, customerId, username, password, customerType,
                accessCode, enabled, accountNonExpired, credentialsNonExpired, accountNonLocked, authorities);
        userDetail.setEmail(email);
        userDetail.setPhone(phone);
        userDetail.setExternalUserId(externalUserId);
        return userDetail;
    }
}
