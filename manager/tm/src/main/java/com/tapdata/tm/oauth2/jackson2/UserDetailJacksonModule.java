package com.tapdata.tm.oauth2.jackson2;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import org.springframework.security.jackson2.SecurityJackson2Modules;
import org.springframework.security.jackson2.SimpleGrantedAuthorityMixin;

import java.util.HashSet;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/24 下午7:55
 */
public class UserDetailJacksonModule extends SimpleModule {

    public UserDetailJacksonModule() {
        super(UserDetail.class.getName(), new Version(1, 0, 0, null, null, null));
    }

    @Override
    public void setupModule(SetupContext context) {
        SecurityJackson2Modules.enableDefaultTyping(context.getOwner());
        context.setMixInAnnotations(HashSet.class, HashSetMixin.class);
        context.setMixInAnnotations(Long.class, LongMixin.class);
        context.setMixInAnnotations(SimpleGrantedAuthority.class, SimpleGrantedAuthorityMixin.class);
        context.setMixInAnnotations(UserDetail.class, UserDetailMixin.class);
    }
}
