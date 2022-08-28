package io.tapdata.wsserver.channels.gateway;

import java.lang.annotation.*;

@Target(value = {ElementType.TYPE} )
@Retention(value = RetentionPolicy.RUNTIME)
@Documented
@interface GatewaySession {

}