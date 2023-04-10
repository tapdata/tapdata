package com.tapdata.tm.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.coyote.ProtocolHandler;
import org.apache.coyote.http11.Http11NioProtocol;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.multipart.MultipartResolver;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/10 8:26 下午
 * @description
 */
@Configuration
@Slf4j
public class BeanConfig {
    @Value("${server.keepAliveTimeout:60000}")
    private String keepAliveTimeoutStr;
    @Value("${server.maxKeepAliveRequests:1024}")
    private String maxKeepAliveRequestsStr;

    @Bean(name = "multipartResolver")
    public MultipartResolver multipartResolver() {
        CommonsMultipartResolver resolver = new CommonsMultipartResolver();
        resolver.setDefaultEncoding("UTF-8");
        return resolver;
    }

    @Bean
    public TomcatServletWebServerFactory tomcatServletWebServerFactory(){
        int keepAliveTimeout = parseInt(keepAliveTimeoutStr, 60000);
        int maxKeepAliveRequests = parseInt(maxKeepAliveRequestsStr, 1024);

        TomcatServletWebServerFactory tomcatServletWebServerFactory = new TomcatServletWebServerFactory();
        tomcatServletWebServerFactory.addConnectorCustomizers((connector)->{
            ProtocolHandler protocolHandler = connector.getProtocolHandler();
            if(protocolHandler instanceof Http11NioProtocol){
                Http11NioProtocol http11NioProtocol = (Http11NioProtocol)protocolHandler;
                http11NioProtocol.setKeepAliveTimeout(keepAliveTimeout);//millisecond
                http11NioProtocol.setMaxKeepAliveRequests(maxKeepAliveRequests);
            }
        });
        return tomcatServletWebServerFactory;
    }

    private Pattern pattern = Pattern.compile("^\\d+$");
    private int parseInt(String str, int defaultVal) {
        Matcher m = pattern.matcher(str);
        if (m.matches()) {
            return Integer.parseInt(str);
        }
        return defaultVal;
    }
}
