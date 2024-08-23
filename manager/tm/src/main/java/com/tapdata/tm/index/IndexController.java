package com.tapdata.tm.index;

import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.utils.WebUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/11/16 上午10:29
 */
@Controller
@Slf4j
public class IndexController {

    @Value("${application.title}")
    private String name;
    @Value("${application.version}")
    private String mainVersion;
    @Value("${application.commit_version}")
    private String commitVersion;
    @Value("${application.build}")
    private String build;

    @Autowired
    @Qualifier("memoryCache")
    private CacheManager memoryCache;

    @GetMapping
    public String index() {
        return "forward:/index.html";
    }

    @GetMapping({"/docs", "/docs/"})
    public String forwardDocs() {
        return "forward:/docs/index.html";
    }

    @GetMapping({"/docs/en", "/docs/en/"})
    public String forwardDocsEn() {
        return "forward:/docs/en/index.html";
    }

    @GetMapping("/docs/**/{path:[^\\.]*}")
    public String deepForwardDocs() {
        return "forward:/docs/index.html";
    }

    @GetMapping("/docs/en/**/{path:[^\\.]*}")
    public String deepForwardDocsEn() {
        return "forward:/docs/en/index.html";
    }

    @GetMapping("/version")
    @ResponseBody
    public String version(HttpServletRequest request){
        String ip = WebUtils.getRealIpAddress(request);
        return String.format(
                "{\"name\": \"%s\", \"mainVersion\": \"%s\", \"commitVersion\": \"%s\", \"build\": \"%s\", \"clientIp\": \"%s\"}",
                name, mainVersion, commitVersion, build, ip);
    }

    @GetMapping("/health")
    @ResponseBody
    public ResponseMessage<Boolean> health() {
        return new ResponseMessage<>();
    }

    @GetMapping("/cache/{name}")
    @ResponseBody
    public ResponseMessage<String> cache(@PathVariable String name) {

        Collection<String> cacheNames = "all".equalsIgnoreCase(name) ?
                memoryCache.getCacheNames() : Arrays.asList(name.split(","));
        if (cacheNames.size() > 0) {
            cacheNames.forEach(n -> {
                Cache cache = memoryCache.getCache(n);
                if (cache != null) {
                    cache.clear();
                }
            });
        }

        ResponseMessage<String> resp = new ResponseMessage<>();
        try {
            resp.setData(java.net.InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return resp;
    }
}
