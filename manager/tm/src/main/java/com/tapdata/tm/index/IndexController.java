package com.tapdata.tm.index;

import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.utils.WebUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/11/16 上午10:29
 */
@Controller
public class IndexController {

    @Value("${application.title}")
    private String name;
    @Value("${application.version}")
    private String mainVersion;
    @Value("${application.commit_version}")
    private String commitVersion;
    @Value("${application.build}")
    private String build;

    @GetMapping
    public String index() {
        return "/index.html";
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
}
