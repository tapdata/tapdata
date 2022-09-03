package com.tapdata.tm.proxy.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.proxy.dto.LoginDto;
import com.tapdata.tm.proxy.dto.ProxyTokenDto;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.tapdata.pdk.core.utils.JWTUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;


@Tag(name = "Proxy", description = "代理网关相关接口")
@Slf4j
@RestController
@RequestMapping("/api/proxy")
public class ProxyController extends BaseController {
    private static final String key = "asdfFSDJKFHKLASHJDKQJWKJehrklHDFJKSMhkj3h24jkhhJKASDH723ty4jkhasdkdfjhaksjdfjfhJDJKLHSAfadsf";
    private static final int wsPort = 8246;
    /**
     *
     * @return
     */
    @Operation(summary = "Generate jwt token")
    @PostMapping()
    public ResponseMessage<ProxyTokenDto> save(@RequestBody LoginDto loginDto, HttpServletRequest request) {
        if(loginDto == null || loginDto.getClientId() == null || loginDto.getService() == null || loginDto.getTerminal() == null)
            throw new BizException("loginDto is illegal, " + loginDto);
        if(StringUtils.isEmpty(loginDto.getService()) || !loginDto.getService().equalsIgnoreCase("engine"))
            throw new BizException("Illegal service for generating token");
        UserDetail userDetail = getLoginUser();

        ProxyTokenDto proxyTokenDto = new ProxyTokenDto();
        String token = JWTUtils.createToken(key,
                map(
                        entry("service", loginDto.getService().toLowerCase()),
                        entry("clientId", loginDto.getClientId()),
                        entry("terminal", loginDto.getTerminal()),
                        entry("uid", userDetail.getUserId()),
                        entry("cid", userDetail.getCustomerId())
                ), 30000L);
        proxyTokenDto.setToken(token);
        proxyTokenDto.setWsPort(wsPort);
        proxyTokenDto.setWsPath(loginDto.getService());
        return success(proxyTokenDto);
    }
}