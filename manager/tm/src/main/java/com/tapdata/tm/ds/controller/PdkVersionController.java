package com.tapdata.tm.ds.controller;

import com.tapdata.tm.ds.dto.PdkVersionCheckDto;
import com.tapdata.tm.ds.service.impl.PkdSourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

@Controller
@RequestMapping("/check")
public class PdkVersionController {

    @Autowired
    private PkdSourceService pkdSourceService;
    @RequestMapping("/pdk_version")
    public String versionCheck(@RequestParam(value = "days", defaultValue = "7") int days, Model model) {
        List<PdkVersionCheckDto> data = pkdSourceService.versionCheck(days);
        model.addAttribute("pdks", data);
        return "checkVersion";
    }
}
