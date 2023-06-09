package com.tapdata.tm.cache.controller;

import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.thymeleaf.spring5.SpringTemplateEngine;

@Controller
@Setter(onMethod_ = {@Autowired})
public class CacheController {
    private SpringTemplateEngine templateEngine ;

    @RequestMapping("/cache/clear")
    public String clear() {
        templateEngine.clearTemplateCache();
        return "redirect:/";
    }
}
