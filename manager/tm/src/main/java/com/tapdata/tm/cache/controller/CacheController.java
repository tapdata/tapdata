package com.tapdata.tm.cache.controller;

import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.thymeleaf.spring6.SpringTemplateEngine;


@Controller("/cache")
@Setter(onMethod_ = {@Autowired})
public class CacheController {
    private SpringTemplateEngine templateEngine ;

    @RequestMapping
    public String cacheIndex() {
        templateEngine.clearTemplateCache();
        return "cache/index";
    }

    @RequestMapping("/clear")
    public String clear() {
        templateEngine.clearTemplateCache();
			return "redirect:/cache";
		}
}
