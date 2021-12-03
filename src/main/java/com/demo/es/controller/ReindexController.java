package com.demo.es.controller;

import com.demo.es.service.ReindexService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;

@Controller
public class ReindexController {
    @Autowired
    private ReindexService service;

   @RequestMapping("/_reindex")
    @ResponseBody
    public String reIndex() {
        try {
            if (!service.reIndex()) {
                return "failed";
            }
        } catch (IOException e) {
            e.printStackTrace();
            return "failed";
        } 
        return "success";
    }
}
