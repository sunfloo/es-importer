package com.demo.es.controller;

import com.demo.es.service.ImportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;

@Controller
public class PositionController {
    @Autowired
    private ImportService service;

    @RequestMapping("/importAll")
    @ResponseBody
    public   String  importAll(){
        try {
            service.importAll();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  "success";
    }
}
