package com.demo.es.util;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;


@Data
@AllArgsConstructor
public class ScrolledResult<T> {

    private String scrollId;
    private List<T> resultList;
}
