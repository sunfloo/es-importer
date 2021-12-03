package com.demo.es.service;

import java.io.IOException;

public interface ReindexService {
    /**
     * 重新索引
     */
    boolean reIndex() throws IOException;
}
