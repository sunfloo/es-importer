package com.demo.es.service.impl;

import com.demo.es.service.ReindexService;
import com.demo.es.util.EsHelper;
import com.demo.es.util.ScrolledResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.elasticsearch.core.query.AliasBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class ReindexServiceImpl implements ReindexService {
    @Value("${es.cs.oldIndex}")
    private String oldIndex;
    @Value("${es.cs.newIndex}")
    private String newIndex;
    @Value("${es.cs.indexType:_doc}")
    private String indexType;
    @Value("${es.cs.oldIndexIdName:msgId}")
    private String oldIndexIdName;
    @Value("${es.cs.newIndexMapping}")
    private String newIndexMapping;
    @Value("${es.cs.needCreateNewIndex:false}")
    private Boolean needCreateNewIndex;
    @Value("${es.cs.needDeleteOldIndex:true}")
    private Boolean needDeleteOldIndex;
    @Value("${es.cs.scrollSize:1000}")
    private Integer scrollSize;
    @Value("${es.cs.scrollExpire:60000}")
    private Integer scrollExpire;
    @Autowired
    private BulkProcessor bulkProcessor;
    @Autowired
    private EsHelper esHelper;

    @Override
    public boolean reIndex() {
        if (!check()) {
            return false;
        }

        createNewIndex();

        carryData();

        deleteOldIndex();

        connectNewIndexToOld();

        return true;
    }

    private boolean check() {
        if (!esHelper.indexExists(oldIndex)) {
            log.error("old index {} not exists, stop reindex", oldIndex);
            return false;
        }
        return true;
    }

    private void connectNewIndexToOld() {
        long start = System.currentTimeMillis();
        esHelper.addAlias(new AliasBuilder()
                .withIndexName(newIndex)
                .withAliasName(oldIndex)
                .build());
        log.info("create alias {} for index {} success, cost {} ms",
                newIndex, oldIndex, System.currentTimeMillis() - start);
    }

    private void deleteOldIndex() {
        if (needDeleteOldIndex) {
            long start = System.currentTimeMillis();
            boolean deleted = esHelper.deleteIndex(oldIndex);
            if (deleted) {
                log.info("delete old index {} success, cost {} ms",
                        oldIndex, System.currentTimeMillis() - start);
            } else {
                log.info("delete old index {} failed", oldIndex);
            }
        }
    }

    private void carryData() {
        ScrolledResult<Map> scrollResult = esHelper.startScroll(
                scrollExpire, scrollSize, esHelper.getAllQuery(oldIndex, indexType), Map.class);

        int count = 0;
        List<Map> cacheList = new ArrayList();
        while (scrollResult != null && scrollResult.getResultList().size() > 0) {
            List<Map> resultList = scrollResult.getResultList();
            cacheList.addAll(resultList);
            count += resultList.size();
            // 每1万条 写一次  不足的批次的数据 最后一次提交处理
            if (count % 10000 == 0) {
                log.info("es bulk submit data to index {} type {}, size {}, total {}",
                        newIndex, indexType, resultList.size(), count);

                for (Map<String, Object> data : cacheList) {
                    bulkProcessor.add(new IndexRequest(newIndex, indexType,
                            String.valueOf(data.get(oldIndexIdName)))
                            .source(data));
                }
                cacheList.clear();
                bulkProcessor.flush();
            }
            scrollResult = esHelper.continueScroll(scrollResult.getScrollId(), scrollExpire, Map.class);
        }
        
        if (!CollectionUtils.isEmpty(cacheList)) {
            log.info("es bulk submit data to index {} size {} type {}, total {}",
                    newIndex, indexType, cacheList.size(), count);
        }
        // 处理剩余未提交的数据
        for (Map<String, Object> data : cacheList) {
            bulkProcessor.add(new IndexRequest(newIndex, indexType,
                    String.valueOf(data.get(oldIndexIdName)))
                    .source(data));
        }
        bulkProcessor.flush();
    }

    private void createNewIndex() {
        if (needCreateNewIndex) {
            if (esHelper.indexExists(newIndex)) {
                log.warn("new index {} exists, skip create", newIndex);
            } else {
                if (StringUtils.isBlank(newIndexMapping)) {
                    log.error("newIndexMapping is needed");
                    throw new IllegalArgumentException("newIndexMapping is needed");
                }
                log.info("create new index with name {} type {} start", newIndex, indexType);
                esHelper.createIndex(newIndex);
                esHelper.putMapping(newIndex, indexType, newIndexMapping);
                log.info("create new index with name {} type {} success", newIndex, indexType);
            }
        }
    }
}

