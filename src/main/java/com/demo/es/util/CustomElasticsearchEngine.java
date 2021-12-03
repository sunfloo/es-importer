package com.demo.es.util;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.DefaultResultMapper;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@Component
public class CustomElasticsearchEngine {

    @Autowired
    private Client client;

    public <T> ScrolledResult<T> startScroll(long scrollTimeInMills, int size, SearchQuery query, Class<T> clazz) {
        SearchRequestBuilder requestBuilder = client.prepareSearch(toArray(query.getIndices()))
                .setTypes(toArray(query.getTypes()))
                .setScroll(TimeValue.timeValueMillis(scrollTimeInMills))
                .setFrom(0)
                .setSize(size);
        if (!CollectionUtils.isEmpty(query.getElasticsearchSorts())) {
            for (SortBuilder sortBuilder : query.getElasticsearchSorts()) {
                requestBuilder.addSort(sortBuilder);
            }
        }

        if (!CollectionUtils.isEmpty(query.getFields())) {
            requestBuilder.setFetchSource(toArray(query.getFields()), null);
        }
        if (query.getFilter() != null) {
            requestBuilder.setPostFilter(query.getFilter());
        }
        requestBuilder.setQuery(query.getQuery());
        SearchResponse response = getSearchResponse(requestBuilder.execute());
        return mapResults(response, clazz);
    }

    private static String[] toArray(List<String> values) {
        String[] valuesAsArray = new String[values.size()];
        return values.toArray(valuesAsArray);
    }

    public <T> ScrolledResult<T> continueScroll(@Nullable String scrollId, long scrollTimeInMillis, Class<T> clazz) {

        SearchResponse response = getSearchResponse(client.prepareSearchScroll(scrollId)
                .setScroll(TimeValue.timeValueMillis(scrollTimeInMillis)).execute());
        return mapResults(response, clazz);
    }

    public <T> Page<T> getPage(ScrolledResult<T> result, Pageable pageable) {
        if (result == null || result.getResultList() == null || result.getResultList().isEmpty()) {
            return new PageImpl(Collections.emptyList());
        }
        if (pageable == null) {
            return new PageImpl(result.getResultList());
        }
        return new PageImpl(result.getResultList(), pageable, result.getResultList().size());
    }

    private SearchResponse getSearchResponse(ActionFuture<SearchResponse> response) {
        return response.actionGet();
    }

    private <T> ScrolledResult<T> mapResults(SearchResponse response, Class<T> clazz) {
        List<T> results = new ArrayList<>();
        for (SearchHit searchHit : response.getHits()) {
            if (searchHit != null) {
                T result = new DefaultResultMapper().mapEntity(searchHit.getSourceAsString(), clazz);
                results.add(result);
            }
        }
        return new ScrolledResult(response.getScrollId(), results);
    }
}
