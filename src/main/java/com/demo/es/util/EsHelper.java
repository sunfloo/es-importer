package com.demo.es.util;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.query.AliasQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.elasticsearch.client.Requests.indicesExistsRequest;
import static org.elasticsearch.cluster.metadata.AliasAction.Type.ADD;

/**
 * @author songrunxin
 * @date 2021/12/2 4:35 下午
 */
@Component
public class EsHelper {

    @Autowired
    private Client client;
    @Autowired
    private CustomElasticsearchEngine engine;

    public boolean createIndex(String indexName, Object settings) {
        CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName);
        if (settings instanceof String) {
            createIndexRequestBuilder.setSettings(String.valueOf(settings));
        } else if (settings instanceof Map) {
            createIndexRequestBuilder.setSettings((Map) settings);
        } else if (settings instanceof XContentBuilder) {
            createIndexRequestBuilder.setSettings((XContentBuilder) settings);
        }
        return createIndexRequestBuilder.execute().actionGet().isAcknowledged();
    }

    public boolean createIndex(String indexName) {
        Assert.notNull(indexName, "No index defined for Query");
        return client.admin().indices()
                .create(Requests.createIndexRequest(indexName))
                .actionGet().isAcknowledged();
    }

    public boolean putMapping(String indexName, String type, Object mapping) {
        Assert.notNull(indexName, "No index defined for putMapping()");
        Assert.notNull(type, "No type defined for putMapping()");
        PutMappingRequestBuilder requestBuilder = client.admin().indices()
                .preparePutMapping(indexName).setType(type);
        if (mapping instanceof String) {
            requestBuilder.setSource(String.valueOf(mapping));
        } else if (mapping instanceof Map) {
            requestBuilder.setSource((Map) mapping);
        } else if (mapping instanceof XContentBuilder) {
            requestBuilder.setSource((XContentBuilder) mapping);
        }
        return requestBuilder.execute().actionGet().isAcknowledged();
    }

    public boolean deleteIndex(String indexName) {
        Assert.notNull(indexName, "No index defined for delete operation");
        if (indexExists(indexName)) {
            return client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet().isAcknowledged();
        }
        return false;
    }

    public Boolean addAlias(AliasQuery query) {
        Assert.notNull(query.getIndexName(), "No index defined for Alias");
        Assert.notNull(query.getAliasName(), "No alias defined");
        AliasAction aliasAction = new AliasAction(ADD, query.getIndexName(), query.getAliasName());
        if (query.getFilterBuilder() != null) {
            aliasAction.filter(query.getFilterBuilder());
        } else if (query.getFilter() != null) {
            aliasAction.filter(query.getFilter());
        } else if (isNotBlank(query.getRouting())) {
            aliasAction.routing(query.getRouting());
        } else if (isNotBlank(query.getSearchRouting())) {
            aliasAction.searchRouting(query.getSearchRouting());
        } else if (isNotBlank(query.getIndexRouting())) {
            aliasAction.indexRouting(query.getIndexRouting());
        }
        return client.admin().indices().prepareAliases().addAliasAction(aliasAction).execute().actionGet().isAcknowledged();
    }

    public boolean indexExists(String indexName) {
        return client.admin().indices().exists(indicesExistsRequest(indexName)).actionGet().isExists();
    }

    public <T> ScrolledResult<T> startScroll(long scrollTimeInMills, int size, SearchQuery query, Class<T> clazz) {
        return engine.startScroll(scrollTimeInMills, size, query, clazz);
    }

    public <T> ScrolledResult<T> continueScroll(@Nullable String scrollId, long scrollTimeInMillis, Class<T> clazz) {
        return engine.continueScroll(scrollId, scrollTimeInMillis, clazz);
    }

    public SearchQuery getAllQuery(String index, String type) {
        return new NativeSearchQueryBuilder()
                .withIndices(index).withTypes(type)
                .withQuery(QueryBuilders.matchAllQuery())
                .build();

    }
}