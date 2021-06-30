package com.demo.es.config;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Slf4j
@Configuration
public class EsConfig {
    @Value("${elasticsearch.rest.uris}")
    private String hostlist;

    @Value("${elasticsearch.rest.clustername:}")
    private String clustername;

    @Bean
    public Client client() throws UnknownHostException {
        String[] split = hostlist.split(",");
        InetSocketTransportAddress[] addresses = new InetSocketTransportAddress[split.length];
        for (int i = 0; i < split.length; i++) {
            String item = split[i];
            System.out.println(item);
            addresses[i] = new InetSocketTransportAddress(InetAddress.getByName(item.split(":")[0]), Integer.parseInt(item.split(":")[1]));
        }
        TransportClient.Builder builder = TransportClient.builder();
        if (clustername != null && !clustername.isEmpty()) {
            Settings settings = Settings.builder().put("cluster.name", clustername).build();
            builder.settings(settings);
        }
        Client client = builder.build().addTransportAddresses(addresses);
        return client;
    }

    @Bean
    public BulkProcessor bulkProcessor(Client client) {

        return BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                log.info("Try to insert data number : "
                        + bulkRequest.numberOfActions());
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                log.info("************** Success insert data number : {} , {}"
                        , bulkRequest.numberOfActions(), bulkResponse.buildFailureMessage());
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                log.error("{} data bulk failed,reason :{}", bulkRequest.numberOfActions(), throwable);
            }

        }).setBulkActions(15000)
                .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(100))
                .setConcurrentRequests(10)
//                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
    }

}