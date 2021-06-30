package com.demo.es.service.impl;

import com.demo.es.service.ImportService;
import com.demo.es.util.DBHelper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class ImportServiceImpl implements ImportService {
    @Autowired
    private DBHelper dbHelper;
    @Autowired
    private BulkProcessor bulkProcessor;
    private static final String ES_INDEX = "commonphrase_index";
    private static final String ES_INDEX_TYPE = "commonphrase_type";

    @Override
    public void importAll() throws IOException {
        writeMySQLDataToES("commonphrase");
    }

    private void writeMySQLDataToES(String tableName) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            connection = dbHelper.getConn();
            log.info("start handle data :" + tableName);
            String sql = "select * from " + tableName;
            ps = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            // 根据自己需要设置 fetchSize
            ps.setFetchSize(100);
            rs = ps.executeQuery();

            int count = 0;
            // c 就是列的名字   v 就是列对应的值
            String c = null;
            String v = null;
            Map<String, Object> map = null;
            ResultSetMetaData colData = rs.getMetaData();
            List<Map<String, Object>> dataList = new ArrayList<>();
            while (rs.next()) {
                count++;
                map = new HashMap(128);
                for (int i = 1; i <= colData.getColumnCount(); i++) {
                    c = colData.getColumnName(i);
                    v = rs.getString(c);
                    if ("id".equals(c)) {
                        map.put("commonPhraseId", v);
                        continue;
                    } else if (c.contains("DateTime") && v != null && !v.isEmpty()) {
                        map.put(c, rs.getTimestamp(c).getTime());
                        continue;
                    } else if ("tenantId".equals(c) || "seq".equals(c)) {
                        map.put(c, Integer.valueOf(v));
                        continue;
                    } else if ("parentId".equals(c)) {
                        map.put(c, Long.valueOf(v));
                        continue;
                    } else if ("leaf".equals(c) || "deleted".equals(c)) {
                        map.put(c, "0".equals(v) ? false : true);
                        continue;
                    }
                    map.put(c, v);
                }
                dataList.add(map);
                // 每1万条 写一次   不足的批次的数据 最后一次提交处理
                if (count % 10000 == 0) {
                    log.info("mysql handle data  number:" + count);
                    // 将数据添加到 bulkProcessor
                    for (Map<String, Object> hashMap2 : dataList) {
                        bulkProcessor.add(new IndexRequest(ES_INDEX, ES_INDEX_TYPE, String.valueOf(hashMap2.get("commonPhraseId"))).source(hashMap2));
                    }
                    // 每提交一次 清空 map 和  dataList
                    map.clear();
                    dataList.clear();
                    bulkProcessor.flush();
                }
            }
            // 处理 未提交的数据
            for (Map<String, Object> hashMap2 : dataList) {
                bulkProcessor.add(new IndexRequest(ES_INDEX, ES_INDEX_TYPE, String.valueOf(hashMap2.get("commonPhraseId"))).source(hashMap2));
            }
            bulkProcessor.flush();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                ps.close();
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

