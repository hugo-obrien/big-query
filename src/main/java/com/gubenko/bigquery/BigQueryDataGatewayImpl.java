package com.gubenko.bigquery;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class BigQueryDataGatewayImpl implements BigQueryDataGateway {
    @Value("${bq.batch.size:10000}")
    private Integer bqBatchSize;

    @Autowired
    private BigQueryManager manager;

    public Map insertRow(String dataset, String table, Map<String, Object> content) {
        TableId tableId = TableId.of(dataset, table);
        InsertAllResponse response = manager.bigQuery().insertAll(
                InsertAllRequest.newBuilder(tableId)
                        .addRow(content)
                        .build());
        return response.getInsertErrors();
    }

    @Override
    public Map<Long, List<BigQueryError>> insertMultipleRows(String dataset, String table, List<Map<String, Object>> content) {
        Map<Long, List<BigQueryError>> result = new HashMap<>();
        Queue queue = new LinkedList(content);
        while (!queue.isEmpty()) {
            List page = new ArrayList();
            while (!queue.isEmpty() && page.size() <= bqBatchSize) {
                page.add(queue.poll());
            }
            result.putAll(insertAll(dataset, table, page));
        }
        return result;
    }

    @Override
    public Map<Long, List<BigQueryError>> insertAll(String dataset, String table, List<Map<String, Object>> content) {
        TableId tableId = TableId.of(dataset, table);
        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(tableId);
        List<Map<String, Object>> rows = new ArrayList<>(content);
        rows.forEach(builder::addRow);
        InsertAllResponse response = manager.bigQuery().insertAll(builder.build());
        return response.getInsertErrors();
    }
}
