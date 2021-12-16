package com.gubenko.bigquery;

import com.google.cloud.bigquery.BigQueryError;

import java.util.List;
import java.util.Map;

public interface BigQueryDataGateway {

    Map insertRow(String dataset, String table, Map<String, Object> content);

    Map<Long, List<BigQueryError>> insertMultipleRows(String dataset, String table, List<Map<String, Object>> content);

    Map<Long, List<BigQueryError>> insertAll(String dataset, String table, List<Map<String, Object>> content);
}
