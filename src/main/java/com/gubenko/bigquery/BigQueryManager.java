package com.gubenko.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;

public interface BigQueryManager {

    BigQuery bigQuery();

    BigQueryWriteSettings getWriteSettings();
}
