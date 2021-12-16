package com.gubenko.bigquery;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Objects;

@Service
@Slf4j
public class BigQueryManagerImpl implements BigQueryManager {

    @Value("${big.query.keys.file:bq-test-keys.json}")
    private String keyFile;

    private BigQuery bigQuery;
    private GoogleCredentials credentials;

    @PostConstruct
    private void initialize() {
        try {
            credentials = GoogleCredentials.fromStream(
                    Objects.requireNonNull(BigQueryManagerImpl.class.getClassLoader().getResourceAsStream(keyFile)));
            bigQuery = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
        } catch (Exception ex) {
            log.error("Unable to initialize BigQuery: {}", ex.getMessage(), ex);
        }
    }

    @Override
    public BigQuery bigQuery() {
        return bigQuery;
    }

    @Override
    public BigQueryWriteSettings getWriteSettings() {
        try {
            return BigQueryWriteSettings.newBuilder()
                    .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                    .build();
        } catch (IOException ex) {
            log.error("Unable to build BigQuery write settings: {}", ex.getMessage(), ex);
        }
        return null;
    }
}
