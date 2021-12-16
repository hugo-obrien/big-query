package com.gubenko.bigquery;

import com.google.cloud.bigquery.TableResult;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryUtils {

    public static void printQueryResult(TableResult result) {
        StringBuilder builder = new StringBuilder();
        result.getSchema().getFields().forEach(field -> {
            builder.append(field.getName()).append(" | ");
        });
        log.info(builder.toString());
        builder.setLength(0);

        result.iterateAll().forEach(
                row -> {
                    row.forEach(
                            val -> {
                                builder.append(val.getValue()).append(" | ");
                            });
                    log.info(builder.toString());
                    builder.setLength(0);
                });
    }
}
