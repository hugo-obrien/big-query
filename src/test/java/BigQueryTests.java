import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.gubenko.bigquery.ApplicationContext;
import com.gubenko.bigquery.BigQueryDataGateway;
import com.gubenko.bigquery.BigQueryManager;
import com.gubenko.bigquery.QueryUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.*;

@Slf4j
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ApplicationContext.class)
public class BigQueryTests {

    private final String TEST_DATASET = "create_dataset_test";
    private final String TEST_TABLE = "test_table";

    @Autowired
    private BigQueryManager bigQueryManager;
    @Autowired
    private BigQueryDataGateway gateway;

    @Test
    public void test() {
        log.info("BigQuery initialization test");
        bigQueryManager.bigQuery();
    }

    @SneakyThrows
    @Test
    public void testQuery() {
        String query = String.format("select * from %s.%s;", TEST_DATASET, TEST_TABLE);
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
        TableResult result = bigQueryManager.bigQuery().query(queryConfig);
        Assert.assertNotNull(result);
        QueryUtils.printQueryResult(result);
    }

    @Test
    public void insertRowTest() {
        Map<String, Object> rowContent = new HashMap<>();
        Date date = new Date();
        rowContent.put("id", date.hashCode());
        rowContent.put("user_fk", date.hashCode());
        rowContent.put("session_version", date.toString());
        rowContent.put("timestamp", date.getTime() / 1000);

        Map response = gateway.insertRow(TEST_DATASET, TEST_TABLE, rowContent);

        if (!response.isEmpty()) {
            response.forEach((k, v) -> {
                log.error("Response error: {}", v);
            });
        }
        Assert.assertTrue(response.isEmpty());
    }

    @Test
    public void insertMultipleRowsTest() {
        List<Map<String, Object>> rows = new ArrayList<>();

        Map<String, Object> rowContent1 = new HashMap<>();
        Date date = new Date();
        rowContent1.put("id", date.hashCode());
        rowContent1.put("user_fk", date.hashCode());
        rowContent1.put("session_version", date.toString());
        rowContent1.put("timestamp", date.getTime() / 1000);
        rows.add(rowContent1);

        Map<String, Object> rowContent2 = new HashMap<>();
        date = new Date();
        rowContent2.put("id", date.hashCode());
        rowContent2.put("user_fk", date.hashCode());
        rowContent2.put("timestamp", date.getTime() / 1000);
        rows.add(rowContent2);

        Map response = gateway.insertMultipleRows(TEST_DATASET, TEST_TABLE, rows);
        if (!response.isEmpty()) {
            response.forEach((k, v) -> {
                log.error("Response error: {}", v);
            });
        }
        Assert.assertTrue(response.isEmpty());
    }

    @SneakyThrows
    @Test
    public void metadataTest() {
        String metadataQuery = "select * from `create_dataset_test.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` where table_name = 'metadata_test';";
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(metadataQuery).build();
        TableResult result = bigQueryManager.bigQuery().query(queryConfig);
        Assert.assertNotNull(result);
        QueryUtils.printQueryResult(result);
    }
}
