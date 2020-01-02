package hive;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.*;
import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * HiveTest
 */
public class HiveTest {
    public static void main(String[] args) throws Exception {
        String name = "test";
        String defaultDatabase = "test";
        String hiveConfDir = "hive/conf";
        String version = "1.2.1";
        final EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance();
        final EnvironmentSettings environmentSettings = builder.build();
        final HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        final TableEnvironment tableEnvironment =  TableEnvironment.create(environmentSettings);
        tableEnvironment.registerCatalog("hive", hiveCatalog);
        final JobExecutionResult result = tableEnvironment.execute("select * from test.test");
        if (result != null) {
            System.out.println(result);
        }
        tableEnvironment.execute("hive-test");
    }
}
