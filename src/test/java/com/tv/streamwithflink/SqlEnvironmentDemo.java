package com.tv.streamwithflink;

// **********************
// 1. FLINK STREAMING QUERY
// **********************
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;

// ******************
// 2. FLINK BATCH QUERY
// ******************
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

// **********************
// 3. BLINK STREAMING QUERY
// **********************
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// ******************
// 4. BLINK BATCH QUERY
// ******************
import static org.apache.flink.table.api.Expressions.$;


/**
 * @Description
 * @Author Allen
 * @Date 2020-11-11 15:34
 **/
public class SqlEnvironmentDemo {
    public static void main(String[] args) {

        // **********************
        // 1. FLINK STREAMING QUERY
        // **********************
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
        // or TableEnvironment fsTableEnv = TableEnvironment.create(fsSettings);

        // ******************
        // 2. FLINK BATCH QUERY
        // ******************
        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbEnv);

        // **********************
        // 3. BLINK STREAMING QUERY
        // **********************
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        // or TableEnvironment bsTableEnv = TableEnvironment.create(bsSettings);

        // ******************
        // 4. BLINK BATCH QUERY
        // ******************
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);

        bbTableEnv.useCatalog("custom_catalog");
        bbTableEnv.useDatabase("custom_database");

        Table orders = bbTableEnv.from("Orders");
        // compute revenue for all customers from France
        Table revenue = orders
                .filter($("cCountry").isEqual("FRANCE"))
                .groupBy($("cID"), $("cName"))
                        .select($("cID"), $("cName"), $("revenue").sum().as("revSum"));



    }

}
