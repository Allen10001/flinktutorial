package com.tv.streamwithflink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;

/**
 * @Description
 * @Author Allen
 * @Date 2020-11-20 10:39
 **/
public class KafkaConnectorDemo {
    public static void main(String[] args) {

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(bsEnv, bsSettings);

        tableEnvironment
                // declare the external system to connect to
                .connect(
                        new Kafka()
                                .version("0.10")
                                .topic("test-input")
                                .startFromEarliest()
                                .property("bootstrap.servers", "localhost:9092")
                )

                // declare a format for this system
                .withFormat(
                    new Json()
                        .failOnMissingField(false)
                )

                // declare the schema of the table
                .withSchema(
                        new Schema()
                                .field("rowtime", DataTypes.TIMESTAMP(3))
                                .rowtime(new Rowtime()
                                        .timestampsFromField("timestamp")
                                        .watermarksPeriodicBounded(60000)
                                )
                                .field("user", DataTypes.BIGINT())
                                .field("message", DataTypes.STRING())
                )

                // create a table with given name
                .createTemporaryTable("MyUserTable");



        tableEnvironment.connect(new FileSystem()
                .path("file:///path/to/whatever"))
                .withFormat(new OldCsv());


    }
}
