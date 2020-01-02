package kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;

import java.util.Properties;

/**
 * KafkaReader
 */
public class KafkaTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "banana-master-01");
        props.setProperty("group.id", "flink-group");
        FlinkKafkaConsumer09<String> consumer =
            new FlinkKafkaConsumer09<>("test", new SimpleStringSchema(), props);
        final DataStreamSource<String> stringDataStreamSource = env.addSource(consumer);
        stringDataStreamSource.print();
        env.execute();
    }
}
