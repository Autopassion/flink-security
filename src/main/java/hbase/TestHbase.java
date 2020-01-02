package hbase;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestHbase {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStreamSource<Tuple2<String, String>> hbaseSource = env.addSource(new HbaseReader());
        final SingleOutputStreamOperator<String> map = hbaseSource.map(tuple2 -> {
            final StringBuilder builder = new StringBuilder();
            builder.append(tuple2.f0 + ":" + tuple2.f1);
            return builder.toString();
        });
        map.print();
        env.execute();
    }
}
