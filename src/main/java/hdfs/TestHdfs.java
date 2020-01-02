package hdfs;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * hdfs测试
 */
public class TestHdfs {
    
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final DataSource<String> text = env.readTextFile(args[0]);
        final AggregateOperator<Tuple2<String, Integer>> result = text.flatMap(new Tokenizer()).groupBy(0).sum(1);
        result.print();
        result.writeAsText(args[1]);
        env.execute();
    }
    
    
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
    
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.toLowerCase().split(",");
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
