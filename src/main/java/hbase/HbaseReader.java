package hbase;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.Iterator;

/**
 * HbaseReader
 */
public class HbaseReader extends RichSourceFunction<Tuple2<String,String>>{
    private Connection conn = null;
    private Scan scan = null;
    private Table table = null;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        final org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, "banana-master-01.ruisdata.com,banana-master-02.ruisdata.com,banana-master-03.ruisdata.com");
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        configuration.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,300);
        configuration.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 300);
        configuration.set("hadoop.security.authentication", "kerberos");
        conn = ConnectionFactory.createConnection(configuration);
        table = conn.getTable(TableName.valueOf("test:student"));
        scan = new Scan().addFamily(Bytes.toBytes("info"));
    }
    
    @Override
    public void run(SourceContext ctx) throws Exception {
        final ResultScanner result = table.getScanner(scan);
        final Iterator<Result> iterator = result.iterator();
        while (iterator.hasNext()) {
            final Result row = iterator.next();
            final String rowKey = Bytes.toString(row.getRow());
            final StringBuilder buffer = new StringBuilder();
            for (Cell cell : row.rawCells()) {
                final String value = Bytes.toString(CellUtil.cloneValue(cell));
                buffer.append(value+"_");
            }
            final String valueString = buffer.replace(buffer.length() - 1, buffer.length(), "").toString();
            final Tuple2<String, String> tuple2 = new Tuple2<>();
            tuple2.f0 = rowKey;
            tuple2.f1 = valueString;
            ctx.collect(tuple2);
        }
    }
    
    @Override
    public void cancel() {
    
    }
    
    @Override
    public void close() throws Exception {
        try {
            if (table != null) {
                table.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
