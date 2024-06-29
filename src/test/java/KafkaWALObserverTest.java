
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.TableName;

public class KafkaWALObserverTest {
    public static void main(String[] args) {
        try {
            Configuration config = HBaseConfiguration.create();
            config.addResource("hbase-site.xml");
            Connection connection = ConnectionFactory.createConnection(config);

            TableName tableName = TableName.valueOf("test");
            Table table = connection.getTable(tableName);

            // Creating a sample Put operation to test WALObserver
            Put put = new Put(Bytes.toBytes("row1"));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("qual1"), Bytes.toBytes("value1"));
            table.put(put);

            table.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

