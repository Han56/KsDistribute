package read.utils;

import entity.DataElement;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author han56
 * @description 功能描述
 * @create 2021/12/23 下午3:41
 */
public class HBaseApi {

    public static Configuration configuration;
    public static Connection connection;
    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop101,hadoop102,hadoop103");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
    * insert one second data to HBase Table
    * */
    public void addOneSecondRowData(String columnFam, List<DataElement> dataElementList){
        try {
            Table table = connection.getTable(TableName.valueOf("disquake"));
            String rowKey = dataElementList.get(0).getDataCalendar();
            List<Put> putList = new ArrayList<>();
            Put put = new Put(Bytes.toBytes(rowKey));
            for (DataElement dataElement:dataElementList){
                put.addColumn(Bytes.toBytes(columnFam),Bytes.toBytes("x1"),System.currentTimeMillis(),Bytes.toBytes(String.valueOf(dataElement.getX1())));
                put.addColumn(Bytes.toBytes(columnFam),Bytes.toBytes("y1"),System.currentTimeMillis(),Bytes.toBytes(String.valueOf(dataElement.getY1())));
                put.addColumn(Bytes.toBytes(columnFam),Bytes.toBytes("z1"),System.currentTimeMillis(),Bytes.toBytes(String.valueOf(dataElement.getZ1())));
                put.addColumn(Bytes.toBytes(columnFam),Bytes.toBytes("x2"),System.currentTimeMillis(),Bytes.toBytes(String.valueOf(dataElement.getX2())));
                put.addColumn(Bytes.toBytes(columnFam),Bytes.toBytes("y2"),System.currentTimeMillis(),Bytes.toBytes(String.valueOf(dataElement.getY2())));
                put.addColumn(Bytes.toBytes(columnFam),Bytes.toBytes("z2"),System.currentTimeMillis(),Bytes.toBytes(String.valueOf(dataElement.getZ2())));
                putList.add(put);
                Thread.sleep(1);
            }
            table.put(putList);
            table.close();
            System.out.println("插入一秒数据成功");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
