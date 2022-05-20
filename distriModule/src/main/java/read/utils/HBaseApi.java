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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    //定义日志
    private Logger logger = LoggerFactory.getLogger(HBaseApi.class);
    /*
    * insert one second data to HBase Table
    * */
    public void addOneSecondRowData(String qualify, List<DataElement> dataElementList,Table table){
        try {
            //Table table = connection.getTable(TableName.valueOf("dq"));
            String rowKey = dataElementList.get(0).getDataCalendar();
            //List<Put> putList = new ArrayList<>();
            Put put = new Put(Bytes.toBytes(rowKey));
            for (DataElement dataElement:dataElementList){
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes(qualify),System.currentTimeMillis()+1,Bytes.toBytes(String.valueOf(dataElement)));
                table.put(put);
            }
            System.out.println("插入一秒数据成功");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
