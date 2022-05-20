package apiTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author han56
 * @description 功能描述【HBase JAVAAPI】
 * @create 2021/10/4 下午6:16
 */
public class Api {

    public static Configuration configuration;
    public static Connection connection;
    public static CompletableFuture<AsyncConnection> asyncConnection;
    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop101,hadoop102,hadoop103");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            //创建异步HBase连接
            asyncConnection = ConnectionFactory.createAsyncConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    /*
     * 判断表名是否存在
     * */
    public static boolean isTableExits(String tableName) throws IOException {

        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }



    /*
     *  创建数据表
     * */
    public static void createTable(String table, List<String> columnFamily) throws IOException {
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        //判断是否存在表
        if (isTableExits(table)){
            System.out.println(table+"已经存在");
            return;
        }
        //创建表属性对象，表名需要转为字节
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(table));
        //创建多个列族
        for (String cf:columnFamily)
            descriptor.addFamily(new HColumnDescriptor(cf));
        //根据上述所写属性，创建数据表
        admin.createTable(descriptor);
        System.out.println(table+"已创建成功");
    }

    /*
     * 删除表方法
     * */
    public static void deleteTable(String tableName) throws IOException {
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        //判断是否存在表
        if (isTableExits(tableName)){
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println(tableName+"已删除成功");
            return;
        }
        System.out.println(tableName+"不存在");
    }

    /*
     * 向表中插入一行数据
     * */
    public static void addOneRowData(String tableName,String rowKey,String columnFamily,
                                     String column,String value) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
            table.put(put);
            table.close();
            System.out.println("插入数据成功！");
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /*
     * 获取全部数据
     * */
    public static void getAllRowsInfo(String tableName){
        CompletableFuture<Result> asyncResult = new CompletableFuture<>();

        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            //实例化扫描region的scan对象
            Scan scan = new Scan();
            ResultScanner results = table.getScanner(scan);
            for (Result result:results){
                Cell[] cells = result.rawCells();
                for (Cell cell:cells){
                    //得到rowKey
                    System.out.println("行键:"+Bytes.toString(CellUtil.cloneRow(cell)));
                    //得到列族
                    System.out.println("列族:"+Bytes.toString(CellUtil.cloneFamily(cell)));
                    //得到列
                    System.out.println("列:"+Bytes.toString(CellUtil.cloneQualifier(cell)));
                    //得到值
                    System.out.println("值:"+Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * 根据  rowKey 获取一行数据
     * */
    public static void getOneRowInfo(String tableName,String rowKey){
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.readAllVersions();
            Result result = table.get(get);
            for (Cell cell:result.rawCells()){
                //得到rowKey
                System.out.println("行键:"+Bytes.toString(result.getRow()));
                //得到列族
                System.out.println("列族:"+Bytes.toString(CellUtil.cloneFamily(cell)));
                //得到列
                System.out.println("列:"+Bytes.toString(CellUtil.cloneQualifier(cell)));
                //得到值
                System.out.println("值:"+Bytes.toString(CellUtil.cloneValue(cell)));
                //获取时间戳
                System.out.println("时间戳:"+cell.getTimestamp());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * 根据 列族：列 获取数据
     * */
    public static void getRowQualifier(String tableName,String rowKey,String family,String qualifier){
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            //根据 行键值 获取对应数据
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier));
            Result result = table.get(get);
            for (Cell cell:result.rawCells()){
                //得到rowKey
                System.out.println("行键:"+Bytes.toString(result.getRow()));
                //得到列族
                System.out.println("列族:"+Bytes.toString(CellUtil.cloneFamily(cell)));
                //得到列
                System.out.println("列:"+Bytes.toString(CellUtil.cloneQualifier(cell)));
                //得到值
                System.out.println("值:"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSystemTmp(){
        System.out.println(System.currentTimeMillis());
        System.out.println(System.currentTimeMillis()+1);
    }

    /*
    * 单线程插入多行数据 Put[]
    * */
    public static void putRowsData(int start,int end){
        try {
            Table table = connection.getTable(TableName.valueOf("test1"));
            String rowKey = "rowkey3";
            List<Put> putList = new ArrayList<>();
            Put put = new Put(Bytes.toBytes(rowKey));
//            for(int j=0;j<50;j++){
//
//            }
            //*(j+1)
            for (int i=start;i<end;i++){
                put.addColumn(Bytes.toBytes("fam1"),Bytes.toBytes("name"),System.currentTimeMillis(),Bytes.toBytes("branch"+i));
                put.addColumn(Bytes.toBytes("fam1"),Bytes.toBytes("age"),System.currentTimeMillis(),Bytes.toBytes(String.valueOf(i)));
                putList.add(put);
                Thread.sleep(1);
            }
            table.put(putList);
            putList.clear();

            table.close();
            System.out.println("批量插入成功");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /*
    * 多线程并行插入多行数据
    * */
    public static void insertMultiProcess() throws  InterruptedException {
        //start stop 用于程序计时
        long start = System.currentTimeMillis();
        Thread t1 = new Thread(){
            public void run(){
                try {
                    putRowsData(0,200);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        };
        Thread t2 = new Thread(){
            public void run(){
                try {
                    putRowsData(200,400);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        };
        Thread t3 = new Thread(){
            public void run(){
                try {
                    putRowsData(400,600);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        };
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
        System.out.print("用时:");
        System.out.println(System.currentTimeMillis()-start);
    }

    /*
     * 删除多行数据
     * */
    public static void deleteMutiRow(String tableName,List<String> rows){
        try{
            Table table = connection.getTable(TableName.valueOf(tableName));
            List<Delete> deleteList = new ArrayList<>();
            for (String row:rows){
                Delete delete = new Delete(Bytes.toBytes(row));
                deleteList.add(delete);
            }
            table.delete(deleteList);
            table.close();
            System.out.println("删除操作成功！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
//        System.out.println("====判断数据表是否存在====");
//        boolean f = isTableExits("test1");
//        System.out.println(f);

        /*
        * 查看插入数据
        * */
        Table table = connection.getTable(TableName.valueOf("dq"));
        Get get = new Get(Bytes.toBytes("2019-09-2511:05:20"));
        get.readAllVersions();
        get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("V"));
        Result result = table.get(get);
        int sum = 0;
        for (Cell cell: result.rawCells()){
            System.out.println("行键:"+Bytes.toString(result.getRow()));
            System.out.println("列族:"+Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:"+Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:"+Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:"+cell.getTimestamp());
            System.out.println("版本数："+sum++);
        }

/*      System.out.println("====测试创建test表====");
        List<String> columnFamily = new LinkedList<>();
        String column_fam_one = "personal_info";
        columnFamily.add(column_fam_one);
        createTable(tableName,columnFamily);*/

/*        System.out.println("====删除表测试====");
        deleteTable(tableName);*/

/*        System.out.println("====向test表插入一行数据测试====");
        String rowKey = "1002";String columnFam = "personal_info";String  column = "age";String value = "22";
        addOneRowData(tableName,rowKey,columnFam,column,value);*/

/*        System.out.println("====获取"+tableName+"表的所有值方法测试====");
        getAllRowsInfo(tableName);*/

/*        System.out.println("====获取"+tableName+"表的一行数据方法测试====");
        getOneRowInfo(tableName,"1002");*/

/*        System.out.println("====获取某列族：列数据====");
        getRowQualifier(tableName,"1001","personal_info","name");*/
    }

}