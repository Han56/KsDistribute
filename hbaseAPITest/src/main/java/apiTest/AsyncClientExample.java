package apiTest;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

/**
 * @author han56
 * @description 功能描述：异步客户端的实现demo
 * @create 2022/4/26 下午2:27
 */
public class AsyncClientExample extends Configured implements Tool {

    //设置日志
    private static final Logger LOG = LoggerFactory.getLogger(AsyncClientExample.class);

    //设置线程池大小
    private static final int THREAD_POOL_SIZE = 16;

    //设置默认操作数
    private static final int DEFAULT_NUM_OPS = 100;

    //设置列族
    private static final byte[] FAMILY = Bytes.toBytes("info");

    //设置列
    private static final byte[] QUAL = Bytes.toBytes("age");

    private final AtomicReference<CompletableFuture<AsyncConnection>> future = new AtomicReference<>();

    //将预定义连接配置进行加载
    public static Configuration configuration;
    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop101,hadoop102,hadoop103");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
    }

    //异步建立连接
    private CompletableFuture<AsyncConnection> getAsyncConn(){
        CompletableFuture<AsyncConnection> f = future.get();
        if(f!=null)
            return f;
        for (;;){
            if(future.compareAndSet(null,new CompletableFuture<>())){
                CompletableFuture<AsyncConnection> toComplete = future.get();
                addListener(ConnectionFactory.createAsyncConnection(configuration),(conn,error)->{
                    if (error!=null){
                        toComplete.completeExceptionally(error);
                        //重置future 防止与下一次连接发生混乱
                        future.set(null);
                        return;
                    }
                    toComplete.complete(conn);
                });
                return toComplete;
            }else {
                f = future.get();
                if (f!=null)
                    return f;
            }
        }
    }

    //关闭异步连接
    private CompletableFuture<Void> closeConn(){
        CompletableFuture<AsyncConnection> f = future.get();
        if (f==null)
            return CompletableFuture.completedFuture(null);
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        addListener(f,(conn,error)->{
            if (error==null){
                IOUtils.closeQuietly(conn);
                LOG.warn("关闭异步连接失败");
            }
            closeFuture.complete(null);
        });
        return closeFuture;
    }

    private byte[] getKey(int i){
        return Bytes.toBytes(String.format("%08x",i));
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 1 || args.length > 2) {
            System.out.println("Usage: " + this.getClass().getName() + " tableName [num_operations]");
            return -1;
        }

        TableName tableName = TableName.valueOf(args[0]);
        int numOps = args.length>1 ? Integer.parseInt(args[1]):DEFAULT_NUM_OPS;
        ExecutorService threadPool =  Executors.newFixedThreadPool(THREAD_POOL_SIZE,
                new ThreadFactoryBuilder().setNameFormat("AsyncClientExample-pool-%d").setDaemon(true)
                        .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
        //我们将使用 线程池+AsyncTable 这个方法对Hbase Table进行操作；使用RawAsyncTable则不需要线程池
        // RawAsyncTable的使用要小心，因为使用的得当性能会很出色，如果使用的不好 甚至会非常糟糕

        CountDownLatch latch = new CountDownLatch(numOps);
        IntStream.range(0,numOps).forEach(i->{
            CompletableFuture<AsyncConnection> future = getAsyncConn();
            addListener(future,(conn,error)->{
                if(error!=null){
                    LOG.warn("异步连接失败："+i,error);
                    latch.countDown();
                    return;
                }
                AsyncTable<?> table = conn.getTable(tableName,threadPool);
                addListener(table.put(new Put(getKey(i)).addColumn(FAMILY,QUAL,Bytes.toBytes(i))),
                        (putResp,putErr)->{
                            if (putErr!=null){
                                LOG.warn("存储失败："+i,putErr);
                                latch.countDown();
                                return;
                            }
                            LOG.warn("异步存储成功，接下来进行异步查询测试");
                            addListener(table.get(new Get(getKey(i))),(result,getErr)->{
                                if (getErr!=null){
                                    LOG.warn("异步查询失败："+i,getErr);
                                    latch.countDown();
                                    return;
                                }
                                if (result.isEmpty()){
                                    LOG.warn("获取数据失败，服务器返回空结果集");
                                }else if (!result.containsColumn(FAMILY,QUAL)){
                                    LOG.warn("获取数据失败，因为HBase中不存在这个表："+Bytes.toString(FAMILY)+":"+Bytes.toString(QUAL));
                                }else {
                                    int v = Bytes.toInt(result.getValue(FAMILY,QUAL));
                                    if(v!=i){
                                        LOG.warn("获取数据失败："+i+"与对应列中的数据不一致："+Bytes.toString(FAMILY) +
                                                ":" + Bytes.toString(QUAL));
                                    }else {
                                        LOG.info("获取数据成功："+i);
                                    }
                                }
                                latch.countDown();
                            });
                        });
            });
        });
        latch.await();
        closeConn().get();
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new AsyncClientExample(), new String[]{"student"});
    }


}
