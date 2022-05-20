package writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author han56
 * @description 功能描述【流量统计的Bean对象】
 * @create 2021/10/20 下午8:03
 */
//继承Writable接口
public class FlowBean implements Writable {

    /*
    * 1.上行流量  2.下行流量   3.总流量
    * */
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    /*
    * 七步骤：提供无参构造方法
    * */
    public FlowBean(){
        super();
    }

    /*
    * getter setter
    * */

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow+this.downFlow;
    }

    /*
    * 七步骤：实现序列化反序列化方法，注意顺序必须一致
    * */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow  = in.readLong();
        this.sumFlow = in.readLong();
    }

    /*
    * 七步骤：重写 toString() 方法
    * */
    @Override
    public String toString(){
        return upFlow+"\t"+downFlow+"\t"+sumFlow;
    }
}
