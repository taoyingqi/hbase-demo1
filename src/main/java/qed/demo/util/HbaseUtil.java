package qed.demo.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by MT-T450 on 2017/6/20.
 */
public class HbaseUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseUtil.class);
    private Configuration conf = null;
    private Connection conn = null;
    private static HbaseUtil instance = null;

    private HbaseUtil(){
        init();
    }
    public void init(){
        try{
            conf =  HBaseConfiguration.create();
            conf.set("hbase.master", "10.0.110.121:16000");
            conf.set("hbase.zookeeper.quorum", "ambari01,ambari02,ambari03");
            conf.set("zookeeper.znode.parent", "/hbase-unsecure");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.client.pause", "30");
            conf.set("hbase.client.retries.number", "3");
            conf.set("hbase.rpc.timeout", "2000");
            conf.set("hbase.client.operation.timeout", "3000");
            conf.set("hbase.client.scanner.timeout.period", "10000");

            conn = ConnectionFactory.createConnection(conf);
        }catch(Exception e){
            LOGGER.error("初始化hbase连接失败"+e.getMessage(),e);
        }
    }

    public static HbaseUtil getInstance(){
        if(instance == null){
            synchronized (HbaseUtil.class) {
                if(instance == null){
                    instance = new HbaseUtil();
                }
            }
        }
        return instance;
    }

    public TableName[] list() throws IOException {
        return conn.getAdmin().listTableNames();
    }

    /**
     * 获取htable操作类
     * @param tableName
     * @return
     * @throws IOException
     */
    public Table getHtable(String tableName) throws IOException {
        return conn.getTable(TableName.valueOf(tableName));
    }

    /**
     *
     * @param table
     */
    public void relaseHtable(Table table){
        if(table == null){
            return;
        }
        try {
            table.close();
        } catch (IOException e) {
            LOGGER.error(e.getMessage(),e);
        }
    }

    /**
     * 关闭hbase连接
     */
    public void destory(){
        try {
            conn.close();
            instance = null;
        } catch (IOException e) {
            LOGGER.error(e.getMessage(),e);
        }
    }


}
