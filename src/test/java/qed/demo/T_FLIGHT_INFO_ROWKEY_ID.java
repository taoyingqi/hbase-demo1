package qed.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Before;
import org.junit.Test;
import qed.demo.util.TimeUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static qed.demo.util.TimeUtil.DATE_TIME_MILLIS_TYPE;

/**
 * Created by MT-T450 on 2017/6/22.
 */
public class T_FLIGHT_INFO_ROWKEY_ID {
    public Configuration configuration;
    private String tableName = "T_FLIGHT_INFO";

    @Before
    public void before() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.master", "10.0.110.121:16000");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "ambari02,ambari03,ambari01");
        configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
        configuration.set("hbase.client.pause", "30");
        configuration.set("hbase.client.retries.number", "3");
        configuration.set("hbase.rpc.timeout", "2000");
        configuration.set("hbase.client.operation.timeout", "3000");
        configuration.set("hbase.client.scanner.timeout.period", "10000");
    }

    /**
     * 查询所有数据
     */
    @Test
    public void QueryAll() {
//        HTablePool pool = new HTablePool(configuration, 1000);
//        HTable table = (HTable) pool.getTable(tableName);
        HTable table = null;
        try {
            table = new HTable(configuration, tableName);
            Filter filter = new PageFilter(40);
            Scan scan = new Scan();
            scan.setFilter(filter);
            scan.setMaxVersions();
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                System.out.println("-->>获得到rowkey:" + new String(r.getRow()));
                /*for (KeyValue kv : r.raw()) {
                    System.out.println(
                            "列：" + new String(kv.getFamily()) + ":" + new String(kv.getQualifier())
                            + ", 值：" + new String(kv.getValue())
                            + ", 时间戳：" + TimeUtil.formatDate(kv.getTimestamp(), DATE_TIME_MILLIS_TYPE)
                    );
                }*/
            }
            Configuration HBASE_CONFIG = new Configuration();
            HBASE_CONFIG.set("hbase.zookeeper.quorum", "192.168.1.1");
            HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据 rowKey 获取
     */
    @Test
    public void get() {
        HTable table = null;
        try {
            table = new HTable(configuration, tableName);
            Get get = new Get(Bytes.toBytes("0000346e921042b9b3b09f9f0781e45b"));
            get.setMaxVersions(10);
            get.addColumn(Bytes.toBytes("CF"), Bytes.toBytes("ABSTAT"));
            Result result = table.get(get);
            List<KeyValue> list = result.list();
            for(final KeyValue kv:list){
                // System.out.println("value: "+ kv+ " str: "+Bytes.toString(kv.getValue()));
                System.out.println(String.format("family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.",
                        Bytes.toString(kv.getFamily()),
                        Bytes.toString(kv.getQualifier()),
                        Bytes.toString(kv.getValue()),
                        TimeUtil.formatDate(kv.getTimestamp(), DATE_TIME_MILLIS_TYPE)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void selectByRowKeyPrefix() {
        String key = "bfc908d78f024882916df2a389ced391";
        try {
            HTable table = new HTable(configuration, tableName);
            Scan scan = new Scan();
//            Filter prefixFilter = new PrefixFilter(Bytes.toBytes(key));
//            scan.setFilter(prefixFilter);
            /*List<byte[]> list = new ArrayList();
            list.add(new Bytes.toBytes(key));
            Filter filter = new FuzzyRowFilter(Arrays.asList(
                    new Pair<byte[], byte[]>(
                            Bytes.toBytesBinary(key),
                            new byte[] {1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0})));*/
            scan.setRowPrefixFilter(Bytes.toBytes(key));
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                System.out.println("-->>获得到rowkey:" + new String(r.getRow()));
                /*for (KeyValue kv : r.raw()) {
                    System.out.println(
                            "列：" + new String(kv.getFamily()) + ":" + new String(kv.getQualifier())
                                    + ", 值：" + new String(kv.getValue())
                                    + ", 时间戳：" + TimeUtil.formatDate(kv.getTimestamp(), DATE_TIME_MILLIS_TYPE)
                    );
                }*/
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void selectByRowFilter() {
        try {
            HTable table = new HTable(configuration, tableName);
            Scan scan = new Scan();
            Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("_20170514"));
            scan.setFilter(filter);
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                System.out.println("-->>获得到rowkey:" + new String(r.getRow()));
                for (KeyValue kv : r.raw()) {
                    System.out.println(
                            "列：" + new String(kv.getFamily()) + ":" + new String(kv.getQualifier())
                                    + ", 值：" + new String(kv.getValue())
                                    + ", 时间戳：" + TimeUtil.formatDate(kv.getTimestamp(), DATE_TIME_MILLIS_TYPE)
                    );
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}