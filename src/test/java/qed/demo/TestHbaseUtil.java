package qed.demo;

import org.apache.hadoop.hbase.TableName;
import org.junit.Test;
import qed.demo.util.HbaseUtil;

import java.io.IOException;

/**
 * Created by MT-T450 on 2017/6/20.
 */
public class TestHbaseUtil {

    @Test
    public void testList() throws IOException {
        TableName[] tableNames = HbaseUtil.getInstance().list();
        for (TableName name : tableNames) {
            System.out.println(name);
        }
    }


}
