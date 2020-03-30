package edu.comillas.mibd;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

public class Visualizador {

    public static void PrintResult(Result result) {
        String resultRowKey = Bytes.toString(result.getRow());
        if(!result.isEmpty()) {
            for (Cell kv : result.listCells()) {
                String cf = Bytes.toString(CellUtil.cloneFamily(kv));
                String qual = Bytes.toString(CellUtil.cloneQualifier(kv));
                String value = Bytes.toString(CellUtil.cloneValue(kv));
                Long ts = kv.getTimestamp();
                System.out.println(resultRowKey + ": " + "cf: " + cf + ", " + "qual: " + qual + ", " + "value: " + value + ", " + "ts: " + ts);
            }
        }
    }

    public static void PrintResult(ResultScanner result) {
        for(Result res : result)
        {
            PrintResult(res);
        }
        System.out.println("\n\n");
    }


}
