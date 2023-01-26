import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;

public class TablePartB{

   public static void main(String[] args) throws IOException {

	Configuration conf = HBaseConfiguration.create();
	HBaseAdmin admin = new HBaseAdmin(conf);
   	HTableDescriptor[] tableDescriptor =  admin.listTables();
	//TODO
	for(int i=0; i < tableDescriptor.length;i++){
         System.out.println(tableDescriptor[i].getNameAsString());
     }
   }
}

