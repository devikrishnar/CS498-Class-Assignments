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

public class TablePartF{

   public static void main(String[] args) throws IOException {

	// TODO      
	// DON' CHANGE THE 'System.out.println(xxx)' OUTPUT PART
	// OR YOU WON'T RECEIVE POINTS FROM THE GRADER      

	Configuration config = HBaseConfiguration.create();

	// Instantiating HTable class
	HTable table = new HTable(config, "powers");

	// Instantiating the Scan class
	Scan scan = new Scan();

	// Scanning the required columns
	scan.addColumn(Bytes.toBytes("custom"), Bytes.toBytes("color"));
	scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("hero"));
	scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("power"));
	scan.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("name"));
	scan.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("xp"));

	// Getting the scan result
	ResultScanner scanner1 = table.getScanner(scan);

	for (Result result1 = scanner1.next(); result1 != null; result1 = scanner1.next()){
	     	String color1 = Bytes.toString(result1.getValue(Bytes.toBytes("custom"), Bytes.toBytes("color")));
		String name1 = Bytes.toString(result1.getValue(Bytes.toBytes("professional"), Bytes.toBytes("name")));
		ResultScanner scanner2 = table.getScanner(scan);
		for (Result result2 = scanner2.next(); result2 != null; result2 = scanner2.next()){
			String color2 = Bytes.toString(result2.getValue(Bytes.toBytes("custom"), Bytes.toBytes("color")));
			String name2 = Bytes.toString(result2.getValue(Bytes.toBytes("professional"), Bytes.toBytes("name")));
			if (color1.equals(color2) && !name1.equals(name2)) {
				String power1 = Bytes.toString(result1.getValue(Bytes.toBytes("personal"), Bytes.toBytes("power")));
				String power2 = Bytes.toString(result2.getValue(Bytes.toBytes("personal"), Bytes.toBytes("power")));
				System.out.println(name1 + ", " + power1 + ", " + name2 + ", " + power2 + ", "+color1);
			}
		}
	}
   }
}
