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
import org.apache.hadoop.hbase.client.Get;


import org.apache.hadoop.hbase.util.Bytes;

public class TablePartD{

   public static void main(String[] args) throws IOException {

	// TODO      
        // Instantiating Configuration class
	Configuration config = HBaseConfiguration.create();

	// Instantiating HTable class
	HTable table = new HTable(config, "powers");

	// Instantiating Get class
	Get g1 = new Get(Bytes.toBytes("row1"));

	// Reading the data
	Result result1 = table.get(g1);

	// Reading values from Result class object
	byte [] value1 = result1.getValue(Bytes.toBytes("personal"),Bytes.toBytes("hero"));
	byte [] value2 = result1.getValue(Bytes.toBytes("personal"),Bytes.toBytes("power"));
	byte [] value3 = result1.getValue(Bytes.toBytes("professional"),Bytes.toBytes("name"));
	byte [] value4 = result1.getValue(Bytes.toBytes("professional"),Bytes.toBytes("xp"));
	byte [] value5 = result1.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color"));

	// Printing the values
	
	String hero = Bytes.toString(value1);
	String power = Bytes.toString(value2);
	String name = Bytes.toString(value3);
	String xp = Bytes.toString(value4);
	String color = Bytes.toString(value5);
	System.out.println("hero: "+hero+", power: "+power+", name: "+name+", xp: "+xp+", color: "+color);

	Get g2 = new Get(Bytes.toBytes("row19"));

	// Reading the data
	Result result2 = table.get(g2);

	byte [] value11 = result2.getValue(Bytes.toBytes("personal"),Bytes.toBytes("hero"));
	byte [] value12 = result2.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color"));

	hero = Bytes.toString(value11);
	color = Bytes.toString(value12);
	System.out.println("hero: "+hero+", color: "+color);


	Get g3 = new Get(Bytes.toBytes("row1"));

	// Reading the data
	Result result3 = table.get(g3);

	byte [] value21 = result3.getValue(Bytes.toBytes("personal"),Bytes.toBytes("hero"));
	byte [] value22 = result3.getValue(Bytes.toBytes("professional"),Bytes.toBytes("name"));
	byte [] value23 = result3.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color"));

	hero = Bytes.toString(value21);
	name = Bytes.toString(value22);
	color = Bytes.toString(value23);
	System.out.println("hero: "+hero+", name: "+name+", color: "+color); 
   }
}

