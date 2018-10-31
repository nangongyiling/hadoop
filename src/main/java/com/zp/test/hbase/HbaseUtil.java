package com.zp.test.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseUtil {

	private Configuration conf;
	
	public HbaseUtil() {
		conf = HBaseConfiguration.create();
		conf.addResource("classpath:/conf/hbase-site.xml");
		conf.set("hbase.zookeeper.quorum", "test.mysql.com:2181");
	}
	
	public boolean existTable(String tableName) throws IOException {
		TableName tblName = TableName.valueOf(Bytes.toBytes(tableName));
		Admin admin = ConnectionFactory.createConnection(conf).getAdmin();
		if(admin.tableExists(tblName)) {
			return true;
		}
		return false;
	}
	
	public boolean createTable(String tableName) {
		TableName tblName = TableName.valueOf(Bytes.toBytes(tableName));
		HTableDescriptor desc = new HTableDescriptor(tblName);
		desc.addFamily(new HColumnDescriptor(Bytes.toBytes("cf1")));
		desc.addFamily(new HColumnDescriptor(Bytes.toBytes("cf2")));
		
		Connection conn;
		try {
			conn = ConnectionFactory.createConnection(conf);
			conn.getAdmin().createTable(desc);
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	public static void main(String[] args) throws IOException {
		HbaseUtil util = new HbaseUtil();
		if(util.createTable("hbase_01")) {
			System.out.println("============== create ok");
		}else {
			System.out.println("============== create failed");
		}
		
		if(util.existTable("hbase_01")) {
			System.out.println("============== exists");
		}else {
			System.out.println("============== not exists");
		}
	}
}
