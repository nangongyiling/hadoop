package com.zp.test.hbase;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;

public class HBase {

	private static ApplicationContext ctx;
	
	public static void main(String args[]) {
		ctx = new ClassPathXmlApplicationContext("classpath:/conf/applicationContext.xml");
		HbaseTemplate template = (HbaseTemplate)ctx.getBean("hbaseTemplate");
		hbaseGet(template);
		hbaseScan(template);
	}
	
	private static void hbaseScan(HbaseTemplate template) {
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("cf1"));
		scan.addFamily(Bytes.toBytes("cf2"));
		List<Map<String,String>> list = template.find("hbase_01", scan, new RowMapper<Map<String,String>>() {

			@Override
			public Map<String, String> mapRow(Result result, int rowNum) throws Exception {
				List<Cell> celist = result.listCells();
				Map<String,String> map = new HashMap<String,String>();
				if(celist!=null &&!celist.isEmpty()) {
					for(Cell cell:celist) {
						String rowkey = Bytes.toString(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength());
						map.put("RowKey", rowkey);
						String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
						String family = Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
						String quali = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
						map.put(family+":"+quali, value);
					}
				}
				return map;
			}
		
		});
		
		for(int i=0;i<list.size();i++) {
			Map<String,String> map = list.get(i);
			Iterator<Entry<String,String>> iter = map.entrySet().iterator();
			while(iter.hasNext()) {
				Entry<String,String> entry = iter.next();
				StringBuffer sb = new StringBuffer();
				sb.append(entry.getKey()+"=").append(entry.getValue()+",");
				System.out.println(sb.toString());
			}
		}
	}
	
	private static void hbaseGet(HbaseTemplate template) {
		List<String> rows = template.find("hbase_01", "cf1", new RowMapper<String>() {

			@Override
			public String mapRow(Result result, int rowNum) throws Exception {
				return Bytes.toString(result.value());
			}
			
		});
		for(String s:rows) {
			System.out.println(s);
		}
	}
}
