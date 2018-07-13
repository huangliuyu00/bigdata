package tohbase.impl;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import com.alibaba.fastjson.JSONObject;

/**
 * oracle 数据导入到 hbase中
 * 
 */
public class OracleToHbaseSale {
	public static final String SPLIT = "~";
	public static final String COLSNAME = "s";

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();// 获取配置信息
		InputStream in = OracleToHbaseSale.class.getResourceAsStream("../conf/db.properties");
		Properties properties = new Properties();
		properties.load(in);
		String diverClass = properties.getProperty("driver.class");
		String url = properties.getProperty("driver.url");
		String userName = properties.getProperty("driver.username");
		String passWord = properties.getProperty("driver.password");
		// oracle驱动信息
		DBConfiguration.configureDB(conf, diverClass,
				url, userName, passWord);
		Job job = Job.getInstance(conf, "OracleToHbaseSale");
		job.setJarByClass(OracleToHbaseSale.class);
		job.setInputFormatClass(DBInputFormat.class);// 输入格式
		String[] fields = { "id", "ypbm", "uploadtime", "scph", "corpid", "xssl", "dw", "ghdw","corpid_zw", "ghdw_zw" };// 从oracle中读取所需要的字段
		DBInputFormat.setInput(job, XsTable.class, "tableName", null, null, fields);
		job.setMapperClass(DBInputMapper.class);// 设置map，class
		job.setMapOutputKeyClass(Text.class);// 设置map的输出的key的类型
		job.setMapOutputValueClass(Text.class);// 设置map的输出value的类型
		TableMapReduceUtil.initTableReducerJob("h_medi_sale", HBaseToHBaseReducer.class, job);// reduce的设置
		
		boolean success = job.waitForCompletion(true);
		
		System.exit(success ? 0 : 1);
	}

	/**
	 * 对应oracle的tableName
	 * 
	 */
	public static class XsTable implements DBWritable {
		private String id;
		private String ypbm;
		private Date uploadtime;
		private String scph;
		private String corpid;
		private Double xssl;
		private String dw;
		private String ghdw;
		private String corpid_zw;
		private String ghdw_zw;
		
		@Override
		public void readFields(ResultSet result) throws SQLException {
			id = result.getString("id");
			ypbm = result.getString("ypbm");
			uploadtime = result.getDate("uploadtime");
			scph = result.getString("scph");
			corpid = result.getString("corpid");
			xssl = result.getDouble("xssl");
			dw = result.getString("dw");
			ghdw = result.getString("ghdw");
			corpid_zw = result.getString("corpid_zw");
			ghdw_zw = result.getString("ghdw_zw");
		}

		@Override
		public void write(PreparedStatement stmt) throws SQLException {
			stmt.setString(1, id);
			stmt.setString(2, ypbm);
			stmt.setDate(3, uploadtime);
			stmt.setString(4, scph);
			stmt.setString(5, corpid);
			stmt.setDouble(6, xssl);
			stmt.setString(7, dw);
			stmt.setString(8, ghdw);
			stmt.setString(9, corpid_zw);
			stmt.setString(10, ghdw_zw);
		}
	}

	/**
	 * mapper
	 * 
	 */
	public static class DBInputMapper extends Mapper<LongWritable, XsTable, Text, Text> {
		@Override
		protected void map(LongWritable key, XsTable value, Mapper<LongWritable, XsTable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String yuefen = null;
			Integer tian = null;
			String time = null;
			String ypbm = value.ypbm;
			Date uploadtime = value.uploadtime;
			String scph = value.scph;
			String corpid = value.corpid;
			Double sl = value.xssl;
			String dw = value.dw;
			String ghdw = value.ghdw;
			String corpid_zw = value.corpid_zw;
			String ghdw_zw = value.ghdw_zw;
			if (StringUtils.isBlank(ypbm)) {
				ypbm = "isnull";
			}

			if (StringUtils.isBlank(scph)) {
				scph = "isnull";
			}
			if (StringUtils.isBlank(corpid)) {
				corpid = "isnull";
			}
			if (sl == null) {
				sl = 0d;
			}
			if (StringUtils.isBlank(dw)) {
				dw = "isnull";
			}
			if (StringUtils.isBlank(ghdw)) {
				ghdw = "isnull";
			}
			if (StringUtils.isBlank(corpid_zw)) {
				corpid_zw = StringUtils.isBlank(corpid) ? "isnull" : corpid;
			}
			if (StringUtils.isBlank(ghdw_zw)) {
				ghdw_zw = StringUtils.isBlank(ghdw) ? "isnull" : ghdw;
			}

			if (uploadtime != null) {
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
				time = dateFormat.format(value.uploadtime).replace("-", "");
				yuefen = time.substring(0, 6);
				tian = Integer.parseInt(time.substring(6, time.length()));

				StringBuffer newRowkey = new StringBuffer();

				// map的key的设置 ： ypbm~uploadtime[yyyyMM]~scph~uploadtime[dd]
				newRowkey.append(ypbm).append(SPLIT).append(yuefen).append(SPLIT).append(scph).append(SPLIT)
						.append(tian);
				Col col = new Col(value.id,ypbm,scph, corpid, Double.toString(sl), dw, ghdw, corpid_zw, ghdw_zw);// map的value设置
				Text k = new Text(newRowkey.toString());
				Text v = new Text(JSONObject.toJSONString(col));
				context.write(k, v);
			}

		}
	}

	/**
	 * reducer
	 * 
	 */
	public static class HBaseToHBaseReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

		@Override
		protected void reduce(Text key, Iterable<Text> value,
				Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws IOException,
				InterruptedException {

			String strVey = key.toString();
			// ypbm~uploadtime[yyyyMM]~scph 作为rowkey
			String rowkey = StringUtils.substringBeforeLast(strVey, SPLIT);
			// uploadtime[dd] 作为列名
			String colName = StringUtils.substringAfterLast(strVey, SPLIT);
			byte[] bytesRowkey = Bytes.toBytes(rowkey);
			List<Col> list = new ArrayList<Col>();
			Put put = new Put(bytesRowkey);
			Iterator<Text> iterator = value.iterator();
			while (iterator.hasNext()) {
				Col col = JSONObject.parseObject(iterator.next().toString(), Col.class);
				list.add(col);
			}
			put.add(Bytes.toBytes(COLSNAME), Bytes.toBytes(colName), Bytes.toBytes(JSONObject.toJSONString(list)));
			context.write(new ImmutableBytesWritable(bytesRowkey), put);
		}
	}

}
