package id2221.topten;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

public class TopTen {
	// This helper function parses the stackoverflow into a Map for us.
	public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
			for (int i = 0; i < tokens.length - 1; i += 2) {
				String key = tokens[i].trim();
				String val = tokens[i + 1];
				map.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException e) {
			System.err.println(xml);
		}

		return map;
	}

	public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		// Stores a map of user reputation to the record
		TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Map<String, String> user = TopTen.transformXmlToMap(value.toString()); // Use helper class to parse and map
																					// users.xml

			String id = user.get("Id");
			String rep = user.get("Reputation");

			// Skip rows that do not contain relevant user data
			if (id == null || rep == null) {
				return;
			}

			// Add the valid record to the TreeMap
			repToRecordMap.put(Integer.parseInt(rep), new Text(value));
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Output our ten records to the reducers with a null key
			try {
				for (int i = 0; i < 10; i++) {
					// Remove and return key-value pair with greatest key (reputation)
					Map.Entry<Integer, Text> entry = repToRecordMap.pollLastEntry();
					context.write(NullWritable.get(), new Text(entry.getValue())); // Overwrite key with null
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
		// Stores a map of user reputation to the record
		private TreeMap<Text, Text> repToRecordMap = new TreeMap<Text, Text>();

		public void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values) {
				Map<String, String> user = TopTen.transformXmlToMap(value.toString());
				String rep = user.get("Reputation");
				String id = user.get("Id");

				// Add this record to our map with the reputation as the key
				repToRecordMap.put(new Text(rep), new Text(id));

				// Prune TreeMap if it happens to have more than 10 records, removes user with
				// least rep
				if (repToRecordMap.size() > 10) {
					// Remove first element since its sorted by reputation (integer) in ascending
					// order
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}

			/*
			 * Once all the values have been iterated over, the values of Id and Reputation
			 * contained in the TreeMap should be stored in the table topten in HBase. Store
			 * into the columns info:rep and info:id.
			 */
			try {
				int i = 0;
				for (Map.Entry<Text, Text> entry : repToRecordMap.descendingMap().entrySet()) {
					// Create put operation to add record to HBase
					Put putHBase = new Put(Bytes.toBytes(Integer.toString(i)));
					putHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"),
							Bytes.toBytes(entry.getKey().toString()));
					putHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"),
							Bytes.toBytes(entry.getValue().toString()));

					// write data to HBase table
					context.write(null, putHBase);
					i++;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TopTen");
		job.setJarByClass(TopTen.class);

		job.setMapperClass(TopTenMapper.class);
		job.setReducerClass(TopTenReducer.class);

		job.setNumReduceTasks(1); // Multiple reducers would shard the data, resulting in multiple top ten lists

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job); // start job

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
