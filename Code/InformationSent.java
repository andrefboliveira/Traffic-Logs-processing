package pt.ulisboa.ciencias.di.cloud_computing;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InformationSent {

	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
		private Text sourceIP = new Text();
		private LongWritable bytesSent = new LongWritable();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (!(key.get() == 0 && value.toString()
					.equals("sIP,dIP,sPort,dPort,protocol,packets,bytes,flags,sTime,duration,eTime,sensor"))) {

				StringTokenizer st = new StringTokenizer(value.toString(), ",");

				sourceIP.set(st.nextToken());

				for (int i = 0; i < 5; i++) {
					st.nextToken();
				}

				bytesSent.set(Long.parseLong(st.nextToken()));
				context.write(sourceIP, bytesSent);
			}
		}
	}

	public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
		Long max = Long.MIN_VALUE;
		Text maxWord = new Text();

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			Long sum = Long.valueOf(0);
			for (LongWritable val : values) {
				sum += val.get();
			}

			// context.write(key, new LongWritable(sum));

			if (sum > max) {
				max = sum;
				maxWord.set(key);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(maxWord, new LongWritable(max));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Bytes sent");
		job.setJarByClass(InformationSent.class);

		job.setMapperClass(Map.class);

		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileUtils.deleteDirectory(new File(args[1]));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}