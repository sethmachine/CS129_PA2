package code;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import utils.StringIntegerList;

/**
 * This extracts all article lemma indices whose articleID
 * exists in the list of all ARTICLE_LEMMA_INDEX built in PA1 part B.
 * 
 */
public class GetArticlesLemmaMapred 
{
	//@formatter:off
	/**
	 * Input:
	 * 		articleId 	lemma frequencies
	 * Output
	 * 		lemma 			inverted lemma frequencies
	 * @author Seth-David Donald Dworman
	 *
	 */
	//@formatter:on
	public final static String PROFESSION_TRAIN_PATH = "/user/hadoop19/input/profession_train.txt";
	public static int totalArticles = 0;
	public static class InvertedIndexMapper extends Mapper<Text, Text, Text, StringIntegerList> 
	{
		private static ArrayList<String> articles = new ArrayList<String>();
		@Override
		protected void setup(Mapper<Text, Text, Text, StringIntegerList>.Context context) throws IOException, InterruptedException 
		{
			FileSystem fs = FileSystem.get(context.getConfiguration());
			URI u = context.getCacheFiles()[0];
			Path p = new Path(u.getPath());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
			String line;
			while ((line = br.readLine()) != null) 
			{
				totalArticles += 1;
   			articles.add(java.net.URLDecoder.decode(line, "UTF-8"));
			}
			br.close();
			System.out.println("Total articles: " + totalArticles);
		}
		
		@Override
		public void map(Text articleId, Text indices, Context context) 
		throws IOException, InterruptedException 
		{
			String article = java.net.URLDecoder.decode(articleId.toString(), "UTF-8");
			if(articles.contains(article))
			{
				StringIntegerList lemmaIndices = new StringIntegerList();
				lemmaIndices.readFromString(indices.toString());
				context.write(articleId, lemmaIndices);
			}
		}
	}

	public static void main(String[] args) throws Exception
	{
		Job job1 = Job.getInstance(new Configuration());
		job1.getConfiguration().set("mapreduce.job.queuename", "hadoop19");
		job1.setJobName("job1");
		job1.addCacheFile(new URI(PROFESSION_TRAIN_PATH));
		job1.setJarByClass(GetArticlesLemmaMapred.class);
		job1.setMapperClass(InvertedIndexMapper.class);
		job1.setInputFormatClass(KeyValueTextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(StringIntegerList.class);
		//input should be /user/hadoop19/pa1/B/part-r-00000
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.submit();
	}
}
