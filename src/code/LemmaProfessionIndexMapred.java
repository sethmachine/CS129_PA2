package code;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import utils.StringIntegerList;
import utils.StringIntegerList.StringInteger;
import utils.StringDoubleList;
//import utils.KeyValueParser;

import java.lang.Math;

/**
 * This class is used for Section A of assignment 2.
 * 
 */
public class LemmaProfessionIndexMapred 
{
	//@formatter:off
	/**
	 * Input:
	 * 		articleId 	lemma frequencies
	 * Output
	 * 		profession	lemma likelihoods
	 * @author Seth-David Donald Dworman
	 *
	 */
	//@formatter:on
	public final static String PROFESSION_TRAIN_PATH = "/user/hadoop19/input/profession_train.txt";
	public final static String PROFESSION_PRIOR_PATH = "/user/hadoop19/pa2/A/profession_prior.txt";
	public static HashMap<String, ArrayList<String>> professionTrain = new HashMap<String, ArrayList<String>>();
	public static HashMap<String, Integer> professionCount = new HashMap<String, Integer>();
	public static int totalArticles = 0; //total articles in training set
	public static BufferedWriter bw;
	private static final int NAME = 0;
	private static final int PROFESSIONS = 1;
	public static class InvertedIndexMapper extends Mapper<Text, Text, Text, StringIntegerList> 
	{
		@Override
		protected void setup(Mapper<Text, Text, Text, StringIntegerList>.Context context) throws IOException, InterruptedException 
		{
			FileSystem fs = FileSystem.get(context.getConfiguration());
			URI u = context.getCacheFiles()[0];
			Path p = new Path(u.getPath());
			//Path pp = new Path(PROFESSION_PRIOR_PATH);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
			//bw = new BufferedWriter(new OutputStreamWriter(fs.create(pp)));
			//KeyValueParser keyValueParser = new KeyValueParser();
			String line;
			while ((line = br.readLine()) != null) 
			{
				totalArticles += 1;
				String[] nameProfessions = line.split(" : ");
				ArrayList<String> professions = new ArrayList<String>();
				for(String profession: nameProfessions[PROFESSIONS].split(", "))
				{
					if (professionCount.get(profession) == null)
					{
						professionCount.put(profession, 1);
					}
					else
					{
						professionCount.put(profession, professionCount.get(profession) + 1);
					}
					professions.add(profession);
				}
				professionTrain.put(java.net.URLDecoder.decode(nameProfessions[NAME], "UTF-8"), professions);
			}
			br.close();
			/*for(String profession: professionCount.keySet())
			{
				bw.write(Math.log(professionCount.get(profession) / totalArticles) + "\n");
			}
			bw.close();*/
		}
		
		@Override
		public void map(Text articleId, Text indices, Context context) 
		throws IOException, InterruptedException 
		{
			StringIntegerList lemmaIndices = new StringIntegerList();
			lemmaIndices.readFromString(indices.toString());
			ArrayList<String> professions = professionTrain.get(articleId.toString());
			if (professions != null)
			{
				for (String profession: professions)
				{
					context.write(new Text(profession), lemmaIndices);
				}
			}
		}
	}

	public static class InvertedIndexReducer extends Reducer<Text, StringIntegerList, Text, StringDoubleList> 
	{
		@Override
		public void reduce(Text profession, Iterable<StringIntegerList> lemmaIndices, Context context)
		throws IOException, InterruptedException 
		{
			Iterator<StringIntegerList> iter = lemmaIndices.iterator();
			HashMap<String, Double> dict = new HashMap<String, Double>();
			HashMap<String, Integer> lemmaCounts = new HashMap<String, Integer>();
			double total = 0.0;
			while(iter.hasNext())
			{
				total += 1;
				StringIntegerList curr = iter.next();
				//System.out.println(curr.toString()); //values are correct up to here...
				List<StringInteger> lemmas2 = curr.getIndices();
				ArrayList<String> arr = new ArrayList<String>();
				for(StringInteger si: lemmas2)
				{
					arr.add(si.getString());
				}
				for(String s: arr)
				{
					if (lemmaCounts.get(s) == null)
					{
						lemmaCounts.put(s, 1);
					}
					else
					{
						int oldCounts = lemmaCounts.get(s);
						lemmaCounts.put(s,  oldCounts + 1);
					}
				}
			}
			//System.out.println("total articles for " + profession.toString() + " : " + totalArticles);
			for(String s: lemmaCounts.keySet())
			{
				//System.out.println("total times for " + s + " : " + lemmaCounts.get(s));
				dict.put(s, Math.log(lemmaCounts.get(s) / total)); //log probabilities
			}
			StringDoubleList invertedIndices = new StringDoubleList(dict);
			context.write(profession, invertedIndices);
			//bw.write(Math.log(professionCount.get(profession) / totalArticles) + "\n");
		}
	}

	public static void main(String[] args) throws Exception
	{
		Job job1 = Job.getInstance(new Configuration());
		job1.getConfiguration().set("mapreduce.job.queuename", "hadoop19");
		job1.setJobName("job1");
		job1.addCacheFile(new URI(PROFESSION_TRAIN_PATH));
		job1.setJarByClass(LemmaProfessionIndexMapred.class);
		job1.setMapperClass(InvertedIndexMapper.class);
		job1.setReducerClass(InvertedIndexReducer.class);
		job1.setInputFormatClass(KeyValueTextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(StringIntegerList.class);
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		//job1.submit();
		job1.waitForCompletion(true);
	}
}
