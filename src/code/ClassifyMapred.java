package code;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collections;

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
import utils.StringDoubleList.StringDouble;

/**
 * This class is used for Section B of assignment 2.
 * 
 */
public class ClassifyMapred
{
	//@formatter:off
	/**
	 * Input:
	 * 		articleId 	lemma frequencies
	 * Output
	 * 		articleId		professions
	 * @author Seth-David Donald Dworman
	 *
	 */
	//@formatter:on
	public final static String PROFESSION_LEMMA_INDEX_PATH = "/user/hadoop19/pa2/A/part-r-00000";
	public final static String PROFESSION_TEST_PATH = "/user/hadoop19/input/profession_test.txt";
	//used to get the prior
	//public final static String PROFESSION_PRIOR_PATH = "/user/hadoop19/input/profession_train.txt";
	//public static HashMap<String, Double> professionPrior = new HashMap<String, Double>();
	public static HashMap<String, StringDoubleList> professionLikelihood = new HashMap<String, StringDoubleList>();
	public static ArrayList<String> professionTest = new ArrayList<String>();
	private static final int KEY = 0;
	private static final int VALUE = 1;
	private static final int TOP_LABELS = 3; //take the 3 most likely class labels
	public static class InvertedIndexMapper extends Mapper<Text, Text, Text, StringDouble> 
	{
		@Override
		protected void setup(Mapper<Text, Text, Text, StringDouble>.Context context) throws IOException, InterruptedException 
		{
			FileSystem fs = FileSystem.get(context.getConfiguration());
			URI u = context.getCacheFiles()[0];
			Path p = new Path(u.getPath());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
			String line;
			while ((line = br.readLine()) != null) 
			{
				String[] keyValue = line.split("\t");
				StringDoubleList lemmaLikelihoods = new StringDoubleList();
				lemmaLikelihoods.readFromString(keyValue[VALUE]);
				professionLikelihood.put(keyValue[KEY], lemmaLikelihoods);
			}
			br.close();
			u = context.getCacheFiles()[1];
			p = new Path(u.getPath());
			br = new BufferedReader(new InputStreamReader(fs.open(p)));
			while((line = br.readLine()) != null)
			{
				professionTest.add(java.net.URLDecoder.decode(line, "UTF-8"));
			}
			br.close();
		}
		
		@Override
		public void map(Text articleId, Text indices, Context context) 
		throws IOException, InterruptedException 
		{
			String article = java.net.URLDecoder.decode(articleId.toString(), "UTF-8");
			if(professionTest.contains(article))
			{
			StringIntegerList lemmaIndices = new StringIntegerList();
			lemmaIndices.readFromString(indices.toString());
			List<StringInteger> lemmas = lemmaIndices.getIndices();
			ArrayList<String> articleLemmas = new ArrayList<String>();
			HashMap<String, Integer> lemmaFreqs = new HashMap<String, Integer>();
			for(StringInteger si: lemmas)
			{
				lemmaFreqs.put(si.getString(), si.getValue());
				articleLemmas.add(si.getString());
			}
			for(String profession: professionLikelihood.keySet())
			{
				double sum = 0.0;
				StringDoubleList lemmaLikelihood = professionLikelihood.get(profession);
				List<StringDouble> lemmas2 = lemmaLikelihood.getIndices();
				Set<String> keys = lemmaFreqs.keySet();
				for(StringDouble sd: lemmas2)
				{
					if(keys.contains(sd.getString()))
					{
						sum += lemmaFreqs.get(sd.getString()) * sd.getValue();
					}
					/*else //absence of a feature/word, not sure if necessary
					{
						sum += Math.log(1.0 - Math.exp(sd.getValue()));
					}*/
				}
				if(sum != 0.0)
				{
					context.write(articleId, new StringDouble(profession, sum));
				}
			}
			}
		}
	}

	public static class InvertedIndexReducer extends Reducer<Text, StringDouble, Text, Text> 
	{
		@Override
		public void reduce(Text articleId, Iterable<StringDouble> likelihood, Context context)
		throws IOException, InterruptedException 
		{
			Iterator<StringDouble> iter = likelihood.iterator();
			HashMap<String, Double> labelLikelihood = new HashMap<String, Double>();
			ArrayList<StringDouble> arrsd = new ArrayList<StringDouble>();
			int z = 0;
			while(iter.hasNext())
			{
				z += 1;
				StringDouble sd = iter.next();
				//System.out.println(sd);
				arrsd.add(sd);
				arrsd.add(new StringDouble(sd.getString(), sd.getValue()));
				labelLikelihood.put(sd.getString(), sd.getValue());
			}
			//Collections.sort(arrsd);
			System.out.println(labelLikelihood); //this is correct
			ArrayList<StringDouble> tester = new ArrayList<StringDouble>();
			for(String key: labelLikelihood.keySet())
			{
				tester.add(new StringDouble(key, labelLikelihood.get(key)));
			}
			Collections.sort(tester); //tester looks correct
			//System.out.println(arrsd); //this hasn't been correct :[
			String professions = "";
			for(int i = 0; i < TOP_LABELS; i++)
			{
				professions += tester.get(i).getString();
				if (i < TOP_LABELS - 1)
				{
					professions += ", ";
				}
			}
			context.write(new Text(articleId.toString() + " :"), new Text(professions));
			System.out.println("total professions: " + z + ", length of arrsd: " + arrsd.size());
		}
	}

	public static void main(String[] args) throws Exception
	{
		Job job1 = Job.getInstance(new Configuration());
		job1.getConfiguration().set("mapreduce.job.queuename", "hadoop19");
		job1.setJobName("job1");
		job1.addCacheFile(new URI(PROFESSION_LEMMA_INDEX_PATH));
		job1.addCacheFile(new URI(PROFESSION_TEST_PATH));
		//job1.addCacheFile(new URI(PROFESSION_PRIOR_PATH));
		job1.setJarByClass(ClassifyMapred.class);
		job1.setMapperClass(InvertedIndexMapper.class);
		job1.setReducerClass(InvertedIndexReducer.class);
		job1.setInputFormatClass(KeyValueTextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(StringDouble.class);
		//reads in the ArticleLemmaIndex from PA1 as input file
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.submit();
		//job1.waitForCompletion(true);
	}
}
