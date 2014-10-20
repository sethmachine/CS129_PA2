package utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;

import utils.KeyValueParser;

public class Eval
{
	public static HashMap<String, ArrayList<String>> getKeyValueMap(String path, String keySeparator, String valueSeparator) throws IOException
	{
		HashMap<String, ArrayList<String>> keyValueMap = new HashMap<String, ArrayList<String>>();
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line;
		KeyValueParser parser = new KeyValueParser(keySeparator, valueSeparator);
		while ((line = br.readLine()) != null) 
		{
			keyValueMap.putAll(parser.getKeyValueMap(line));
		}
		br.close();
		return keyValueMap;
	}
	public static void main(String[] args) throws IOException
	{
		HashMap<String, ArrayList<String>> testMap = getKeyValueMap(args[0], " :\t", ", ");
		HashMap<String, ArrayList<String>> goldMap = getKeyValueMap(args[1], " : ", ", ");
		ArrayList<String> missing = new ArrayList<String>();
		//System.out.println(testMap);
		//System.out.println(goldMap.keySet());
		double tp = 0.0;
		int total = 0;
		int unmatched = 0;
		for(String articleID: testMap.keySet())
		{
			ArrayList<String> testLabels = testMap.get(articleID);
			try
			{
				total += 1;
				for(String goldLabel: goldMap.get(articleID))
				{
					if (testLabels.contains(goldLabel))
					{
						tp += 1;
						break;
					}
				}
			}
			catch(NullPointerException e){missing.add(articleID); unmatched += 1; total -= 1;}
		}
		System.out.println("Precision: " + (tp / total));
		System.out.println("Unmatched articles: " + unmatched);
		//System.out.println(missing);
		System.out.println("Size of test set: " + testMap.keySet().size());
		System.out.println("Size of training set: " + goldMap.keySet().size());
	}

}
