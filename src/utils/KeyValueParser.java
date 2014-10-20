package utils;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;

public class KeyValueParser
{
	private static final int KEY = 0;
	private static final int VALUE = 1;
	private String keyValueSeparator;
	private String valueSeparator;
	
	public KeyValueParser()
	{
		keyValueSeparator = " : ";
		valueSeparator = ", ";
	}
	
	public KeyValueParser(String keyValueSeparator, String valueSeparator)
	{
		this.keyValueSeparator = keyValueSeparator;
		this.valueSeparator = valueSeparator;
	}
	
	public HashMap<String, ArrayList<String>> getKeyValueMap(String line) throws UnsupportedEncodingException
	{
		HashMap<String, ArrayList<String>> keyValueMap = new HashMap<String, ArrayList<String>>();
		ArrayList<String> values = new ArrayList<String>();
		String[] keyValueArr = line.split(keyValueSeparator);
		String keyStr = keyValueArr[KEY];
		String valueStr = keyValueArr[VALUE];
		for(String value: valueStr.split(valueSeparator))
		{
			values.add(java.net.URLDecoder.decode(value, "UTF-8"));
		}
		keyValueMap.put(java.net.URLDecoder.decode(keyStr, "UTF-8"), values);
		return keyValueMap;
	}

}
