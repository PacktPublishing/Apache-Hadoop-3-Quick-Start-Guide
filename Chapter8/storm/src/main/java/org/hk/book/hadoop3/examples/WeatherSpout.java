package org.hk.book.hadoop3.examples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;

import org.apache.http.impl.client.HttpClientBuilder;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.json.JSONArray;
import org.json.JSONObject;

public class WeatherSpout extends BaseRichSpout{

	private SpoutOutputCollector collector;
	
	public void nextTuple() {
		// TODO Auto-generated method stub
		String weatherURL = "https://query.yahooapis.com/v1/public/yql?"
				+ "q=select%20item.condition.text%20from%20weather.forecast%20where%20woeid%20in%20("
				+ "select%20woeid%20from%20geo.places(1)%20where%20text%3D%22dallas%2C%20tx%22)&"
				+ "format=json&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys";
		HttpClient client = HttpClientBuilder.create().build();

		HttpPost post = new HttpPost(weatherURL);
		List nameValuePairs = new ArrayList(1);
		 //nameValuePairs.add(new BasicNameValuePair('name', 'value')); //you can as many name value pair as you want in the list.
		try {
			post.setEntity(new UrlEncodedFormEntity(nameValuePairs));
			HttpResponse response = client.execute(post);
			 BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			 String line = "";
			 String jsonMesg = "";
			 while ((line = rd.readLine()) != null) {
				 jsonMesg+= line;
			 }
			 System.out.println(jsonMesg);		
			 collector.emit(new Values(jsonMesg));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}
	}

	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		// TODO Auto-generated method stub
		outputFieldsDeclarer.declare(new Fields("weather"));
	}
}
