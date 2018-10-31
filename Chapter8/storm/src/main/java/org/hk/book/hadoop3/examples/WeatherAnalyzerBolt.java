package org.hk.book.hadoop3.examples;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.JSONObject;

public class WeatherAnalyzerBolt extends BaseRichBolt{
	
	OutputCollector collector;
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		String jsonMesg = tuple.getString(0);
		if(jsonMesg == null || jsonMesg.length() < 1)
			return;
		JSONObject js = new JSONObject(jsonMesg);
		 String condition = js.getJSONObject("query").getJSONObject("results").
				 getJSONObject("channel").getJSONObject("item").getJSONObject("condition").getString("text");
		 System.out.println(condition);
		 
		 if ("tornado".equals(condition) || 
				 "tropical storm".equals(condition) ||
				 "hurricane".equals(condition) ||
				 "thunderstorms".equals(condition) ||
				 "freezing rain".equals(condition))
			 collector.emit(new Values("code red"));
		 else 
			 collector.emit(new Values("no alarm"));
			 
		
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("alarm"));
		
	}

}
