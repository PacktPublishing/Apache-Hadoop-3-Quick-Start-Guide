package org.hk.book.hadoop3.examples;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class WeatherTopology {

	/**
	 * This is a real time streaming program for consistently checking the weather of a city, and 
	 * raise an alarm in case if the weather condition is bad.  
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// Create the topology
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("weather-spout", new WeatherSpout());
		builder.setBolt("weather-bolt", new WeatherAnalyzerBolt(), 2).shuffleGrouping("weather-spout");
		// Create a configuration object.
		Config conf = new Config();
		conf.setNumWorkers(1);
		StormSubmitter.submitTopology("myTopology", conf, builder.createTopology());
	}

}
