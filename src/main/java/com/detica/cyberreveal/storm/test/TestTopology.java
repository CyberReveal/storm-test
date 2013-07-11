package com.detica.cyberreveal.storm.test;


import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.impl.Log4JLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.detica.cyberreveal.storm.test.bolt.FilePrinterBolt;
import com.detica.cyberreveal.storm.test.bolt.PrinterBolt;
import com.detica.cyberreveal.storm.test.bolt.WordCountBolt;
import com.detica.cyberreveal.storm.test.bolt.WordSplitBolt;
import com.detica.cyberreveal.storm.test.spout.BookLineSpout;

public class TestTopology {

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, IOException {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("line", new BookLineSpout(), 2);
		builder.setBolt("wordSplitter", new WordSplitBolt(), 2)
				.shuffleGrouping("line");
		builder.setBolt("wordCount", new WordCountBolt(), 2).shuffleGrouping(
				"wordSplitter");
		builder.setBolt("printWordCount", new PrinterBolt(), 2)
				.shuffleGrouping("wordCount");
		builder.setBolt("printWordCountToFile", new FilePrinterBolt(new File("target/wordCounts.out")), 2)
		.shuffleGrouping("wordCount");

		Config conf = new Config();
		conf.setDebug(true);
		conf.put("inputFile",
				"src/main/resources/AdventuresOfSherlockHolmes.txt");
		if (args != null && args.length > 0) {
			conf.setNumWorkers(20);

			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}

}
