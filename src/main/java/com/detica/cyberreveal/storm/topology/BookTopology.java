package com.detica.cyberreveal.storm.topology;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.detica.cyberreveal.storm.bolt.FilePrinterBolt;
import com.detica.cyberreveal.storm.bolt.PrinterBolt;
import com.detica.cyberreveal.storm.bolt.WordCountBolt;
import com.detica.cyberreveal.storm.bolt.WordSplitBolt;
import com.detica.cyberreveal.storm.spout.BookLineSpout;

/**
 * The Class TestTopology.
 */
public final class BookTopology implements Runnable {

	private final String topologyName;
	private static final Logger LOG = LoggerFactory
			.getLogger(BookTopology.class);
	private final String inputFile;
	private final File wordCountOutputFile;

	/**
	 * Instantiates a new book topology.
	 * 
	 * @param topologyName
	 *            the topology name
	 */
	public BookTopology(String topologyName, String inputFile,
			File wordCountOutputFile) {
		this.topologyName = topologyName;
		this.inputFile = inputFile;
		this.wordCountOutputFile = wordCountOutputFile;
	}

	@Override
	public void run() {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("line", new BookLineSpout(), 2);
		builder.setBolt("wordSplitter", new WordSplitBolt(), 2)
				.shuffleGrouping("line");
		builder.setBolt("wordCount", new WordCountBolt(), 2).shuffleGrouping(
				"wordSplitter");
		builder.setBolt("printWordCount", new PrinterBolt(), 2)
				.shuffleGrouping("wordCount");
		try {
			builder.setBolt("printWordCountToFile",
					new FilePrinterBolt(this.wordCountOutputFile), 2)
					.shuffleGrouping("wordCount");
		} catch (IOException e) {
			e.printStackTrace();
		}

		Config conf = new Config();
		conf.setDebug(true);
		conf.put("inputFile", this.inputFile);
		if (this.topologyName != null) {
			conf.setNumWorkers(20);

			try {
				StormSubmitter.submitTopology(this.topologyName, conf,
						builder.createTopology());
			} catch (AlreadyAliveException e) {
				LOG.error("Error submitting topology");
			} catch (InvalidTopologyException e) {
				LOG.error("Error submitting topology", e);
			}
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
}
