package com.detica.cyberreveal.storm.test.spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.detica.cyberreveal.storm.test.bolt.PrinterBolt;
import com.detica.cyberreveal.storm.test.bolt.WordCountBolt;
import com.detica.cyberreveal.storm.test.bolt.WordSplitBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class BookLineSpout extends BaseRichSpout {
	SpoutOutputCollector collector;
	private List<String> lines;
	private int index;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.lines = new ArrayList<String>();
		this.index = 0;
		this.collector = collector;
		File inputFile = new File((String) conf.get("inputFile"));
		try {
			FileReader inStream = new FileReader(inputFile);
			try {
				BufferedReader buff = new BufferedReader(inStream);
				try {
					String line = buff.readLine();
					while (line != null) {
						this.lines.add(line);
						line = buff.readLine();
					}
				} finally {
					buff.close();
				}
			} finally {
				inStream.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void nextTuple() {
		if (!this.lines.isEmpty()) {
			String line = this.lines.remove(0);
			this.collector.emit(new Values(line));
		}
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

}