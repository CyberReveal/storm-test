package com.detica.cyberreveal.storm.test.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordSplitBolt extends BaseBasicBolt {

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String line = tuple.getStringByField("line");
		String[] words = line.split("\\s|[\\.,\\?!:;'\"£$%^&\\*\\(\\)\\-\\=\\_\\+\\[\\]\\{\\}@\\#\\~\\>\\<]");
		for (int i = 0; i < words.length; i++) {
			String word = words[i].toLowerCase().trim();
			if (word.length() > 0) {
				collector.emit(new Values(word));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
