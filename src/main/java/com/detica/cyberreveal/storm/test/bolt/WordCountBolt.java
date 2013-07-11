package com.detica.cyberreveal.storm.test.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt extends BaseBasicBolt {

	Map<String, Long> wordCounts = new HashMap<String, Long>();

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String word = tuple.getStringByField("word");
		Long wordCount = wordCounts.get(word);
		if (wordCount == null) {
			wordCount = 0L;
		}
		wordCount = wordCount + 1;
		wordCounts.put(word, wordCount);
		collector.emit(new Values(word, wordCount));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

}
