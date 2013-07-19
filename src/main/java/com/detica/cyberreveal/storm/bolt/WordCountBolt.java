package com.detica.cyberreveal.storm.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * A Storm Bolt which takes a word as an input and outputs the word with a count
 * of the number of times the word has been seen previously.
 */
public class WordCountBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 5623239456140401639L;
	private final Map<String, Long> wordCounts = new HashMap<String, Long>();

	@Override
	public void execute(final Tuple tuple, final BasicOutputCollector collector) {
		String word = tuple.getStringByField("word");
		Long wordCount = this.wordCounts.get(word);
		// If word has not been seen before, add it to the map
		if (wordCount == null) {
			wordCount = 0L;
		}
		wordCount = wordCount + 1;
		this.wordCounts.put(word, wordCount);
		collector.emit(new Values(word, wordCount));
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

}
