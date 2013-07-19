package com.detica.cyberreveal.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * A storm bolt which splits a line into words.
 */
public class WordSplitBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1990152678196466476L;

	@Override
	public void execute(final Tuple tuple, final BasicOutputCollector collector) {
		String line = tuple.getStringByField("line");
		// split line by whitespace and punctuation characters
		String[] words = line
				.split("\\s|[\\.,\\?!:;'\"£$%^&\\*\\(\\)\\-\\=\\_\\+\\[\\]\\{\\}@\\#\\~\\>\\<]");
		for (int i = 0; i < words.length; i++) {
			String word = words[i].toLowerCase().trim();
			if (word.length() > 0) {
				collector.emit(new Values(word));
			}
		}
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
