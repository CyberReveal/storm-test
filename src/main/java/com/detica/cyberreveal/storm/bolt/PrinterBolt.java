package com.detica.cyberreveal.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * A Storm Bolt which prints all received tuples to System.out
 */
public class PrinterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -5237229359039158290L;

	@Override
	public void execute(final Tuple tuple, final BasicOutputCollector collector) {
		System.out.println(tuple);
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer ofd) {
	}

}
