package com.detica.cyberreveal.storm.test.bolt;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class FilePrinterBolt extends BaseBasicBolt {

	private File outputFile;

	public FilePrinterBolt(File outputFile) throws IOException {
		this.outputFile = outputFile;

	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		try {
			FileWriter writer = new FileWriter(outputFile, true);
			try {
				writer.append(tuple.toString() + "\n");
			} finally {
				writer.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
	}

}
