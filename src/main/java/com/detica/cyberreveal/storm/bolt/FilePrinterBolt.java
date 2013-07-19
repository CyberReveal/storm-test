package com.detica.cyberreveal.storm.bolt;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * a Storm Bolt which appends all recieved tuples to a specified file.
 */
public class FilePrinterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 327323938874334973L;
	File outputFile;

	/**
	 * Instantiates a new file printer bolt.
	 * 
	 * @param outputFile
	 *            the output file
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public FilePrinterBolt(final File outputFile) throws IOException {
		this.outputFile = outputFile;

	}

	@Override
	public void execute(final Tuple tuple, final BasicOutputCollector collector) {
		try {
			FileWriter writer = new FileWriter(this.outputFile, true);
			writer.append(tuple.toString() + "\n");
			writer.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer ofd) {
	}

}
