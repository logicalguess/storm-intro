package logicalguess.storm.counter

import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.tuple.{Fields, Tuple, Values}

class EmailExtractor extends BaseBasicBolt {
  override def execute(tuple: Tuple, basicOutputCollector: BasicOutputCollector): Unit = {
    val commit = tuple.getStringByField("entry")
    val parts = commit.split(" ")
    basicOutputCollector.emit(new Values(parts(1)))
  }

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(new Fields("email"))
  }
}
