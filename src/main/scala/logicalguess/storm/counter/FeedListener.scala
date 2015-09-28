package logicalguess.storm.counter

import java.util

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.{Fields, Values}

class FeedListener extends BaseRichSpout {

  private var output: SpoutOutputCollector = null
  private val entries = List[String](
    "COMMENT dan@scalageeks.io",
    "LIKE john@scalageeks.io",
    "LIKE john@scalageeks.io",
    "COMMENT john@scalageeks.io",
    "LIKE dan@scalageeks.io",
    "LIKE jerry@scalageeks.io",
    "COMMENT dan@scalageeks.io",
    "LIKE steve@scalageeks.io"
  )

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(new Fields("entry"))
  }

  override def nextTuple(): Unit = {
    for (entry <- entries) {
      output.emit(new Values(entry))
    }
  }

  override def open(map: util.Map[_, _], topologyContext: TopologyContext, spoutOutputCollector: SpoutOutputCollector): Unit = {
    output = spoutOutputCollector
  }
}
