package logicalguess.storm.counter

import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Fields
import backtype.storm.utils.Utils
import backtype.storm.{Config, LocalCluster}

object LocalTopologyRunner {

  private val oneMinute = 60000
  private val tenMinutes = 600000

  def main (args: Array[String]) {
    val builder = new TopologyBuilder()

    builder.setSpout("feed-listener", new FeedListener())

    builder.setBolt("email-extractor", new EmailExtractor())
      .shuffleGrouping("feed-listener")

    builder.setBolt("email-counter", new EmailCounter())
      .fieldsGrouping("email-extractor", new Fields("email"))

    val config = new Config()
    config.setDebug(false)

    val topology = builder.createTopology()

    val cluster = new LocalCluster()
    cluster.submitTopology("count-topology", config, topology)

    Utils.sleep(oneMinute)
    cluster.killTopology("count-topology")
    cluster.shutdown()
  }

}
