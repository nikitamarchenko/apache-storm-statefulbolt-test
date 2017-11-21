package name.nmarchenko.org.apache.storm.statefulbolttest;

import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StormGenericTopology {
	public static final Logger LOG = LoggerFactory
			.getLogger(StormGenericTopology.class);

	private final TopologyProperties topologyProperties;

	public StormGenericTopology(TopologyProperties topologyProperties) {
		this.topologyProperties = topologyProperties;
	}
	
	public void runTopology() throws Exception{

		StormTopology stormTopology = buildTopology();
		String stormExecutionMode = topologyProperties.getStormExecutionMode();
	
		switch (stormExecutionMode){
			case ("cluster"):
				StormSubmitter.submitTopology(topologyProperties.getTopologyName(), topologyProperties.getStormConfig(), stormTopology);
				break;
			case ("local"):
			default:
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology(topologyProperties.getTopologyName(), topologyProperties.getStormConfig(), stormTopology);
				Thread.sleep(topologyProperties.getLocalTimeExecution());
				cluster.killTopology(topologyProperties.getTopologyName());
				cluster.shutdown();
				System.exit(0);
		}	
	}
	
	private StormTopology buildTopology()
	{
		/*
		BrokerHosts kafkaBrokerHosts = new ZkHosts(topologyProperties.getZookeeperHosts());
		String kafkaTopic = topologyProperties.getKafkaTopic();
		SpoutConfig kafkaConfig = new SpoutConfig(kafkaBrokerHosts, kafkaTopic, "/storm/kafka/"+topologyProperties.getTopologyName(), kafkaTopic);
		kafkaConfig.forceFromStart = topologyProperties.isKafkaStartFromBeginning();
		*/

		TopologyBuilder builder = new TopologyBuilder();

		/*
		builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), topologyProperties.getKafkaSpoutParallelism());
		builder.setBolt("FilterBolt", new FilterMessageBolt(), topologyProperties.getFilterBoltParallelism()).shuffleGrouping("KafkaSpout");
		builder.setBolt("TCPBolt", new TCPBolt(), topologyProperties.getTcpBoltParallelism()).shuffleGrouping("FilterBolt");
		*/


		builder.setSpout("RandomIntegerSpout", new RandomIntegerSpout(), 1);
		builder.setBolt("StatefulReadBolt", new StatefulReadBolt(), 1).noneGrouping("RandomIntegerSpout");
		builder.setBolt("StatefulWriteBolt", new StatefulWriteBolt(), 2).shuffleGrouping("RandomIntegerSpout");

		return builder.createTopology();
	}
	
	public static void main(String[] args) throws Exception {
		String propertiesFile = args[0];
		TopologyProperties topologyProperties = new TopologyProperties(propertiesFile);
		StormGenericTopology topology = new StormGenericTopology(topologyProperties);
		topology.runTopology();
	}
}
