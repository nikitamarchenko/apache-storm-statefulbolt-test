package name.nmarchenko.org.apache.storm.statefulbolttest;

import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class StatefulWriteBolt extends BaseStatefulBolt<KeyValueState<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(RandomIntegerSpout.class);
    KeyValueState<String, String> kvState;
    private OutputCollector collector;

    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        long value = ((Number) input.getValueByField("value")).longValue();
        long ts = ((Number) input.getValueByField("ts")).longValue();
        long msgid = ((Number) input.getValueByField("msgid")).longValue();

        String v1 = String.format("%d %d %d", msgid, ts, value);

        LOG.info(v1);
        kvState.put("input", v1);

        collector.ack(input);
    }

    @Override
    public void initState(KeyValueState<String, String> state) {
        kvState = state;
        kvState.put("input", "none");
    }

}