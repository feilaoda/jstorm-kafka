kafka spout for jstorm

Example:

```
        builder.setSpout("kafka-spout", new KafkaSpout(), 1);
        builder.setBolt("print-bolt", new PrintBolt(), 2).shuffleGrouping("kafka-spout");
```

```
public class PrintBolt implements IBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(PrintBolt.class);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        // TODO Auto-generated method stub

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        // TODO Auto-generated method stub
        String message;
        try {
            message = new String(tuple.getBinaryByField("bytes"), "UTF-8");
            logger.debug("Message:{}", message);
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub

    }

}
```
