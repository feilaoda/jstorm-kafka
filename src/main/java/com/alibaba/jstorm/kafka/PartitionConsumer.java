package com.alibaba.jstorm.kafka;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

/**
 *
 * @author feilaoda
 *
 */
public class PartitionConsumer {
    private static Logger logger = LoggerFactory.getLogger(PartitionConsumer.class);

    static enum EmitState {
        EMIT_MORE, EMIT_END, EMIT_NONE
    }

    private int partition;
    private KafkaConsumer consumer;

    private PartitionCoordinator coordinator;

    private KafkaSpoutConfig config;
    private LinkedList<MessageAndOffset> emittedMessages = new LinkedList<MessageAndOffset>();
    private SortedSet<Long> pendingOffsets = new TreeSet<Long>();
    private SortedSet<Long> failedOffsets = new TreeSet<Long>();
    private long emittedOffset;
    private long lastCommittedOffset;
    private ZkState zkState;
    private Map stormConf;

    public PartitionConsumer(Map conf, KafkaSpoutConfig config, int partition, ZkState offsetState) {
        this.stormConf = conf;
        this.config = config;
        this.partition = partition;
        this.consumer = new KafkaConsumer(config);
        this.zkState = offsetState;

        Long jsonOffset = null;
        try {
            Map<Object, Object> json = offsetState.readJSON(zkPath());
            if (json != null) {
                // jsonTopologyId = (String)((Map<Object,Object>)json.get("topology"));
                jsonOffset = (Long) json.get("offset");
            }
        } catch (Throwable e) {
            logger.warn("Error reading and/or parsing at ZkNode: " + zkPath(), e);
        }

        try {
            if (config.fromBeginning) {
                emittedOffset = consumer.getOffset(config.topic, partition, kafka.api.OffsetRequest.EarliestTime());
            } else if(config.startOffsetTime == -1){
                emittedOffset = consumer.getOffset(config.topic, partition, kafka.api.OffsetRequest.LatestTime());
            } else
            {
                if (jsonOffset == null) {
                    lastCommittedOffset = consumer.getOffset(config.topic, partition, kafka.api.OffsetRequest.LatestTime());
                } else {
                    lastCommittedOffset = jsonOffset;
                }
                emittedOffset = lastCommittedOffset;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        logger.info("start consume from offset {}", emittedOffset);
    }

    public EmitState emit(KafkaOutCollector collector) {
        int count = 0;
        try {
            if (emittedMessages.isEmpty()) {
                boolean filled = fillMessages();
                if(!filled) {
                    return EmitState.EMIT_END;
                }
            }

            List<Object> bytesList = new ArrayList<Object>();
            while (true) {
                MessageAndOffset toEmitMsg = emittedMessages.pollFirst();
                if (toEmitMsg == null) {
                    break;
                }
                bytesList.add(toByteArray(toEmitMsg.message().payload()));
                count++;
                if (count >= config.sendBatchCount) {
                    break;
                }
            }
            collector.emit(bytesList, new KafkaMessageId(partition, 0));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            collector.commit();
        }
        if (emittedMessages.isEmpty() && count == 0) {
            return EmitState.EMIT_END;
        } else {
            return EmitState.EMIT_MORE;
        }
    }

    private boolean fillMessages() {

        ByteBufferMessageSet msgs;
        try {
            long start = System.currentTimeMillis();
            long startOffset = emittedOffset;
            msgs = consumer.fetchMessages(partition, emittedOffset);

            if (msgs == null) {
                logger.info("fetch null message from offset {}", emittedOffset);
                return false;
            }

            int count = 0;
            for (MessageAndOffset msg : msgs) {
                count += 1;
                pendingOffsets.add(emittedOffset);
                emittedMessages.add(msg);
                emittedOffset = msg.nextOffset();

//                logger.debug("fillmessage fetched a message:{}, offset:{}", msg.message().toString(), msg.offset());
            }
            long end = System.currentTimeMillis();
            logger.info("fetch message from partition:" + partition + ", start offset:"+startOffset+", latest offset:" + emittedOffset + ", size:" + msgs.sizeInBytes() + ", count:" + count
                    + ", time:" + (end - start));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
        return true;
    }

    public void commitState() {
        try {
            if(!failedOffsets.isEmpty()) {
                long offset = failedOffsets.first();
                if (emittedOffset > offset) {
                    emittedOffset = offset;
                    pendingOffsets.tailSet(offset).clear();
                }
                failedOffsets.tailSet(offset).clear();
            }
            long lastOffset = lastCompletedOffset();
            if (lastOffset != lastCommittedOffset) {
                Map<Object, Object> data = new HashMap<Object, Object>();
                data.put("topology", stormConf.get("topology"));
                data.put("offset", lastOffset);
                data.put("partition", partition);
                data.put("broker", ImmutableMap.of("host", consumer.getLeaderBroker().host(), "port", consumer.getLeaderBroker().port()));
                data.put("topic", config.topic);
                zkState.writeJSON(zkPath(), data);
                lastCommittedOffset = lastOffset;
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

    }

    public long lastCompletedOffset() {
        long lastOffset = 0;
        if (pendingOffsets.isEmpty()) {
            lastOffset = emittedOffset;
        } else {
            try {
                lastOffset = pendingOffsets.first();
            } catch (NoSuchElementException e) {
                lastOffset = emittedOffset;
            }
        }
        return lastOffset;
    }

    public void ack(long offset) {
        try {
            pendingOffsets.remove(offset);
        } catch (Exception e) {
            logger.error("offset ack error " + offset);
        }
    }

    public void fail(long offset) {
        failedOffsets.add(offset);
    }

    public void close() {
        coordinator.removeConsumer(partition);
        consumer.close();
    }

    @SuppressWarnings("unchecked")
    public Iterable<List<Object>> generateTuples(Message msg) {
        Iterable<List<Object>> tups = null;
        ByteBuffer payload = msg.payload();
        if (payload == null) {
            return null;
        }
        tups = Arrays.asList(toTuple(toByteArray(payload)));
        return tups;
    }

    public static List<Object> toTuple(Object... values) {
        List<Object> ret = new ArrayList<Object>();
        for (Object v : values) {
            ret.add(v);
        }
        return ret;
    }

    public static byte[] toByteArray(ByteBuffer buffer) {
        byte[] ret = new byte[buffer.remaining()];
        buffer.get(ret, 0, ret.length);
        return ret;
    }

    private String zkPath() {
        return config.zkRoot + "/kafka/offset/topic/" + config.topic + "/" + config.clientId + "/" + partition;
    }

    public PartitionCoordinator getCoordinator() {
        return coordinator;
    }

    public void setCoordinator(PartitionCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public KafkaConsumer getConsumer() {
        return consumer;
    }

    public void setConsumer(KafkaConsumer consumer) {
        this.consumer = consumer;
    }
}
