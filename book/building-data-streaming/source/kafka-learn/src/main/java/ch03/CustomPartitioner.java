package ch03;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

/**
 * @author zacconding
 * @Date 2018-10-03
 * @GitHub : https://github.com/zacscoding
 */
public class CustomPartitioner implements Partitioner {

    @Override
    public int partition(String topicName, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topicName);

        int size = partitions.size();
        // TODO : Partition logic here
        return 0;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> map) {
    }
}
