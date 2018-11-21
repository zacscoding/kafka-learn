package ch04;

import java.util.Collection;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

/**
 * @author zacconding
 * @Date 2018-10-30
 * @GitHub : https://github.com/zacscoding
 */
public class DemoRebalancer implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        // TODO :: Things to Do before your partition got revoked
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        // TODO :: Things to do when new partition get assigned
    }
}