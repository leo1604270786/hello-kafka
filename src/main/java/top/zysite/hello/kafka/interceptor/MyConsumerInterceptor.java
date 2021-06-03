package top.zysite.hello.kafka.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 消费者拦截器
 *
 * @author Leo
 * @create 2021/5/27 16:30
 **/
@Slf4j
public class MyConsumerInterceptor implements ConsumerInterceptor<String, String> {

    /**
     * KafkaConsumer 会在 poll 方法返回之前调用该方法，可以在该方法中对消息进行过滤
     * @param records
     * @return
     */
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        System.out.println("********** before interceptor: " + records.count() + "**********");
        Map<TopicPartition, List<ConsumerRecord<String, String>>> newRecords = new HashMap<>();
        //遍历每个topic、partition
        for (TopicPartition topicPartition : records.partitions()) {
            //获取特定topic、partition下的消息列表
            List<ConsumerRecord<String, String>> recordList = records.records(topicPartition);
            //过滤
            List<ConsumerRecord<String, String>> filteredList = recordList.stream()
                    .filter(record -> !record.value().contains("filter")).collect(Collectors.toList());
            //放入新的消息记录里
            newRecords.put(topicPartition, filteredList);
        }
        ConsumerRecords<String, String> filteredRecords = new ConsumerRecords<>(newRecords);
        System.out.println("********** after interceptor: " + filteredRecords.count() + "**********");
        //返回过滤后的消息记录
        return filteredRecords;
    }

    /**
     * 提交完offset之后调用该方法
     * @param offsets
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (!offsets.isEmpty()) {
            offsets.forEach(((topicPartition, offsetAndMetadata) -> {
                log.info("partition : " + topicPartition + ", offset : " + offsetAndMetadata);
            }));
        }
    }


    @Override
    public void close() {

    }


    @Override
    public void configure(Map<String, ?> configs) {

    }
}