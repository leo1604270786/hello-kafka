package top.zysite.hello.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * kafka 消费服务
 *
 * @author Leo
 * @create 2020/12/31 16:06
 **/
@Slf4j
@Service
public class KafkaConsumerService {

    /**
     * 消费单条消息,topics 可以监听多个topic，如：topics = {"topic1", "topic2"}
     * @param message 消息
     */
    @KafkaListener(id = "consumerSingle", topics = "hello-kafka-test-topic")
    public void consumerSingle(String message) {
        log.info("consumerSingle ====> message: {}", message);
    }


/*    @KafkaListener(id = "consumerBatch", topicPartitions = {
            @TopicPartition(topic = "hello-batch1", partitions = "0"),
            @TopicPartition(topic = "hello-batch2", partitionOffsets = @PartitionOffset(partition = "2", initialOffset = "4"))
    })*/
    /**
     * 批量消费消息
     * @param messages
     */
    @KafkaListener(id = "consumerBatch", topics = "hello-batch")
    public void consumerBatch(List<ConsumerRecord<String, String>> messages) {
        log.info("consumerBatch =====> messageSize: {}", messages.size());
        log.info(messages.toString());
    }

    /**
     * 指定消费异常处理器
     * @param message
     */
    @KafkaListener(id = "consumerException", topics = "hello-kafka-test-topic", errorHandler = "consumerAwareListenerErrorHandler")
    public void consumerException(String message) {
        throw new RuntimeException("consumer exception");
    }

    /**
     * 验证ConsumerInterceptor
     * @param message
     */
    @KafkaListener(id = "interceptor", topics = "consumer-interceptor")
    public void consumerInterceptor(String message) {
        log.info("consumerInterceptor ====> message: {}", message);
    }
}