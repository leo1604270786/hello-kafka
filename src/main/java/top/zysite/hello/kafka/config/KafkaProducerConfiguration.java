package top.zysite.hello.kafka.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import top.zysite.hello.kafka.partitioner.MyPartitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * kafka 生产者配置类
 *
 * @author Leo
 * @create 2020/12/31 15:09
 **/
@Configuration
public class KafkaProducerConfiguration {
    /**
     * 不包含事务 producerFactory
     * @return
     */
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        //kafka 集群地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094");
        //重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        //应答级别
        //acks=0 把消息发送到kafka就认为发送成功
        //acks=1 把消息发送到kafka leader分区，并且写入磁盘就认为发送成功
        //acks=all 把消息发送到kafka leader分区，并且leader分区的副本follower对消息进行了同步就任务发送成功
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //KafkaProducer.send() 和 partitionsFor() 方法的最长阻塞时间 单位 ms
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 6000);
        //批量处理的最大大小 单位 byte
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 4096);
        //发送延时,当生产端积累的消息达到batch-size或接收到消息linger.ms后,生产者就会将消息提交给kafka
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1000);
        //生产者可用缓冲区的最大值 单位 byte
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        //每条消息最大的大小
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1048576);
        //客户端ID
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "hello-kafka");
        //Key 序列化方式
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Value 序列化方式
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //消息压缩：none、lz4、gzip、snappy，默认为 none。
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        //自定义分区器
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class.getName());
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * 包含事务 producerFactory
     * @return
     */
    public ProducerFactory<String, String> producerFactoryWithTransaction() {
        DefaultKafkaProducerFactory<String, String> defaultKafkaProducerFactory = (DefaultKafkaProducerFactory<String, String>) producerFactory();
        //设置事务Id前缀
        defaultKafkaProducerFactory.setTransactionIdPrefix("tx");
        return defaultKafkaProducerFactory;
    }

    /**
     * 不包含事务 kafkaTemplate
     * @return
     */
    @Bean("kafkaTemplate")
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * 包含事务 kafkaTemplate
     * @return
     */
    @Bean("kafkaTemplateWithTransaction")
    public KafkaTemplate<String, String> kafkaTemplateWithTransaction() {
        return new KafkaTemplate<>(producerFactoryWithTransaction());
    }

    /**
     * 以该方式配置事务管理器：就不能以普通方式发送消息，只能通过 kafkaTemplate.executeInTransaction 或
     * 在方法上加 @Transactional 注解来发送消息，否则报错
     * @param producerFactory
     * @return
     */
//    @Bean
//    public KafkaTransactionManager<Integer, String> kafkaTransactionManager(ProducerFactory<Integer, String> producerFactory) {
//        return new KafkaTransactionManager<>(producerFactory);
//    }

}