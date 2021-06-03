package top.zysite.hello.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import top.zysite.hello.kafka.interceptor.MyConsumerInterceptor;

import java.util.HashMap;
import java.util.Map;

/**
 * kafka 消费者配置类
 *
 * @author Leo
 * @create 2020/12/31 15:09
 **/
@Slf4j
@Configuration
public class KafkaConsumerConfiguration {

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        //设置 consumerFactory
        factory.setConsumerFactory(consumerFactory());
        //设置是否开启批量监听
        factory.setBatchListener(false);
        //设置消费者组中的线程数量
        factory.setConcurrency(1);
        return factory;
    }
    /**
     * consumerFactory
     * @return
     */
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        //kafka集群地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094");
        //自动提交 offset 默认 true
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        //自动提交的频率 单位 ms
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        //批量消费最大数量
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        //消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup");
        //session超时，超过这个时间consumer没有发送心跳,就会触发rebalance操作
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 120000);
        //请求超时
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120000);
        //Key 反序列化类
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //Value 反序列化类
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //当kafka中没有初始offset或offset超出范围时将自动重置offset
        //earliest:重置为分区中最小的offset
        //latest:重置为分区中最新的offset(消费分区中新产生的数据)
        //none:只要有一个分区不存在已提交的offset,就抛出异常
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        //设置Consumer拦截器
        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MyConsumerInterceptor.class.getName());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * 消费异常处理器
     * @return
     */
    @Bean
    public ConsumerAwareListenerErrorHandler consumerAwareListenerErrorHandler() {
        return new ConsumerAwareListenerErrorHandler() {
            @Override
            public Object handleError(Message<?> message, ListenerExecutionFailedException exception, Consumer<?, ?> consumer) {
                //打印消费异常的消息和异常信息
                log.error("consumer failed! message: {}, exceptionMsg: {}, groupId: {}", message, exception.getMessage(), exception.getGroupId());
                return null;
            }
        };
    }
}