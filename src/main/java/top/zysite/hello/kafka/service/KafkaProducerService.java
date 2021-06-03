package top.zysite.hello.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.Resource;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * kafka 生产服务
 *
 * @author Leo
 * @create 2020/12/31 16:06
 **/
@Slf4j
@Service
public class KafkaProducerService {
    @Qualifier("kafkaTemplate")
    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    @Qualifier("kafkaTemplateWithTransaction")
    @Resource
    private KafkaTemplate<String, String> kafkaTemplateWithTransaction;

    /**
     * 发送消息（同步）
     * @param topic 主题
     * @param key 键
     * @param message 值
     */
    public void sendMessageSync(String topic, String key, String message) throws InterruptedException, ExecutionException, TimeoutException {
        //可以指定最长等待时间，也可以不指定
        kafkaTemplate.send(topic, message).get(10, TimeUnit.SECONDS);
        log.info("sendMessageSync => topic: {}, key: {}, message: {}", topic, key, message);
        //指定key，kafka根据key进行hash，决定存入哪个partition
//        kafkaTemplate.send(topic, key, message).get(10, TimeUnit.SECONDS);
        //存入指定partition
//        kafkaTemplate.send(topic, 0, key, message).get(10, TimeUnit.SECONDS);
    }

    /**
     * 发送消息并获取结果
     * @param topic
     * @param message
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void sendMessageGetResult(String topic, String key, String message) throws ExecutionException, InterruptedException {
        SendResult<String, String> result = kafkaTemplate.send(topic, message).get();
        log.info("sendMessageSync => topic: {}, key: {}, message: {}", topic, key, message);
        log.info("The partition the message was sent to: " + result.getRecordMetadata().partition());
    }

    /**
     * 发送消息（异步）
     * @param topic 主题
     * @param message 消息内容
     */
    public void sendMessageAsync(String topic, String message) {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);
        //添加回调
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                log.error("sendMessageAsync failure! topic : {}, message: {}", topic, message);
            }

            @Override
            public void onSuccess(SendResult<String, String> stringStringSendResult) {
                log.info("sendMessageAsync success! topic: {}, message: {}", topic, message);
            }
        });
    }

    /**
     * 可以将消息组装成 Message 对象和 ProducerRecord 对象发送
     * @param topic
     * @param key
     * @param message
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    public void testMessageBuilder(String topic, String key, String message) throws InterruptedException, ExecutionException, TimeoutException {
        // 组装消息
        Message msg = MessageBuilder.withPayload(message)
                .setHeader(KafkaHeaders.MESSAGE_KEY, key)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.PREFIX,"kafka_")
                .build();
        //同步发送
        kafkaTemplate.send(msg).get();
        // 组装消息
//        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, message);
//        kafkaTemplate.send(producerRecord).get(10, TimeUnit.SECONDS);
    }

    /**
     * 以事务方式发送消息
     * @param topic
     * @param key
     * @param message
     */
    public void sendMessageInTransaction(String topic, String key, String message) {
        kafkaTemplateWithTransaction.executeInTransaction(new KafkaOperations.OperationsCallback<String, String, Object>() {
            @Override
            public Object doInOperations(KafkaOperations<String, String> kafkaOperations) {
                kafkaOperations.send(topic, key, message);
                //出现异常将会中断事务，消息不会发送出去
                throw new RuntimeException("exception");
            }
        });
    }

}