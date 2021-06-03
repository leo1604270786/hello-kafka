package top.zysite.hello.kafka;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import top.zysite.hello.kafka.service.KafkaProducerService;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RunWith(SpringRunner.class)
@SpringBootTest
class HelloKafkaApplicationTests {

    @Resource
    private KafkaProducerService kafkaProducerService;

    @Test
    void testSendMessageSync() throws Exception {
        String topic = "hello-kafka-test-topic";
        String key = "key1";
        String message = "firstMessage";
        kafkaProducerService.sendMessageSync(topic, key, message);
    }
    @Test
    public void testSendMessageGetResult() throws Exception {
        String topic = "hello-kafka-test-topic";
        String key = "key";
        String message = "helloSendMessageGetResult";
        kafkaProducerService.sendMessageGetResult(topic, key, message);
        kafkaProducerService.sendMessageGetResult(topic, null, message);
    }

    @Test
    public void testSendMessageAsync() {
        String topic = "hello-kafka-test-topic";
        String message = "firstAsyncMessage";
        kafkaProducerService.sendMessageAsync(topic, message);
    }

    @Test
    public void testMessageBuilder() throws Exception {
        String topic = "hello-kafka-test-topic";
        String key = "key1";
        String message = "helloMessageBuilder";
        kafkaProducerService.testMessageBuilder(topic, key, message);
    }

    /**
     * 测试事务
     */
    @Test
    public void testSendMessageInTransaction() {
        String topic = "hello-kafka-test-topic";
        String key = "key1";
        String message = "helloSendMessageInTransaction";
        kafkaProducerService.sendMessageInTransaction(topic, key, message);
    }

    /**
     * 测试批量消费
     * @throws Exception
     */
    @Test
    public void testConsumerBatch() throws Exception {
        //写入多条数据到批量topic：hello-batch
        String topic = "hello-batch";
        for(int i = 0; i < 20; i++) {
            kafkaProducerService.sendMessageSync(topic, null, "batchMessage" + i);
        }
    }

    /**
     * 测试消费者拦截器
     * @throws Exception
     */
    @Test
    public void testConsumerInterceptor() throws Exception {
        String topic = "consumer-interceptor";
        for(int i = 0; i < 2; i++) {
            kafkaProducerService.sendMessageSync(topic,null, "normalMessage" + i);
        }
        kafkaProducerService.sendMessageSync(topic, null, "filteredMessage");
        kafkaProducerService.sendMessageSync(topic, null, "filterMessage");
    }


}
