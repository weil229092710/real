import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.shaded.akka.org.jboss.netty.util.internal.StringUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * Created by root on 2019/7/5 0005.
 */
public class kafka_producer {
    public static String brokerList = "192.168.5.85:9092,192.168.5.86:9092,192.168.5.87:9092";
    public static String topicName = "kong-log-output";
     //public static String topicName = "test";

    public static Properties props = new Properties();

    public static void main(String[] args) throws InterruptedException {
        props.put("bootstrap.servers", brokerList);
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        mongo_kafka();


    }

    public static void mongo_kafka() throws InterruptedException {
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for(int i=0; i<1; i++){

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, getStr(i));
            producer.send(record);

            Thread.sleep(10);
        }


        producer.close();

    }

    public static String getStr(int i){
        System.out.println(i);

        String school_id = String.valueOf(i);
        String grade_id = String.valueOf(i%3);
        String class_id = String.valueOf(i%10);
        String student_id = String.valueOf(i%100);
        String question_id = UUID.randomUUID().toString().replaceAll("-","");
        String child_question_id = "1,2,3,4";
        String subject_id = String.valueOf(i%3);
        String code = "2b0,1a0";
        String judge = String.valueOf(i%2);
        String answer = "[{\"questionId\": \"59a81f822c8afb746bdbf5df\",\"answerType\": 1,\"studentAnswer\": \"[{ \\\"index\\\": 0,\\\"proofreadResult\\\": 0,\\\"stuReply\\\": \\\"\\\"},{ \\\"index\\\": 1,\\\"proofreadResult\\\": 0,\\\"stuReply\\\": \\\"\\\"},{ \\\"index\\\": 2,\\\"proofreadResult\\\": 0,\\\"stuReply\\\": \\\"\\\"},{ \\\"index\\\": 3,\\\"proofreadResult\\\": 0,\\\"stuReply\\\": \\\"\\\"},{ \\\"index\\\": 4,\\\"proofreadResult\\\": 0,\\\"stuReply\\\": \\\"\\\"},{ \\\"index\\\": 5,\\\"proofreadResult\\\": 0,\\\"stuReply\\\": \\\"\\\"},{ \\\"index\\\": 6,\\\"proofreadResult\\\": 0,\\\"stuReply\\\": \\\"\\\"},{ \\\"index\\\": 7,\\\"proofreadResult\\\": 0,\\\"stuReply\\\": \\\"\\\"},{ \\\"index\\\": 8,\\\"proofreadResult\\\": 0,\\\"stuReply\\\": \\\"\\\"},{ \\\"index\\\": 9,\\\"proofreadResult\\\": 0,\\\"stuReply\\\": \\\"\\\"}]\"}]";
        long time = System.currentTimeMillis();



        JSONObject json = JSON.parseObject("{}");
        // nginx数据

        json.put("request", "/v2/s/studentWorks/emend/?sign=74beedf060c354d58308a16d23264e01&t=1600308541557");
        json.put("appVersion", "v3.38.8.20200819S");
        json.put("scheme", "https");
        json.put("body_bytes_sent", "1887");
        json.put("type", "nginx");
        json.put("index_day", "2020.09.13");
        json.put("RSIP", "172.16.50.241:8083");
        json.put("request_time", "0.047");
        json.put("@version", "1");
        json.put("SchoolId", "664");
        json.put("remote_addr", "112.10.81.37");
        json.put("method", "PUT");
        json.put("appName", "com.xh.zhitongyunstu");
        json.put("remote_port", "35534");
        json.put("time_local", "13/Sep/2020:7:59:29 +0800");
        json.put("http_version", "1.1");
        json.put("local_addr", "ztp.yunzuoye.net");
        json.put("message", "\"112.10.81.37\" \"35534\" \"13/Sep/2020:23:59:29 +0800\" \"https\" \"POST /api/v1/platform/login?sign=f0405787de201109a1edd04969f6976e&t=1600012768605 HTTP/1.1\" \"0.047\" \"0.047\" \"200\" \"1887\" \"ztp.yunzuoye.net\" \"com.xh.zhitongyunstu/v3.38.8.20200819S (SM-P355C; android; 6.0.1; R22KA0023CF)\" \"2041\" \"1202\" \"130615\" \"4115\" \"172.16.50.241:8083\" \"000000000001fe370000017488315575\" ");
        json.put("bytes_sent", "2041");
        json.put("@timestamp", "2020-09-13T16:59:29.000Z");
        json.put("request_lenth", "1202");
        json.put("UserId", "158199");
        json.put("TraceId", "000000000001fe370000017488315575");
        json.put("upstream_response_time", "0.047");
        json.put("status", "200");

        return json.toJSONString();
    }
}
