package com.tapdata.constant;

import com.tapdata.entity.Connections;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class KafkaUtil {

	public static KafkaProducer<String, String> createProducer(Connections connections) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", StringSerializer.class.getName());
		KafkaProducer<String, String> producer = new KafkaProducer<>(props);

		return producer;
	}

	public static void main(String[] args) throws ParseException {
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
		System.out.println(sdf.parse(sdf.format(date)));

		Time time = new Time(System.currentTimeMillis());
		System.out.println(time.toString());
	}
}
