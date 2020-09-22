package com.example.service.event.processor.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.Map;

@Service
@Configuration
public class EventProcessorConfig {
	
	@Value("${spring.kafka.producer.transaction-id-prefix}")
	private String transactionIdPrefix;
	
	@Value("${app.producer.standalone.transaction-id-prefix}")
	private String standaloneTransactionIdPrefix;
	
	@Value("${spring.kafka.producer.bootstrap-servers}")
	private String producer_bootstrap_servers;
	
	@Value("${app.producer.producer-per-consumer-partition}")
	private boolean producerPerConsumerPartition;
		
	@Value("${app.producer.client-id}")
	private String producerClientId;
	
	@Value("${spring.kafka.producer.key-serializer}")
	private String keySerializer; 
	
	@Value("${spring.kafka.producer.value-serializer}")
	private String valueSerializer; 
		
    private final Logger logger = LoggerFactory.getLogger(EventProcessorConfig.class);
         
    @Bean
    public Map<String, Object> producerConfigs() throws ClassNotFoundException {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producer_bootstrap_servers);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Class.forName(valueSerializer));
        props.put(ProducerConfig.CLIENT_ID_CONFIG,producerClientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Class.forName(keySerializer));
        return props;
    }
    
    @Bean(name="kafkaProducerFactory")
    public ProducerFactory<Object, Object> kafkaProducerFactory() throws ClassNotFoundException {
    	DefaultKafkaProducerFactory<Object, Object> pf=  new DefaultKafkaProducerFactory<>(producerConfigs());
        pf.setProducerPerConsumerPartition(producerPerConsumerPartition);
        pf.setTransactionIdPrefix(this.transactionIdPrefix);
        return pf;
    }
    
    @Bean(name="kafkaTemplate")
    @Primary
    public KafkaTemplate<Object, Object> kafkaTemplate() throws ClassNotFoundException {
    	KafkaTemplate<Object, Object> kafkaTemplate = new KafkaTemplate<Object, Object>(kafkaProducerFactory());
    	return kafkaTemplate;
    }   

    
    @Bean(name="standaloneTransactionKafkaTemplate")
    public KafkaTemplate<Object, Object> standaloneTransactionKafkaTemplate() throws ClassNotFoundException {
    	KafkaTemplate<Object, Object> kafkaTemplate = new KafkaTemplate<Object, Object>(kafkaProducerFactory());
    	kafkaTemplate.setTransactionIdPrefix(standaloneTransactionIdPrefix);
    	return kafkaTemplate;
    }
}