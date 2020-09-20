package com.example.service.event.processor;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.adapter.DefaultBatchToRecordAdapter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.transaction.ChainedKafkaTransactionManager;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.example.constants.Constants;
import com.example.domain.EventMessage;
import com.example.domain.EventMessageTypeTwo;
import com.example.service.EventMessageService;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

@Service
@Configuration
public class EventProcessor {

	public final static String CONTEXT_RECORD="record";
	
	@Value("${app.consumer.publish-to.topic}")
	private String topicToPublish;
	
	@Value("${app.dlt}")
	private String dlt;
	
	@Value("${spring.kafka.producer.transaction-id-prefix}")
	private String transactionIdPrefix;
	
	@Value("${spring.kafka.producer.bootstrap-servers}")
	private String producer_bootstrap_servers;
	
	@Value("${app.producer.producer-per-consumer-partition}")
	private boolean producerPerConsumerPartition;
	
	@Value("${app.producer.sub-batch-per-partition}")
	private boolean subBatchPerPartition; 
	
	@Value("${app.producer.client-id}")
	private String producerClientId;
	
	@Value("${spring.kafka.producer.key-serializer}")
	private String keySerializer; 
	
	@Value("${spring.kafka.producer.value-serializer}")
	private String valueSerializer; 
	
	@Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;
			
	@Autowired
    EventMessageService eventMessageService;
		
    private final Logger logger = LoggerFactory.getLogger(EventProcessor.class);
        
      @Bean 
	  public RecordMessageConverter converter() { 
		  return new StringJsonMessageConverter(); 
	  }      
	    	  
	  @Bean 
	  public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
		  ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
		  ConsumerFactory<Object, Object> kafkaConsumerFactory, 
		  KafkaTemplate<Object, Object> template
		  , ObjectMapper objectMapper
          ) 
	  { 
		  ConcurrentKafkaListenerContainerFactory<Object, Object>
		  factory = new ConcurrentKafkaListenerContainerFactory<>();
		  configurer.configure(factory, kafkaConsumerFactory);
		  template.setTransactionIdPrefix(this.transactionIdPrefix);
		  factory.setBatchListener(true);
		  factory.setBatchToRecordAdapter(new DefaultBatchToRecordAdapter<>((record, ex) ->  {;
			  template.executeInTransaction(kTemplate ->{
			    	try {
			    		kTemplate.send(dlt, (EventMessage)record.value());
			    	} catch (Exception e) {
						logger.error(e.getMessage());
					}
						return true;
			        });
	        }));
		  logger.info(String.format("KafkaTemplate.transactionIdPrefix: %s - subBatchPerPartition: %s -  producerPerConsumerPartition: %s" +
			  		 " - ConcurrentKafkaListenerContainerFactory EOS Mode: %s", 
					  this.kafkaTemplate.getTransactionIdPrefix()
					  , this.kafkaTemplate.getProducerFactory().getConfigurationProperties().get(Constants.SUB_BATCH_PER_PARTITION)
					  , this.kafkaTemplate.getProducerFactory().isProducerPerConsumerPartition(),
					  factory.getContainerProperties().getEosMode()
					  ));	  
		   return factory;  
	  }
	  
	@KafkaListener(topics = "#{'${app.consumer.subscribed-to.topic}'.split(',')}", containerFactory="kafkaListenerContainerFactory", groupId = "${spring.kafka.consumer.group-id}")
	public void consume(@Payload List<EventMessage> eventMessages,
			@Headers MessageHeaders headers) throws Exception {
		for (EventMessage eventMessage : eventMessages) 
		{
		  logger.info(String.format("Consumed message: %s", eventMessage));
		  EventMessage msg = new EventMessage(eventMessage.getDescription()+"");
		  eventMessageService.insert(msg);
		  this.kafkaTemplate.send(topicToPublish,eventMessage);
		}
    }
    
    @KafkaListener(topics = "${app.consumer.publish-to.topic}", containerFactory="kafkaListenerContainerFactory", groupId = "${spring.kafka.consumer.group-id}")
    public void listenConsumerPublishedTopic(@Payload List<EventMessage> eventMessages,
			@Headers MessageHeaders headers) throws Exception {
		for (EventMessage eventMessage : eventMessages) 
		{
			logger.info(String.format("Recieved in topic %s: %s", topicToPublish, eventMessage));
		}
    }
    
    @KafkaListener(topics = "${app.dlt}", containerFactory="kafkaListenerContainerFactory", groupId = "${spring.kafka.consumer.group-id}")
    public void dltListen(Object eventMessage) {
      logger.info(String.format("Recieved Message in DLT: %s", eventMessage));
    }
    
    public void sendEventMessage(String topic, String input) {
    	this.kafkaTemplate.executeInTransaction(kTemplate -> {
    	    StringUtils.commaDelimitedListToSet(input).stream()
    	      .map(s -> new EventMessage(s))
    	      .forEach(evtMsg -> {
    	    	  logger.info(String.format("Producing message: %s", evtMsg.getDescription()));
    	    	  if (evtMsg.getDescription().toUpperCase().startsWith("PRODUCER_ERROR")) {
    	    		    throw new RuntimeException("ProducerError");
    	    		  }
    	    	  kTemplate.send(topic, evtMsg);
    	    	  });
    	    return null;
    	  });
    }
    

    @Bean
    public Map<String, Object> producerConfigs() throws ClassNotFoundException {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producer_bootstrap_servers);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Class.forName(valueSerializer));
        props.put(ProducerConfig.CLIENT_ID_CONFIG,producerClientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Class.forName(keySerializer));
        props.put(Constants.SUB_BATCH_PER_PARTITION, subBatchPerPartition);
        return props;
    }
    
    @Bean
    public ProducerFactory<Object, Object> kafkaProducerFactory() throws ClassNotFoundException {
    	DefaultKafkaProducerFactory<Object, Object> pf=  new DefaultKafkaProducerFactory<>(producerConfigs());
        pf.setProducerPerConsumerPartition(producerPerConsumerPartition);
        pf.setTransactionIdPrefix(this.transactionIdPrefix);
        return pf;
    }
}