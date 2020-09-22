package com.example.service.event.processor;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.adapter.DefaultBatchToRecordAdapter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import com.example.domain.EventMessage;
import com.example.service.EventMessageService;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
@Configuration
public class EventProcessor {
	
	@Value("${app.consumer.publish-to.topic}")
	private String topicToPublish;
	
	@Value("${app.dlt}")
	private String dlt;
	
	@Value("${app.consumer.sub-batch-per-partition}")
	private boolean subBatchPerPartition;	

	@Value("${app.consumer.eos-mode}")
	private String eosMode;
	
	@Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;
	
	@Autowired
	@Qualifier("standaloneTransactionKafkaTemplate")
    private KafkaTemplate<Object, Object> standaloneTransactionKafkaTemplate;
			
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
		  factory.getContainerProperties().setEosMode(ContainerProperties.EOSMode.valueOf(eosMode));
		  factory.getContainerProperties().setSubBatchPerPartition(subBatchPerPartition);
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
		  logger.info(String.format("KafkaTemplate.transactionIdPrefix: %s -  producerPerConsumerPartition: %s -" +
			  		 " - ConcurrentKafkaListenerContainerFactory EOS Mode: %s - subBatchPerPartition: %s ", 
					  this.kafkaTemplate.getTransactionIdPrefix()
					  , this.kafkaTemplate.getProducerFactory().isProducerPerConsumerPartition(),
					  factory.getContainerProperties().getEosMode()
					  , factory.getContainerProperties().getSubBatchPerPartition()
					  ));	  
		   return factory;  
	  }
	    	  
	@KafkaListener(topics = "#{'${app.consumer.subscribed-to.topic}'.split(',')}", containerFactory="kafkaListenerContainerFactory", groupId = "${spring.kafka.consumer.group-id}")
	public void consume(@Payload List<EventMessage> eventMessages,
			@Headers MessageHeaders headers) throws Exception {
		for (EventMessage eventMessage : eventMessages) 
		{
		  logger.info(String.format("Consuming message: %s - KafkaTemplate.transactionIdPrefix: %s", eventMessage, this.kafkaTemplate.getTransactionIdPrefix()));
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
    	standaloneTransactionKafkaTemplate.executeInTransaction(kTemplate -> {
    	    StringUtils.commaDelimitedListToSet(input).stream()
    	      .map(s -> new EventMessage(s))
    	      .forEach(evtMsg -> {
    	    	  logger.info(String.format("Producing message: %s - standaloneTransactionKafkaTemplate.transactionIdPrefix: %s", evtMsg.getDescription(), standaloneTransactionKafkaTemplate.getTransactionIdPrefix()));
    	    	  if (evtMsg.getDescription().toUpperCase().startsWith("PRODUCER_ERROR")) {
    	    		    throw new RuntimeException("ProducerError");
    	    		  }
    	    	  kTemplate.send(topic, evtMsg);
    	    	  });
    	    return null;
    	  });
    }
}