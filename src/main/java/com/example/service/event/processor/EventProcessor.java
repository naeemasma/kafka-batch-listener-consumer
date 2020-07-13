package com.example.service.event.processor;import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.adapter.DefaultBatchToRecordAdapter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import com.example.domain.EventMessage;
import com.example.domain.EventMessageTypeTwo;
import com.example.service.EventMessageService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;

@Service
@Configuration
public class EventProcessor {

	public final static String CONTEXT_RECORD="record";
	
	@Value("${app.consumer.publish-to.topic}")
	private String topicToPublish;
	
	@Value("${app.dlt}")
	private String dlt;
	
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
    
    @KafkaListener(topics = "${app.consumer.publish-to.topic}", containerFactory="kafkaListenerContainerFactory", groupId = "${app.consumer.group-id}")
    public void listenConsumerPublishedTopic(@Payload List<EventMessage> eventMessages,
			@Headers MessageHeaders headers) throws Exception {
		for (EventMessage eventMessage : eventMessages) 
		{
			logger.info(String.format("Recieved in topic, published by the event processor consumer: ->%s", eventMessage));
		}
    }
    
    @KafkaListener(topics = "${app.dlt}", containerFactory="kafkaListenerContainerFactory", groupId = "${app.consumer.group-id}")
    public void dltListen(Object eventMessage) {
      logger.info(String.format("Recieved Message in DLT: %s", eventMessage));
    }
    
    public void sendEventMessage(String topic, String input) {
    	this.kafkaTemplate.executeInTransaction(kTemplate -> {
    	    StringUtils.commaDelimitedListToSet(input).stream()
    	      .map(s -> new EventMessageTypeTwo(s))
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
}