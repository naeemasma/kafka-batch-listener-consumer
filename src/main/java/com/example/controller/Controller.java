package com.example.controller;

import com.example.service.event.processor.EventProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/kafka")
public class Controller {
	
	@Value("${app.consumer.subscribed-to.topic}")
	private String topicToPublish;
		
    private final EventProcessor eventProcessor;

    @Autowired
    Controller(EventProcessor eventProcessor) {
        this.eventProcessor = eventProcessor;
    }
            
    @PostMapping(path = "/send/message/{event}")
	public void sendEventMessage(@PathVariable String event) {
		this.eventProcessor.sendEventMessage(topicToPublish, event);
	}
}