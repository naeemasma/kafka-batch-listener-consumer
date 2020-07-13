package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.example.service.event.processor.EventProcessor;
@SpringBootApplication
public class EventProcessorApplication implements CommandLineRunner{
	
	@Autowired
    EventProcessor eventProcessor;
			
	private final Logger logger = LoggerFactory.getLogger(EventProcessorApplication.class);
	public static void main(String[] args) {
		SpringApplication.run(EventProcessorApplication.class, args);
	}
	
	@Override
	public void run(String... args) throws Exception {		
	}	
}
