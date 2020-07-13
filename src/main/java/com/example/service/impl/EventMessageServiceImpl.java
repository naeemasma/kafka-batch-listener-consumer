package com.example.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.example.dao.service.EventMessageDao;
import com.example.domain.EventMessage;
import com.example.service.EventMessageService;

@Service("EventMessageService")
public class EventMessageServiceImpl implements EventMessageService {
	private final Logger logger = LoggerFactory.getLogger(EventMessageServiceImpl.class);
	@Autowired
    EventMessageDao eventMessageDao;
	
	@Override
	public int insert(EventMessage eventMessage) {
		eventMessage.setSeverity(
        		eventMessage.getDescription().toUpperCase().startsWith("ERROR")?"CRITICAL":"MEDIUM");
		int rowsUpdated = eventMessageDao.insert(eventMessage);	
		return rowsUpdated;
	}

}
