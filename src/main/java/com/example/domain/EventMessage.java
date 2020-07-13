package com.example.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor
public class EventMessage {
	
	private int id;
	private String description;
	private String severity;

	public EventMessage(String description) {
		super();
		this.description = description;
	}
	
	public EventMessage(int id, String description) {
		this(description);
		this.id = id;
	}
}
