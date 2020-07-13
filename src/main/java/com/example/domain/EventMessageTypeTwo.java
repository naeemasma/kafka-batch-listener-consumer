package com.example.domain;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor
public class EventMessageTypeTwo {	
	private int id;
	private String description;
	
	public EventMessageTypeTwo(String description) {
		super();
		this.description = description;
	}
	
	public EventMessageTypeTwo(int id, String description) {
		this(description);
		this.id = id;
	}
}
