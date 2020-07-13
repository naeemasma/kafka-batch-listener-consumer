package com.example.domain;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor
public class EventMessageTypeThree {
	
	private int id;
	private int description;
	
	public EventMessageTypeThree(int description) {
		super();
		this.description = description;
	}
	
	public EventMessageTypeThree(int id, int description) {
		this(description);
		this.id = id;
	}
}
