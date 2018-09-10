package com.mschroeder.kafka.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ImportantData {
	private int id;
	private String name;
	private String description;
}
