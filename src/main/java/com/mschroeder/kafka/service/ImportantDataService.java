package com.mschroeder.kafka.service;

import com.mschroeder.kafka.domain.ImportantData;

public interface ImportantDataService {
	void syncData(ImportantData data);
}
