package com.mschroeder.kafka.service;

import com.mschroeder.kafka.domain.ImportantData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ImportantDataServiceImpl implements ImportantDataService {
	public void syncData(ImportantData data) {
		log.info("syncing id={}, name={}", data.getId(), data.getName());
	}
}
