package com.alibaba.datax.common.plugin;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.common.element.Record;

public class CTaskPluginCollector extends TaskPluginCollector {
	
	private List<String> dirtyRecords = null;

	@Override
	public void collectDirtyRecord(Record dirtyRecord, Throwable t, String errorMessage) {
		StringBuilder sb = new StringBuilder(100);
		for (int i = 0, len = dirtyRecord.getColumnNumber(); i < len; i++) {
			sb.append(String.valueOf(dirtyRecord.getColumn(i).getRawData()));
		}
		getDirtyRecords().add(sb.toString());
	}

	@Override
	public void collectMessage(String key, String value) {
		
	}

	public List<String> getDirtyRecords() {
		if (null == dirtyRecords) {
			dirtyRecords = new ArrayList<String>();
		}
		return dirtyRecords;
	}

}
