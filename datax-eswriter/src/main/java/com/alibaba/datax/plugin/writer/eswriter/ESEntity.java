package com.alibaba.datax.plugin.writer.eswriter;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESEntity implements Serializable {
	
	private static final Logger LOG = LoggerFactory.getLogger(ESEntity.class);

	private static final long serialVersionUID = 1L;
	
	/** ES _ID字段*/
	private String _id = null;
	/** 数据来源文件 */
	private String sourceFile = null;
	/** 数据写入时间 */
	private String insertTime = null;
	/** 数据更新时间 */
	private String updateTime = null;
	/** 数据录入人员 */
	private String inputPerson = null;
	
	public ESEntity() {
		String nowTime = DateFormatter.TIME.get().format(new Date());
		this.insertTime = nowTime;
	}
	
	protected String generateID() {
		Field[] fields = this.getClass().getDeclaredFields();
		Field field = null;
		StringBuilder sb = new StringBuilder();
		try {
			for (int i = 0, len = fields.length; i < len; i++) {
				field = fields[i];
				if (Modifier.isStatic(field.getModifiers())) continue;
				String fieldName = field.getName();
				if ("serialVersionUID".equalsIgnoreCase(fieldName) || 
						"sourceFile".equalsIgnoreCase(fieldName) || 
							"inputPerson".equalsIgnoreCase(fieldName)) continue;
				field.setAccessible(true);
				Class<?> fieldType = field.getType();
				Object fieldValue = field.get(this);
				field.setAccessible(false);
				if (null == fieldValue) continue;
				if (String.class.isAssignableFrom(fieldType)) {
					fieldValue = String.valueOf(fieldValue).trim();
				}
				sb.append(fieldValue);
			}
		} catch (Exception e) {
			LOG.error("generate id error.", e);
		}
		return MD5Utils.hash(sb.toString());
	}
	
	public String get_id() {
		return StringUtils.isBlank(_id) ? generateID() : _id;
	}

	public void set_id(String _id) {
		this._id = _id;
	}
	
	public void remove_id() {
		this.set_id(null);
	}

	public String getSourceFile() {
		return sourceFile;
	}

	public void setSourceFile(String sourceFile) {
		this.sourceFile = sourceFile;
	}

	public String getInsertTime() {
		return insertTime;
	}

	public void setInsertTime(String insertTime) {
		this.insertTime = insertTime;
	}

	public String getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(String updateTime) {
		this.updateTime = updateTime;
	}

	public String getInputPerson() {
		return inputPerson;
	}

	public void setInputPerson(String inputPerson) {
		this.inputPerson = inputPerson;
	}
	
	
	
}
