package org.cisiondata.modules.address.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

import org.cisiondata.modules.abstr.entity.PKAutoEntity;

/** 行政区划表*/
@Entity
@Table(name="T_ADMINISTRATIVE_DIVISION")
public class AdministrativeDivision extends PKAutoEntity<Long> {

	private static final long serialVersionUID = 1L;
	
	/** 省、自治区、直辖市、特别行政区*/
	@Column(name="PROVINCE")
	private String province = null;
	/** 市、自治州、区*/
	@Column(name="CITY")
	private String city = null;
	/** 县*/
	@Column(name="COUNTY")
	private String county = null;
	/** 街道办事处、镇、乡*/
	@Column(name="VILLAGES_TOWNS")
	private String villagesTowns = null;
	/** 居民委员会、村民委员会*/
	@Column(name="RESIDENTS_COMMITTEE")
	private String residentsCommittee = null;
	/** 编码*/
	@Column(name="CODE")
	private String code = null;
	
	
}
