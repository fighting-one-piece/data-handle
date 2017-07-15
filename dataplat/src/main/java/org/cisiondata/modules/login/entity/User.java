package org.cisiondata.modules.login.entity;

import java.io.Serializable;

import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name="T_USER_ATTRIBUTE")
public class User implements Serializable {

	private static final long serialVersionUID = 1L;
	
	/** ID*/
	private Long id = null;
	/** 账号*/
	private String account = null;
	/** 密码*/
	private String password = null;
	/** 是否删除*/
	private Boolean deleteFlag = Boolean.FALSE;
	/** 服务器中的目录名*/
	private String directory = null;
	
	public String getDirectory() {
		return directory;
	}
	public void setDirectory(String directory) {
		this.directory = directory;
	}
	public String getAccount() {
		return account;
	}
	public void setAccount(String account) {
		this.account = account;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public Boolean getDeleteFlag() {
		return deleteFlag;
	}
	public void setDeleteFlag(Boolean deleteFlag) {
		this.deleteFlag = deleteFlag;
	}
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}

}
