package org.cisiondata.modules.login.dao;

import java.util.Map;

import org.cisiondata.modules.login.entity.User;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Repository;

@Repository("userDAO")
public interface UserDAO {

	/**
	 * 根据条件查询
	 * @param params
	 * @return
	 * @throws DataAccessException
	 */
	public User readDataByCondition(Map<String,Object> params) throws DataAccessException; 
	
	/**
	 * 根据账号修改密码和deleteFlag
	 * @param user
	 * @return
	 * @throws DataAccessException
	 */
	public int update(User user) throws DataAccessException;
}
