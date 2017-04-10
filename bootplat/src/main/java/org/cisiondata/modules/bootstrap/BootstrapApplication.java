package org.cisiondata.modules.bootstrap;

import javax.sql.DataSource;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.annotation.MapperScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

@EnableAutoConfiguration  
@SpringBootApplication  
@ComponentScan  
@MapperScan(basePackages = "org.cisiondata.modules.**.dao") 
public class BootstrapApplication {

	private static Logger LOG = LoggerFactory.getLogger(BootstrapApplication.class);
	
	/** DataSource配置 */  
    @Bean  
    @ConfigurationProperties(prefix="spring.datasource")  
    public DataSource dataSource() {  
        return new org.apache.tomcat.jdbc.pool.DataSource();  
    } 
    
    /** SqlSeesion配置 */  
    @Bean  
    public SqlSessionFactory sqlSessionFactoryBean() throws Exception {  
        SqlSessionFactoryBean sqlSessionFactoryBean = new SqlSessionFactoryBean();  
        sqlSessionFactoryBean.setDataSource(dataSource());  
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();  
        sqlSessionFactoryBean.setMapperLocations(resolver.getResources("classpath*:org/cisiondata/modules/**/mapper/**/*.xml"));  
        return sqlSessionFactoryBean.getObject();  
    }  
   
    @Bean  
    public PlatformTransactionManager transactionManager() {  
        return new DataSourceTransactionManager(dataSource());  
    } 
	
	public static void main(String[] args) {
		SpringApplication.run(BootstrapApplication.class, args);
		LOG.info("Server Bootstrap");
	}
	
}
