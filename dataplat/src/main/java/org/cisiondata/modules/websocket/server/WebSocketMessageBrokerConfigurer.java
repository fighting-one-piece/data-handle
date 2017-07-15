package org.cisiondata.modules.websocket.server;

//import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.AbstractWebSocketMessageBrokerConfigurer;
//import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;

//@Configuration
//@EnableWebSocketMessageBroker
public class WebSocketMessageBrokerConfigurer extends AbstractWebSocketMessageBrokerConfigurer{
	
	@Override
    public void registerStompEndpoints(StompEndpointRegistry stompEndpointRegistry) {
        //加入setAllowedOrigins("*")防止浏览器出现403 Forbidden的问题
        stompEndpointRegistry.addEndpoint("/pay-result").setAllowedOrigins("*").withSockJS();
    }
	
	@Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.enableStompBrokerRelay("/topic", "/queue");
        registry.setApplicationDestinationPrefixes("/app");
    }

}

/**
 * 
<dependency>  
    <groupId>javax.websocket</groupId>  
    <artifactId>javax.websocket-api</artifactId>  
    <version>1.1</version>  
    <scope>provided</scope>  
</dependency> 
<dependency>
	<groupId>io.projectreactor</groupId>
	<artifactId>reactor-core</artifactId>
	<version>2.0.8.RELEASE</version>
</dependency>

<dependency>
	<groupId>io.projectreactor</groupId>
	<artifactId>reactor-net</artifactId>
	<version>2.0.8.RELEASE</version>
</dependency>

<dependency>
	<groupId>io.netty</groupId>
	<artifactId>netty-all</artifactId>
	<version>4.1.6.Final</version>
</dependency>
*/
