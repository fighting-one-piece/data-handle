package org.cisiondata.modules.websocket.server;

import org.cisiondata.modules.websocket.handler.SystemWebSocketHandler;
import org.cisiondata.modules.websocket.interceptor.SystemHandshakeInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.HandshakeInterceptor;

@Configuration  
@EnableWebMvc  
@EnableWebSocket
public class WebSocketConfigurerAdapter extends WebMvcConfigurerAdapter implements WebSocketConfigurer {  
  
    @Override  
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {  
    	/** websocket形式连接, client连接 */
        registry.addHandler(systemWebSocketHandler(), "/webSocketServer").addInterceptors(systemHandshakeInterceptor())
        	.setAllowedOrigins("*");  
        /** 不支持websocket的，采用sockjs */
        registry.addHandler(systemWebSocketHandler(), "/webSocketServer/sockjs").addInterceptors(systemHandshakeInterceptor())
        	.setAllowedOrigins("*").withSockJS();  
         //registry.addHandler(systemWebSocketHandler(), "/webSocketServer").addInterceptors(new WebSocketHandshakeInterceptor());  
         //registry.addHandler(systemWebSocketHandler(), "/sockjs/webSocketServer").addInterceptors(new WebSocketHandshakeInterceptor()).withSockJS();  
         //registry.addHandler(systemWebSocketHandler(), "/webSocketServer/sockjs").withSockJS();  
         /*registry.addHandler(systemWebSocketHandler(), "/ws").addInterceptors(new WebSocketHandshakeInterceptor()); 
            registry.addHandler(systemWebSocketHandler(), "/ws/sockjs").addInterceptors(new WebSocketHandshakeInterceptor()) 
                    .withSockJS();*/  
    }  
      
    @Bean  
    public WebSocketHandler systemWebSocketHandler(){  
        return new SystemWebSocketHandler();  
    }  
    
    @Bean
    public HandshakeInterceptor systemHandshakeInterceptor() {
    	return new SystemHandshakeInterceptor();
    }

}
