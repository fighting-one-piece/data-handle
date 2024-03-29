package org.cisiondata.modules.websocket.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;

public class SystemWebSocketHandler implements WebSocketHandler {

	private Logger log = LoggerFactory.getLogger(SystemWebSocketHandler.class);

	private static final List<WebSocketSession> users = new ArrayList<WebSocketSession>();

	@Override
	public void afterConnectionEstablished(WebSocketSession session) throws Exception {
		System.out.println("ConnectionEstablished");
		log.debug("ConnectionEstablished");
		
		Set<String> keys = session.getHandshakeHeaders().keySet();
		keys.stream().forEach(key -> System.out.println(key));
		System.out.println(session.getHandshakeHeaders().getFirst("Cookie"));  
		
		Map<String, Object> attributes = session.getAttributes();
		for (Map.Entry<String, Object> entry : attributes.entrySet()) {
			System.out.println(entry.getKey() + " : " + entry.getValue());
		}
		
		users.add(session);

		session.sendMessage(new TextMessage("connect success"));
		session.sendMessage(new TextMessage("you can receive message"));
		
//		int i = 1;
//		while (true) {
//			session.sendMessage(new TextMessage("message " + i));
//			i++;
//		}

	}

	@Override
	public void handleMessage(WebSocketSession session, WebSocketMessage<?> message) throws Exception {
		System.out.println("handleMessage: " + message.toString());
		log.info("handleMessage: " + message.toString());
		// sendMessageToUsers();
		session.sendMessage(new TextMessage(new Date() + ""));
	}

	@Override
	public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
		if (session.isOpen()) {
			session.close();
		}
		users.remove(session);

		log.debug("handleTransportError" + exception.getMessage());
	}

	@Override
	public void afterConnectionClosed(WebSocketSession session, CloseStatus closeStatus) throws Exception {
		users.remove(session);
		log.debug("afterConnectionClosed" + closeStatus.getReason());

	}

	@Override
	public boolean supportsPartialMessages() {
		return false;
	}

	/**
	 * 给所有在线用户发送消息
	 * 
	 * @param message
	 */
	public void sendMessageToUsers(TextMessage message) {
		for (WebSocketSession user : users) {
			try {
				if (user.isOpen()) {
					user.sendMessage(message);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
