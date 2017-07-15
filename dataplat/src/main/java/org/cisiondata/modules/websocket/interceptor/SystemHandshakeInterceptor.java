package org.cisiondata.modules.websocket.interceptor;

import java.net.InetSocketAddress;
import java.util.Map;

import javax.servlet.http.HttpSession;

import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.support.HttpSessionHandshakeInterceptor;

public class SystemHandshakeInterceptor extends HttpSessionHandshakeInterceptor {

	@Override
	public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler,
			Map<String, Object> attributes) throws Exception {
		System.out.println("开始拦截类");
		// 解决The extension [x-webkit-deflate-frame] is not supported问题
        if (request.getHeaders().containsKey("Sec-WebSocket-Extensions")) {
            request.getHeaders().set("Sec-WebSocket-Extensions",
                    "permessage-deflate");
        }
        /**
        if (getSession(request) != null) {
            ServletServerHttpRequest servletRequest = (ServletServerHttpRequest) request;
            HttpServletRequest httpServletRequest = servletRequest.getServletRequest();
            attributes.put("userId", httpServletRequest.getParameter("userId"));
        }
        */
//        List<String> values = request.getHeaders().get("accessToken");
//        for (String value : values) {
//        	System.out.println(value);
//        }
        System.out.println("path: " + request.getURI().getPath());
        System.out.println("query: " + request.getURI().getQuery());
        System.out.println("auth: " + request.getURI().getAuthority());
        InetSocketAddress inetSocketAddress = request.getRemoteAddress();
        if (null != inetSocketAddress) {
        	attributes.put("remoteHost", inetSocketAddress.getHostName());
        	if (null != inetSocketAddress.getAddress()) {
        		attributes.put("remoteIp", inetSocketAddress.getAddress().getHostAddress());
        	}
        }
	    return super.beforeHandshake(request, response, wsHandler, attributes);
	}

	@Override
	public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler,
			Exception ex) {
		System.out.println("结束拦截类");
		super.afterHandshake(request, response, wsHandler, ex);
	}
	
	private HttpSession getSession(ServerHttpRequest request) {
        if (request instanceof ServletServerHttpRequest) {
            ServletServerHttpRequest serverRequest = (ServletServerHttpRequest) request;
            return serverRequest.getServletRequest().getSession();
        }
        return null;
    }

}
