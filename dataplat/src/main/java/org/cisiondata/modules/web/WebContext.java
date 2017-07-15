package org.cisiondata.modules.web;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class WebContext {
	
	private static ThreadLocal<WebContext> instance = new ThreadLocal<WebContext>();
	private HttpServletRequest request = null;
	private HttpServletResponse response = null;
	private ServletContext servletContext = null;

	public WebContext(HttpServletRequest request, HttpServletResponse response, ServletContext servletContext) {
		this.request = request;
		this.response = response;
		this.servletContext = servletContext;
	}

	public static WebContext get() {
		return WebContext.instance.get();
	}

	public static void set(WebContext instance) {
		WebContext.instance.set(instance);
	}

	public HttpServletRequest getRequest() {
		return request;
	}

	public HttpServletResponse getResponse() {
		return response;
	}
	
	public ServletContext getServletContext() {
		return servletContext;
	}
}
