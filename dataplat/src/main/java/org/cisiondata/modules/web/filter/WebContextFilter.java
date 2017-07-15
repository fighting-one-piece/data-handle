package org.cisiondata.modules.web.filter;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.cisiondata.modules.web.WebContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebContextFilter implements Filter {
	
	public Logger LOG = LoggerFactory.getLogger(WebContextFilter.class);
	
	private ServletContext ctx = null;

	public void init(FilterConfig config) throws ServletException {
		ctx = config.getServletContext();
	}

	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) 
			throws IOException, ServletException {
		HttpServletRequest httpServletRequest = (HttpServletRequest) request;
		HttpServletResponse httpServletResponse = (HttpServletResponse) response;
		WebContext instance = new WebContext(httpServletRequest, httpServletResponse, ctx);
		try {
			WebContext.set(instance);
			chain.doFilter(request, response);
		} finally {
			WebContext.set(null);
		}
	}
	
	public void destroy() {
	}

}
