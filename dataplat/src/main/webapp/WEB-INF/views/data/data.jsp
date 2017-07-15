<html>
<%@ page language="java" import="java.util.*" pageEncoding="UTF-8"%>
<%
	String path = request.getContextPath();
	String basePath = request.getScheme() + "://" + request.getServerName() + ":" + request.getServerPort()
			+ path;
%>
<head>
<link rel="stylesheet" href="<%=basePath%>/css/data/data.css" />
<script type="text/javascript" src="<%=basePath%>/js/jquery-1.8.0.js"></script>
<script type="text/javascript" src="<%=basePath%>/js/data/data.js"></script>
</head>
<body>
		<div>
			<div id="dbName">
				<h3> 数据名称</h3>
				<button id="getdbName">获取当前用户数据库名</button>
			</div>
			<div id="tableNames">
				<div id="db"></div>
				<h3> 表名称</h3>
				<button id="getableName">获取当前数据库表名</button>
			</div>
			<div id="datas" >
			<h3>数据</h3>
			获取每次间隔的行数<input id="datas" type="text" name="fname" value=100 />
			<button id="button">查看</button>
			</div>
		</div>
	</body>

</html>
