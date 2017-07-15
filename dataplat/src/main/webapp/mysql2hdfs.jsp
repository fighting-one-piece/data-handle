<html>
<%@ page language="java" import="java.util.*" pageEncoding="UTF-8"%>
<%
	String path = request.getContextPath();
	String basePath = request.getScheme() + "://" + request.getServerName() + ":" + request.getServerPort()
			+ path;
%>
<head>
<script type="text/javascript" src="<%=basePath%>/js/jquery-1.8.0.js"></script>
<script type="text/javascript">
	$(function() {
		//alert("加载");
	});
	function sub() {
		var content = document.getElementById("columnname");
		//alert(content.value);
	};
	function addRow() {
		var tr = "<tr><td>数据库列：<input type=\"text\" name=\"mysqlColumn\"> HDFS列：<input type=\"text\" name=\"columnname\"></td><td><input type=\"button\" value=\"Delete\"onclick=\"deleteRow(this)\"></td></tr>";
		$("#myTable").append(tr);
	};
	function deleteRow(r) {
		var i = r.parentNode.parentNode.rowIndex;
		document.getElementById('myTable').deleteRow(i);
	};
</script>
</head>
<body>
	<a id="anchor"></a>
	<h5 align="center">&nbsp;</h5>
	<div align="center">
		<h1>MYSQL2HDFS</h1>
		<form action="mysql/2hdfs" method="post" name="pass">
			<div>
				<!-- <select name="datatype" onchange=alert(this.value)>-->
				<select name="dataindex">
					<option>index</option>
					<option value="financial">financial</option>
				</select> <select name="datatype">
					<option>type</option>
					<option value="car">car</option>
					<option value="finance">finance</option>
				</select>
				<p></p>
				导入人：<input value="" type="text" name="inputPsion">
				<p></p>
				数据库：<input value="" type="text" name="database">
				<p></p>
				导入表：<input value="" type="text" name="table">
				<p></p>
				<input type="button" name="Submit" value="增加一行" onclick="addRow();" />
				<input type="button" name="Submit" value="清空数据" onclick="clean();" />
				<table id="myTable" border="1">
					<tr>
						<td>数据库列：<input value="" type="text" name="mysqlColumn">
							HDFS列：<input value="" type="text" name="columnname"></td>
						<td><input type="button" value="Delete"
							onclick="deleteRow(this)"></td>
					</tr>
					<tr>
						<td>数据库列：<input type="text" name="mysqlColumn">
							HDFS列：<input type="text" name="columnname"></td>
						<td><input type="button" value="Delete"
							onclick="deleteRow(this)"></td>
					</tr>
				</table>
				<p></p>
				<input id="submitLogin" type="submit" name="commit"
					value="生成mapping" onclick="sub()" />
			</div>
		</form>
	</div>
</body>
</html>
