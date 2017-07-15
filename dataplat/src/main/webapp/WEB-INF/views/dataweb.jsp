
<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>数据库操作页面</title>
<style>
#datas{
     display:block;
     height:600px;
     overflow:scroll;
}

 .dataDiv {
	white-space: nowrap;	
}
/*
.dataDiv:hover {
	overflow: visible;
} */
</style>
<script type="text/javascript"
	src="${pageContext.request.contextPath}/js/jquery.min.js"></script>
<script>
	$(function(){
		var index = "qq"
		var type = "qqdata"
		var database = "tjl20120312"
		var table = "111"
		var mysqlColumn = ["c1","c2"]
		var hdfsColumn = ["qq","password"]
		$.ajax({
			type:"post",
			dataType:"json",
			data:{"index":index,"type":type,"database":database,
					"table":table,"mysqlColumn":mysqlColumn,"hdfsColumn":hdfsColumn
			},
			traditional:true,
			success:function(result){
				alert(result.code);
			},
			error:function(){
				alert("error")
			}
			
		})
	})
</script>
</head>
<body>

	<!--数据库名-->
	<div>
		<div id="dbname" class="indexdb" style="float: left; width: 30%;">
			<p style="text-align: center">DB</p>
			<ul id="test">
				<!-- 动态生成li -->
			</ul>
		</div>
		<!--数据表名 -->
		<div id="tbname" style="float: left; width: 20%; display: none;">
			<p style="text-align: center">TB</p>
			<ul id="biao">
				<!-- 动态生成li -->
			</ul>
		</div>
		<!--mapping  提交框-->
		<div id="2hdfs" style="width: 30%; float: left; display: none;">
			<form action="../mysql/2hdfs" method="post" name="pass">
				<div>
					<!-- <select name="datatype" onchange=alert(this.value)>-->
					<select name="dataindex" id="dataindex">
						<option>index</option>
					</select> <select name="datatype" id="datatype">
						<option>type</option>
					</select>
					<p></p>
					导入人：<input value="" type="text" name="inputPsion">
					<p></p>
					数据库：<input id="database" readonly="readonly" type="text"
						name="database">
					<p></p>
					导入表：<input id="tablename" readonly="readonly" type="text"
						name="table">
					<p></p>
					<input type="button" name="Submit" value="增加一行" onclick="addRow();" />
					<input type="button" name="Submit" value="清空数据" onclick="clean();" />
					<table id="myTable" border="1" style="width: 320px;">
						<!-- 动态生成列 -->
						<tr>
							<td>数据库列</td>
							<td>HDFS列</td>
							<td></td>
						</tr>
					</table>
					<p></p>
					<input id="submitLogin" type="submit" name="commit"
						value="生成mapping" onclick="sub()" />
				</div>
			</form>
		</div>

		<!-- 数据前1000 display: none-->
		<div id="datas" style="float: left; width: 50%; display: none;">
			<p style="text-align: center">The first 100 data</p>
			<table id="datateble" border="1px" cellspacing="0">
			</table>
		</div>
	</div>


</body>
</html>
