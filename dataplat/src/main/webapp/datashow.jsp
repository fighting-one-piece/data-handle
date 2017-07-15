<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>数据库操作页面</title>
<style>
#datas {
	display: block;
	height: 600px;
	overflow: scroll;
}

.dataDiv {
	white-space: nowrap;
}

</style>
<script type="text/javascript" src="js/jquery.min.js"></script>
<script src="js/jquery.editable-select.js" ></script>
<link  rel="stylesheet"  type="text/css"  href="css/jquery.editable-select.css"/>
<script>
var pathName=window.document.location.pathname;
var projectName=pathName.substring(0,pathName.substr(1).indexOf('/')+1);
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
				url:projectName +"/mysql/2hdfs",
				traditional:true,
				data:{"index":index,"type":type,"database":database,
						"table":table,"mysqlColumn":mysqlColumn,"hdfsColumn":hdfsColumn
				},
				success:function(result){
					alert(result.code);
				},
				error:function(){
					alert("error")
				}
				
			})
		$('#drpPublisher2').editableSelect(
			     {
			       bg_iframe: true,
			       onSelect: function(list_item) {
			         $('#ddd2').val(this.text.val());
			       },
			       case_sensitive: false, // If set to true, the user has to type in an exact
			                              // match for the item to get highlighted
			       items_then_scroll: 10 ,// If there are more than 10 items, display a scrollbar
			       isFilter:true //If set to true, the item will be filtered according to the matching criteria.
			     }
			   );
		
		$("#drpPublisher2").html('<option value="3225">afgef111111</option><option value="3223">adfge111</option><option value="1032">abcd11111</option><option value="1031">a81811111</option>')
		$.ajax({
			type:"get",
			dataType:"json",
			url: projectName +"/indices",
			success:function(result){
				console.log("index");
				console.log(result);
			},
			error:function(result){
				console.log("请求失败")
			}
		});
		//获取index下type
		var index = "qq"
		$.ajax({
			type:"get",
			dataType:"json",
			data:{"index":index},
			url: projectName +"/types",
			success:function(result){
				console.log("type")
				console.log(result)
			},
			error:function(result){
				console.log("请求失败")
			}
		});
		var type = "qqdata";
		//获取index type下的属性
		$.ajax({
			type:"get",
			dataType:"json",
			data:{"index":index,"type":type},
			url: projectName +"/attributes",
			success:function(result){
				console.log("attribute")
				console.log(result)
			},
			error:function(result){
				console.log("请求失败")
			}
		});
		
	})
	
	
		
</script>
</head>
<body>
<div>
 <label for="name" >过滤之后的用户名</label>
 <select  name = "drpPublisher2"  id = "drpPublisher2"  class = "Winstar-input120" >
     <option value="3225">afgef</option>

	<option value="3223">adfge</option>

	<option value="1032">abcd1</option>

	<option value="1031">a8181</option>
 </select>
 <label for="name" >选中的值</label> <input  type="text"  id="ddd2" />
</div>
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


		<!-- 数据前1000 display: none-->
		<div id="datas" style="float: left; width: 50%; display: none;">
			<p style="text-align: center">The first 100 data</p>
			<table id="datateble" border="1px" cellspacing="0">
				<thead>
					<tr>
						<td><select>
								<option></option>
								<option></option>
						</select></td>
					</tr>
				</thead>
			</table>
		</div>
	</div>
<button id="123">123123</button>

</body>
</html>
