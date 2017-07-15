
<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>josn展示界面</title>
<script type="text/javascript"
	src="${pageContext.request.contextPath}/js/jquery.min.js"></script>
<script>
	$(function() {
		//获取所有json文件
		$.ajax({
			type : "post",
			url : "${pageContext.request.contextPath}/mysql/getJsonList",
			error : function(data) {
				alert("出错了！！:");
			},
			success : function(data) {
				var datas = data.jsonlist
				for (var i in datas) {
					$("#jsonlist").append("<li><input name='jsonname' value="+datas[i]+" type='checkbox'/>"+datas[i]+"</li>");
				}
			}
		});
		//显示单击json文件的数据
		$("#jsonlist").on("click", "li", function() {
			var jsonname=$(this).text();
			//alert(jsonname);
			 $.ajax({
				type : "GET",
				url : "${pageContext.request.contextPath}/mysql/readJson?jsonname="+jsonname,
				error : function(data) {
					alert("出错了！！:");
				},
				success : function(data) {
					var result = data.jsonData;
					//alert(result);
					$("#jsondata").html("");
					$("#jsondata").append(result);
				}
			}); 
		});
	});
	//执行json
	function execute() {
		var chk_value =[];
		$('input:checkbox[name=jsonname]:checked').each(function(){
			chk_value.push($(this).val());
			$.ajax({
				type : "GET",
				url : "${pageContext.request.contextPath}/mysql/execute?jsonname="+$(this).val(),
				error : function(data) {
					alert("出错了！！:");
				},
				success : function(data) {
					var result = data.result;
					console.log(result);
					alert("成功！");
				}
			});
     	});
		if(chk_value.length==0){
			alert("你还没有选择任何内容！");
		}
	};
</script>
</head>
<body>
	<div>${msg}</div>
	<div>
		<ul id="jsonlist"></ul>
	</div>
	<div id="jsondata">
		
	</div>
	<div>
		<input type="button" value="执行选中json" onclick="execute()"/>
	</div>
</body>
</html>
