<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>文件上传</title>
<style type="text/css">
body {
	text-align: center
}
</style>
<script>
	window.onload = function() {
		var inputElement = document.getElementById("input");
		inputElement.addEventListener("change", handleFiles, false);
		function handleFiles() {
			var fileList = this.files;
			var dd = document.getElementById('content');
			var flag = false;
			for (var i = 0; i < fileList.length; i++) {
				var fileName = fileList[i].name;
				if (!flag && fileName.indexOf(".txt") > 0)
					flag = true;
				dd.innerHTML += "<tr><td>" + fileName + "</td><td>"
						+ Math.round(fileList[i].size / 1024 * Math.pow(10, 2))
						/ Math.pow(10, 2) + "KB" + "</td></tr>";
			}
			if (flag)
				document.getElementById("selected2").style.display = "";
		}
	}

	function checkAndUpload() {
		var myselect1 = document.getElementById("selected1");
		var myselect2 = document.getElementById("selected2");
		var index1 = myselect1.selectedIndex;
		var index2 = myselect2.selectedIndex;
		var flag = true;
		if (myselect1.options[index1].value == "no") {
			alert("请选择导入人")
			return flag = false;
		}
		if (flag) {
			var url = '/dataplat/filesUpload?person='
					+ myselect1.options[index1].value + '&separator='
					+ myselect2.options[index2].value;
			document.getElementById('myform').action = url;
		}
	}
</script>
</head>
<body>
	<h1>文件导入数据库</h1>
	<form action="" id="myform"  name="myform" method="post" enctype="multipart/form-data"
		onSubmit="return checkAndUpload();">
		<select id="selected1">
			<option value="no">导入人</option>
			<option value="ly">刘宇</option>
			<option value="skm">宋彪</option>
			<option value="ljp">李建鹏</option>
			<option value="lx">李欣</option>
			<option value="xx">谢鑫</option>
		</select> <input type="file" id="input" multiple name="files"> <select
			style="display: none" id="selected2">
			<option value="no">分隔符</option>
			<option value="t">制表符</option>
			<option value="','">逗号</option>
		</select> <input type="submit" id="submit" value="上传" />
		<table align="center" valign="center" border="1px" id='content'></table>
	</form>
	<br>
	<br>
	<input type="reset" name="rest" id="rest" value="重置"
		onclick="window.location.href='upload'">
	<input type="submit" value="返回首页"
		onclick="window.location.href='index.html'" />
</body>
</html>