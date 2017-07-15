<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>json操作页面</title>
<script type="text/javascript" src="js/jquery.min.js"></script>
<script type="text/javascript" src="js/jquery.autogrow.textarea.js"></script>
<link rel="stylesheet" type="text/css" href="css/sweetalert2.min.css">
<script type="text/javascript" src="js/sweetalert2.min.js"></script>

<script>
var pathName=window.document.location.pathname;
var projectName=pathName.substring(0,pathName.substr(1).indexOf('/')+1);

/**
 * 格式化去日期（含时间）
 */
function formatterDateTime(time) {
	 var date = new Date(time)
    var datetime = date.getFullYear()
            + "-"// "年"
            + ((date.getMonth() + 1) > 10 ? (date.getMonth() + 1) : "0"
                    + (date.getMonth() + 1))
            + "-"// "月"
            + (date.getDate() < 10 ? "0" + date.getDate() : date
                    .getDate())
            + " "
            + (date.getHours() < 10 ? "0" + date.getHours() : date
                    .getHours())
            + ":"
            + (date.getMinutes() < 10 ? "0" + date.getMinutes() : date
                    .getMinutes())
            + ":"
            + (date.getSeconds() < 10 ? "0" + date.getSeconds() : date
                    .getSeconds());
    return datetime;
}
$(function(){
	$("textarea").autogrow();
	readJsonList();
	
	$("#jsonlist").on("click",".jsonListRead",function(){
		$("#updatejsonButton").css('display','none');
		$("#jsondata").attr("disabled","disabled");
		var jsonname = $(this).parent().parent().children().eq(0).html();
		readJsonByJsonName(jsonname);
		
	})
	$("#jsonlist").on("click",".jsonListUpdate",function(){
		$("#jsondata").removeAttr("disabled");
		var jsonname = $(this).parent().parent().children().eq(0).html();
		$("#jsondata").data("jsonName",jsonname);
		readJsonByJsonName(jsonname)
		$("#updatejsonButton").css('display','block');
	})
	$("#jsonlist").on("click",".jsonListDelete",function(){
		var jsonname = $(this).parent().parent().children().eq(0).html();
		swal({
			  title: '确定删除？',
			  text: '删除后不可恢复！',
			  type: 'warning',
			  showCancelButton: true,
			  confirmButtonText: '确定',
			  cancelButtonText: '取消',
			}).then(function(isConfirm) {
			  if (isConfirm === true) {
				  deleteJson(jsonname)
			  }
			}); 
		
		
	})
	
	$("#submitUpdateJson").click(function(){
		var jsonName = $("#jsondata").data("jsonName");
		var data = $("#jsondata").val();
		$.ajax({
			type:"post",
			dataType:"json",
			data:{"jsonName":jsonName,"data":data},
			url:projectName+"/updateJson",
			headers:{"accessToken":sessionStorage.getItem("dataplat_accessToken")},
			success:function(result){
				if (result.code == 1){
					readJsonList();
					$("#updatejsonButton").css('display','none');
					$("#jsondata").attr("disabled","disabled");
					readJsonByJsonName(jsonName);
				} else if (result.code == 428){
					//重新登录
				} else {
					alert(result.failure)
				}
			},
			error:function(){
				console.log("请求失败")
			}
		})
	})
	$("#cancleUpdateJson").click(function(){
		var jsonName = $("#jsondata").data("jsonName");
		$("#updatejsonButton").css('display','none');
		$("#jsondata").attr("disabled","disabled");
		readJsonByJsonName(jsonName);
	})
	
	
})
function readJsonByJsonName(jsonname){
	$("#jsondata").css('display','block');
	$("#jsondata").val("");
	$.ajax({
		type:"get",
		dataType:"json",
		url:projectName+"/readJson",
		data:{"jsonName":jsonname},
		headers:{"accessToken":sessionStorage.getItem("dataplat_accessToken")},
		success:function(result){
			if (result.code == 1){
				$("#jsondata").val(result.data);
			} else if (result.code == 428){
				//重新登录
			} else {
				alert(result.failure)
			}
		},
		error:function(){
			console.log("请求失败")
		}
	})
}
function readJsonList(){
	$.ajax({
		type:"get",
		dataType:"json",
		url:projectName+"/jsonlist",
		headers:{"accessToken":sessionStorage.getItem("dataplat_accessToken")},
		success:function(result){
			if (result.code == 1){
				$("#jsonlist").html("");
				$.each(result.data,function(index,item){
					var str = "<tr><td>"+item.fileName+"</td><td>"+formatterDateTime(item.lastModefied)+"</td><td><button class='jsonListRead'>查看</button><button class='jsonListUpdate'>修改</button><button class='jsonListDelete'>删除</button></td></tr>";
					$("#jsonlist").append(str);
					
				})
			} else if (result.code == 428){
				//重新登录
			} else {
				alert(result.failure)
			}
		},
		error:function(){
			console.log("请求失败")
		}
	})
}
function deleteJson(jsonName){
	$.ajax({
		type:"post",
		dataType:"json",
		data:{"jsonName":jsonName},
		url:projectName+"/deleteJson",
		headers:{"accessToken":sessionStorage.getItem("dataplat_accessToken")},
		success:function(result){
			if (result.code == 1){
				alert("删除成功")
				location.reload();
			} else if (result.code == 428){
				//重新登录
			} else {
				alert(result.failure)
			}
		},
		error:function(){
			console.log("请求失败")
		}
	})
}
</script>
</head>
<body>
<div>
	<table id="jsonListTable">
		<thead>
		<tr>
			<td>
				文件名
			</td>
			<td>
				最后操作时间
			</td>
			<td>
				操作
			</td>
		</tr>
		</thead>
		<tbody id="jsonlist">
		
		</tbody>
	</table>
</div>
	<textarea id="jsondata" style="width: 1639px;height: 765px;display: none;" disabled></textarea>
	<div style="display: none;" id="updatejsonButton">
		<button id="submitUpdateJson">提交</button><button id="cancleUpdateJson">取消</button>
	</div>
</body>
</html>
