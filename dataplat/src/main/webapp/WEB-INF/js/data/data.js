var curWwwPath=window.document.location.href;
	var pathName=window.document.location.pathname;
	var pos=curWwwPath.indexOf(pathName);
	var localhostPaht=curWwwPath.substring(0,pos);
	var projectName=pathName.substring(0,pathName.substr(1).indexOf('/')+1); 
$(function(){
//	$("#getdbName").click(function(){
//		var da=$.get(localhostPaht+projectName+"/readDatabase",function(data,status){
//			console.log(data);
//			$("#dbName table").remove();
//			var table=$("<table border=\"1\">");
//			table.appendTo($("#dbName"));
//			for(var i in data.dbnames){
//				var tr=$("<tr><td>"+data.dbnames[i]+"</td></tr>");
//				tr.appendTo(table);
//			}
//			$("#tableNames").append("</table>");
//  });
//		
//	});
	//点击
	$("#tableNames").on('click','td',function(){
		console.log(this.innerHTML);
		var db=	$("#db p").html();
		var table=this.innerHTML;
		$("#currentTable").remove();
		var b=$("<p id='currentTable' hidden>"+this.innerHTML+"</p>");
		b.appendTo($("#db"));
		var row=$("#datas input").val();
		console.log(row);
		var da=$.get(localhostPaht+projectName+"/readDatas",{tableName:db+"."+table,row:row},function(data){
			console.log(data);
			$("#datas table").remove();
			var table=$("<table border=\"1\"><tr>");
			table.appendTo($("#datas"));
			var tr=$("<tr width=10px,height=15px></tr>");
			for(var i in data.columns ){
				td=$("<td>"+data.columns[i]+"<td>");	
				td.appendTo(tr);
			}
			 tr.appendTo(table);
			 
			 for(var i in data.data){
				 var tr1=$("<tr width=10px,height=15px></tr>");
				 for(var j in data.data[i]){
					 td=$("<td>"+data.data[i][j]+"<td>"); 
					 td.appendTo(tr1);
				 }
				 tr1.appendTo(table);
			 }
			$("#datas ").append("</table>");
		});
	});
	
	//获取表
	$("#dbName").on('click','td',function(){
		console.log(this.innerHTML);
		$("#database").remove();
		var b=$("<p id='database' hidden>"+this.innerHTML+"</p>");
		b.appendTo($("#db"));
		var da=$.get(localhostPaht+projectName+"/readTables",{dbName:this.innerHTML},function(data){
			$("#tableNames table").remove();
			var table=$("<table border=\"1\">");
			table.appendTo($("#tableNames"));
			for(var i in data.tables){
				var tr=$("<tr ><td >"+data.tables[i]+"</td></tr>");
				tr.appendTo(table);
			}
			$("#tableNames").append("</table>");
		});
		
	});
	//按钮点击事件获取数据
	$("#button").click(function(){
		var db=	$("#database").html();
		var table=$("#currentTable").html();
		var row=$("#datas input").val();
		var da=$.get(localhostPaht+projectName+"/readDatas",{tableName:db+"."+table,row:row},function(data){
			console.log(data);
			$("#datas table").remove();
			var table=$("<table border=\"1\"><tr>");
			table.appendTo($("#datas"));
			var tr=$("<tr width=10px,height=15px></tr>");
			for(var i in data.columns ){
				td=$("<td>"+data.columns[i]+"<td>");	
				td.appendTo(tr);
			}
			 tr.appendTo(table);
			 for(var i in data.data){
				 var tr1=$("<tr width=10px,height=15px></tr>");
				 for(var j in data.data[i]){
					 td=$("<td>"+data.data[i][j]+"<td>"); 
					 td.appendTo(tr1);
				 }
				 tr1.appendTo(table);
			 }
			$("#datas ").append("</table>");
		});
	});
	
});
//获取数据库
$(function(){ 
	var da=$.get(localhostPaht+projectName+"/readDatabase",function(data,status){
		console.log(data);
		$("#dbName table").remove();
		var table=$("<table border=\"1\">");
		table.appendTo($("#dbName"));
		for(var i in data.dbnames){
			var tr=$("<tr><td>"+data.dbnames[i]+"</td></tr>");
			tr.appendTo(table);
		}
		$("#tableNames").append("</table>");
});
	   
});  
