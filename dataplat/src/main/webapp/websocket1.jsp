<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>Insert title here</title>
</head>
<body>

	<div>
		<div id="connect-container">
			<input id="radio1" type="radio" name="group1"
				onclick="updateUrl('/socket/myHandler');"> <label
				for="radio1">W3C WebSocket</label> <br> <input id="radio2"
				type="radio" name="group1"
				onclick="updateUrl('/socket/sockjs/myHandler');"> <label
				for="radio2">SockJS</label>
			<div>
				<button id="connect" onclick="connect();">链接</button>
				<button id="disconnect" disabled="disabled" onclick="disconnect();">断开</button>
			</div>
			<div>
				<textarea id="message" style="width: 350px" placeholder="输入要发送的消息"></textarea>
			</div>
			<div>
				<button id="echo" onclick="echo();" disabled="disabled">发送消息</button>
			</div>
		</div>
		<div id="console-container">
			<div id="console"></div>
		</div>
	</div>
</body>

<script src="http://cdn.sockjs.org/sockjs-0.3.min.js"></script>
<script type="text/javascript">
    var ws = null;
    var url = null;
    var transports = [];

    function setConnected(connected) {
      document.getElementById('connect').disabled = connected;
      document.getElementById('disconnect').disabled = !connected;
      document.getElementById('echo').disabled = !connected;
    }

    //修改url，提供了两种url一种是wbsocket的一种是sockjs的
    function updateUrl(urlPath) {
        //如果链接中有sockjs字段就让链接等于传进来的本身
      if (urlPath.indexOf('sockjs') != -1) {
        url = urlPath;
      }
      else {
        if (window.location.protocol == 'http:') {
          url = 'ws://' + window.location.host + urlPath;
        } else {
          url = 'wss://' + window.location.host + urlPath;
        }
      }
    }


    //链接握手
      function connect() {
      if (!url) {
        alert('选择一个url');
        return;
      }
        //判断链接中是否有sockjs，如果有使用sockjs的方式拼链接，如果没有发送ws链接
      ws = (url.indexOf('sockjs') != -1) ? 
      //new SockJS(url, _reserved, options);默认三个参数，中间的基本不用，最后一个是sockjs提供的传输功能，参数是数组（默认是全部开启）
       new SockJS(url) : new WebSocket(url);
       //打开链接
      ws.onopen = function () {
        setConnected(true);
        log('消息: 链接已打开');
      };
      //获取消息
      ws.onmessage = function (event) {
      //调用下面的现实信息的方法
        log('推送的消息: ' + event.data);
      };
      //关闭链接
      ws.onclose = function (event) {
        setConnected(false);
         //调用下面的现实信息的方法
        log('消息: 链接已关闭');
        log(event);
      };
    }

    //发送消息
    function echo() {
      if (ws != null) {
      //获取到输入框中的消息
        var message = document.getElementById('message').value;
         //调用下面的现实信息的方法
        log('发送: ' + message);
        ws.send(message);
      } else {
        alert('消息没有链接地址，请重新连接');
      }
    }

    //将传输回来的信息显示在右侧
    function log(message) {
      var console = document.getElementById('console');
      var p = document.createElement('p');
      p.style.wordWrap = 'break-word';
      p.appendChild(document.createTextNode(message));
      console.appendChild(p);
      //防止消息过长干div外面去
      while (console.childNodes.length > 25) {
        console.removeChild(console.firstChild);
      }
      //console.scrollTop = console.scrollHeight;
    }


    //关闭链接的时候将ws链接清空
    function disconnect() {
      if (ws != null) {
        ws.close();
        ws = null;
      }
      setConnected(false);
    }
  </script>
</html>