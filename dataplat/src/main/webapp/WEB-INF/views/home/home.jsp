<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>
<%
	String path = request.getContextPath();
	String basePath = request.getScheme() + "://" + request.getServerName() + ":" + request.getServerPort()
			+ path + "/";
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>数据分析</title>
<link rel="stylesheet" href="<%=basePath%>/css/bootstrap.css" />
<link rel="stylesheet" href="<%=basePath%>/css/sweetalert2.min.css" />
<script type="text/javascript" src="<%=basePath%>/js/jquery-1.8.0.js"></script>
<script type="text/javascript" src="<%=basePath%>/js/jquery.form.js"></script>
<script type="text/javascript" src="<%=basePath%>/js/sweetalert2.min.js"></script>

<!-- plUpload code start -->
<!-- 配置界面上的css -->
<link rel="stylesheet" type="text/css"
	href="<%=basePath%>/js/plupload-2.3.1/jquery.plupload.queue/css/jquery.plupload.queue.css">
<script type="text/javascript"
	src="<%=basePath%>/js/plupload-2.3.1/plupload.full.min.js"></script>
<script type="text/javascript"
	src="<%=basePath%>/js/plupload-2.3.1/jquery.plupload.queue/jquery.plupload.queue.js"></script>

<!-- 国际化中文支持 -->
<script type="text/javascript"
	src="<%=basePath%>/js/plupload-2.3.1/i18n/zh_CN.js"></script>
</head>
<script type="text/javascript">
  $(function() {
	console.log(sessionStorage.getItem("dataplat_accessToken"));
    // Initialize the widget when the DOM is ready
    var uploader = $("#uploader").pluploadQueue({
      // General settings
      runtimes: 'html5,flash,silverlight,html4',
      //Request path
      url: "<%=basePath%>/filesUpload",

      // Maximum file size
      max_file_size: '10000mb',
      headers:{'accessToken':sessionStorage.getItem("dataplat_accessToken")},
      chunk_size: '1mb',
      multipart_params: {'separator':sessionStorage.getItem("dataplat_accessToken")},
      // Resize images on clientside if we can
      resize: {
        width: 1000,
        height: 2000,
        quality: 90,
        crop: true // crop to exact dimensions
      },

      // Specify what files to browse for
      filters: [
        {title : "data files",
		extensions : "txt,xls,xlsx,csv,zip,rar"}
      ],

      // Rename files by clicking on their titles
      rename: true,
      multi_selection : true,
      // Sort files
      sortable: true,

      // Enable ability to drag'n'drop files onto the widget (currently only HTML5 supports that)
      dragdrop: true,

      // Views to activate
      views: {
        list: true,
        thumbs: true, // Show thumbs
        active: 'thumbs'
      },

      // Flash settings
      flash_swf_url: '<%=basePath%>/js/plupload-2.3.1/Moxie.swf',

      // Silverlight settings
      silverlight_xap_url: '<%=basePath%>/js/plupload-2.3.1/Moxie.xap'
    });

    $("#toStop").on('click', function () {
      uploader.stop();
    });

    $("#toStart").on('click', function () {
      uploader.start();
    });
  });
</script>
<body>
	<div id="uploader">
		<p>Your browser doesn't have Flash, Silverlight or HTML5 support.</p>
	</div>
	<button id="toStop" class="btn btn-sm btn-info">暂停</button>
	<button id="toStart" class="btn btn-sm btn-warning">继续</button>
	<button id="btnOk" class="btn btn-sm btn-success">OK</button>
</body>
</html>