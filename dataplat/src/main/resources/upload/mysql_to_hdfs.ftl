{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "mysqlreader", 
                    "parameter": {
                        "column": ["${mysqlColumn},'${inputPerson}'"], 
                        "connection": [
                            {
                                "jdbcUrl": ["jdbc:mysql://192.168.0.124:3306/${database}?useUnicode=true&characterEncoding=UTF-8"], 
                                "table": ["${table}"]
                            }
                        ], 
                        "password": "root", 
                        "username": "root", 
                        "where": ""
                    }
                   
                }, 
                "writer": {
                    "name": "hdfswriter", 
                    "parameter": {
                        "column": [${hdfsColumn},{"name":"inputPerson","type":"string"}], 
                        "defaultFS": "hdfs://172.20.100.10:9000", 
                        "fieldDelimiter": "$#$", 
                        "fileName": "${type}", 
                        "fileType": "text", 
                        "path": "/warehouse_original/${index}/${type}/${date}/", 
                        "writeMode": "append"
                    }
                }
            }
        ],
        "setting": {
            "speed": {
                "channel": "10"
            }
        }
    }
}