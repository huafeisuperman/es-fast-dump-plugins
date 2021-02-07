#重建索引插件

##功能
!(https://doc.qima-inc.com/download/attachments/286348156/image2020-7-28_13-3-16.png?version=1&modificationDate=1595912601000&api=v2)

##API

###1.提交一个重建请求：
POST /fast/index   
请求：请求body如下      
返回：等结束再返回， 默认是等待的，返回值
```java
{
    "completed": true,
    "task": {
        "node": "80I32JrKQrmELeaV-rHTqw",
        "id": 52,
        "type": "transport",
        "action": "indices:fast/index",
        "status": {
            "total_file": 0,
            "success_file": 0,
            "failed_file": 0,
            "process_file": 0,
            "process_info": [
            ],
            "node_rate": 0
        },
        "description": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data->hf_test2",
        "start_time_in_millis": 1595842985813,
        "running_time_in_nanos": 1210412409894,
        "cancellable": true,
        "headers": {
        }
    },
    "response": {
        "node_status": [
            {
                "node_id": "80I32JrKQrmELeaV-rHTqw",
                "file_status": [
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00000-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 37611,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                    ,
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00001-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 37611,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                    ,
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00002-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 37124,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                    ,
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00003-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 37273,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                    ,
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00004-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 37457,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                    ,
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00005-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 37322,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                    ,
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00006-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 37344,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                    ,
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00007-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 36991,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                    ,
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00008-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 36897,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                    ,
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00009-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 37318,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                    ,
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00010-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 9955,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                ],
                "success": true,
                "msg": "ok"
            }
        ],
        "total_count": 382903,
        "total_success": true,
        "total_file": 11,
        "success_file": 11,
        "failed_file": 0,
        "total_rate": 316,
        "cost": 1210446
    }
}
```

POST  /fast/index?wait_for_completion=true   
请求：请求body如下
返回：立即返回  ，返回值为
{
  "task": "80I32JrKQrmELeaV-rHTqw:52"
}
请求body
```
{
    "source": {
        "source_index": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data",                              资源，对hive来说是hdfs地址，对es来说是索引
        "thread_num": 2,                                                                                                 单个节点并发处理的线程数，默认值是1
        "one_file_thread_num":1,                                                                                         单个资源并发处理数，默认值是1
        "batch_size": 100,                                                                                               单次处理的批量大小，默认是1000
        "source_resolver": "hive",                                                                                       源存储，当前支持hive和es
        "speed_limit": 100,                                                                                              总的速度控制，默认值是10万
        "mode": "insert",                                                                                                doc模式，当前支持insert覆盖，create，update带上版本号的                        
        "source_info": {                                                                                                 源存储连接信息
            "ip": "qabb-qa-hdp-hbase0,qabb-qa-hdp-hbase1",                                                               ip地址
            "port": "8020",                                                                                              端口
            "cluster_name": "yz-cluster-qa"                                                                              集群名
        },
        "query": {                                                                                                       查询，只对源存储是es的，当前只支持term，terms，range查询
            "bool": {
                "should": [
                    {
                        "term": {
                            "kkk": "sfds62461"
                        }
                    }
                ]
            }
        },
        "need_fields": "doc_id,rowkey",                                                                                 只针对源储存是hive的，需要获取的字段值
        "primary_key": "rowkey",                                                                                        只针对源储存是hive的，主键
        "nest_fields": "id"                                                                                             只针对源储存是hive的，内嵌字段
    },
    "target": {
        "target_type": "data",                                                                                          针对es,type类型，默认是_doc
        "target_index_type": "all_to_one",                                                                              索引对应模式，all_to_one,all_to_all,one_to_one,custom
        "target_index": "hf_test2",                                                                                     目标资源，对es来说是索引，对hive来说是hdfs路径
        "remote_info": {                                                                                                目标存储连接信息
            "ip": "10.215.20.68",                                                                                       ip地址
            "port": "9200",                                                                                             端口
            "username": "elastic",                                                                                      用户名
            "password": "Ae5k8IHZRkGO4ebIvrAp"                                                                          密码
        },
        "target_resolver": "es"                                                                                         目标存储，当前支持es和hive
    },
    "rules": {                                                                                                          规则当前只对target_index_type为custom是才生效
        "rule_name": "GroovyRule",                                                                                      规则名字，当前支持HashRule，TimeStampRule，GroovyRule
        "field": "record",                                                                                              规则对应的字段
        "rule_value": "def source = record.get('source');source.put('age', 556)"                                        具体的逻辑,当为groovyRule时，脚本暂不可以包含->符号
    }
}
```

###2.更改速度：PUT /fast/index/speed/{id}
请求：请求body无
返回：
```
{
    "node_status": [
        {
            "node_id": "80I32JrKQrmELeaV-rHTqw",
            "is_success": true,
            "message": "ok",
            "node_speed": 200
        }
    ],
    "success": true,
    "took": 2
}
```

###3.查询任务状态：GET _tasks?parent_task_id={task_id}&detailed
请求：请求body无
返回：
```
{
    "nodes": {
        "80I32JrKQrmELeaV-rHTqw": {
            "name": "es-7",
            "transport_address": "127.0.0.1:9302",
            "host": "127.0.0.1",
            "ip": "127.0.0.1:9302",
            "roles": [
                "master"
                ,
                "ingest"
                ,
                "data"
                ,
                "ml"
            ],
            "attributes": {
                "ml.machine_memory": "8589934592",
                "xpack.installed": "true",
                "ml.max_open_jobs": "20"
            },
            "tasks": {
                "80I32JrKQrmELeaV-rHTqw:53": {
                    "node": "80I32JrKQrmELeaV-rHTqw",
                    "id": 53,
                    "type": "direct",
                    "action": "indices:fast/index[s]",
                    "status": {
                        "total_file": 11,
                        "success_file": 10,
                        "failed_file": 0,
                        "process_file": 1,
                        "process_info": [
                            {
                                "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00009-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                                "total_count": 37318,
                                "current_count": 33600,
                                "current_rate": 579
                            }
                        ],
                        "node_rate": 579
                    },
                    "description": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data->hf_test2",
                    "start_time_in_millis": 1595842987377,
                    "running_time_in_nanos": 1205108794997,
                    "cancellable": true,
                    "parent_task_id": "80I32JrKQrmELeaV-rHTqw:52",
                    "headers": {
                    }
                }
            }
        }
    }
}
```

###4.查看结束任务状态：GET  tasks/{task_id}
请求：请求body无
返回：
```
{
    "completed": true,
    "task": {
        "node": "80I32JrKQrmELeaV-rHTqw",
        "id": 52,
        "type": "transport",
        "action": "indices:fast/index",
        "status": {
            "total_file": 0,
            "success_file": 0,
            "failed_file": 0,
            "process_file": 0,
            "process_info": [
            ],
            "node_rate": 0
        },
        "description": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data->hf_test2",
        "start_time_in_millis": 1595842985813,
        "running_time_in_nanos": 1210412409894,
        "cancellable": true,
        "headers": {
        }
    },
    "response": {
        "node_status": [
            {
                "node_id": "80I32JrKQrmELeaV-rHTqw",
                "file_status": [
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00000-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 37611,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                    ,
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00001-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 37611,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                    ,
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00002-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 37124,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                    ,
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00003-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 37273,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                    ,
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00004-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 37457,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                    ,
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00005-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 37322,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                    ,
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00006-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 37344,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                    ,
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00007-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 36991,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                    ,
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00008-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 36897,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                    ,
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00009-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 37318,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                    ,
                    {
                        "file_path": "/user/hive/warehouse/dev.db/scrm_customer_aggregation_error_data/part-00010-de23dbfd-def8-4d53-9523-906b5c16a854-c000",
                        "total_count": 9955,
                        "status": "SUCCESS",
                        "msg": "ok"
                    }
                ],
                "success": true,
                "msg": "ok"
            }
        ],
        "total_count": 382903,
        "total_success": true,
        "total_file": 11,
        "success_file": 11,
        "failed_file": 0,
        "total_rate": 316,
        "cost": 1210446
    }
}
```

##插件配置
单独给重建用的线程池大小：thread_pool.fast_reindex.size  默认是4
单独给重建用的线程池队列长度：thread_pool.fast_reindex.queue_size  默认是10

##功能扩展
1.delete和update的功能
2.对于失败的资源，可以指定失败资源进行处理等

##风险点
1.一些未知的异常，导致es问题，oom堆栈溢出等
2.线程池大小设置太大，并发任务多的话，会有性能影响
3.队列长度也不能太长，不然可能占用内存
