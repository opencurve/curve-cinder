# 说明
适配社区cinder ussusi版本，实际上其他版本理论上也支持。

# 功能支持列表
- 创建
- 扩容
- 删除
- 挂载
- 卸载
- 创建快照
- 从快照创建卷

# 安装
- 在cinder-volume节点安装curve-sdk，参考[curve-sdk部署](https://github.com/opencurve/curve/blob/master/docs/cn/deploy.md)
- 安装curvefs for python sdk，whl包[下载](https://github.com/opencurve/curve/releases/download/v1.2.5/curvefs-1.2.5+2c4861ca-py2-none-any.whl)
- 将curve目录复制到cinder/volumes/drivers目录中，并重启cinder-volume

# 配置
你需要修改cinder.conf以使能curve后端，参考配置如下：

```
enabled_backends = curve

[curve]
volume_driver = cinder.volume.drivers.curve.CBDDriver
curve_client_conf = /etc/curve/client.conf
cbd_snapshot_api_server = 10.182.14.24:5555
```

同时需要创建对应的volume type并设置好extra spec
```
$ cinder type-create curve
$ cinder type-key curve set volume_backend_name=CURVE
```

# 使用示例
创建一个大小为10G的curve卷
```
$ cinder create --display-name curve-test-01 --volume-type curve 10
```

# 配置项说明

配置项| 默认值 | 含义
---|---|---
curve_client_conf | /etc/curve/client.conf | curve使用的client配置文件
curve_user | cinder | 在curve后端创建卷时采用的用户名
volume_dir | cinder | 卷创建所在的目录
curve_pool | pool1  | cinder使用的curve存储池
cbd_snapshot_api_server | 127.0.0.1:5555 | curve快照克隆服务器的地址
cbd_snapshot_prefix_url | SnapshotCloneService | 固定URL请求前缀
wait_cbd_task_timeout | 600 | 快照/从快照恢复任务的默认超时时间
