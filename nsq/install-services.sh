#!/bin/bash

# NSQ服务安装脚本
# 此脚本将安装NSQ服务并设置开机自启动

echo "开始安装NSQ服务..."

# 检查是否为root用户
if [ "$EUID" -ne 0 ]; then
    echo "请使用root权限运行此脚本"
    exit 1
fi


# 拷贝二进制文件
echo "拷贝二进制文件..."
# 创建nsq目录
if [ ! -d "/usr/share/nsq" ]; then
    echo "创建nsq目录..."
    mkdir -p /usr/share/nsq
fi
cp ./bin/nsqlookupd /usr/share/nsq/
cp ./bin/nsqd /usr/share/nsq/
cp ./bin/nsqadmin /usr/share/nsq/

chmod +x /usr/share/nsq/nsqlookupd
chmod +x /usr/share/nsq/nsqd
chmod +x /usr/share/nsq/nsqadmin

# 复制服务文件到systemd目录
echo "安装服务文件..."
cp nsqlookupd.service /etc/systemd/system/
cp nsqd.service /etc/systemd/system/
cp nsqadmin.service /etc/systemd/system/


# 重新加载systemd配置
echo "重新加载systemd配置..."
systemctl daemon-reload

# 启用服务开机自启动
echo "启用服务开机自启动..."
systemctl enable nsqlookupd.service
systemctl enable nsqd.service
systemctl enable nsqadmin.service


echo "启动服务..."
systemctl restart nsqlookupd nsqd nsqadmin