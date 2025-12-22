#!/bin/bash
# 江鑫数据报表 API 部署脚本
# 适用于 Alibaba Cloud Linux 3.2104 LTS 64位

set -e

echo "=========================================="
echo "江鑫数据报表 API 部署脚本"
echo "=========================================="

# 检查 Docker 是否安装
if ! command -v docker &> /dev/null; then
    echo "Docker 未安装，正在安装..."

    # 安装 Docker
    sudo yum install -y yum-utils
    sudo yum-config-manager --add-repo https://mirrors.aliyun.com/docker-ce/linux/centos/docker-ce.repo
    sudo yum install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

    # 启动 Docker
    sudo systemctl start docker
    sudo systemctl enable docker

    echo "Docker 安装完成"
fi

# 检查 docker-compose 是否可用
if ! docker compose version &> /dev/null; then
    echo "docker compose 不可用，请检查 Docker 安装"
    exit 1
fi

# 创建报表输出目录
mkdir -p reports

# 构建并启动服务
echo "正在构建并启动服务..."
docker compose up -d --build

# 等待服务启动
echo "等待服务启动..."
sleep 5

# 检查服务状态
if curl -s http://localhost:8000/health | grep -q "ok"; then
    echo "=========================================="
    echo "部署成功！"
    echo "=========================================="
    echo ""
    echo "API 地址: http://$(hostname -I | awk '{print $1}'):8000"
    echo "API 文档: http://$(hostname -I | awk '{print $1}'):8000/docs"
    echo ""
    echo "接口列表:"
    echo "  POST /api/report/daily   - 生成日报"
    echo "  POST /api/report/weekly  - 生成周报"
    echo "  POST /api/report/monthly - 生成月报"
    echo "  POST /api/report/custom  - 生成自定义报表"
    echo ""
    echo "查看日志: docker compose logs -f"
    echo "停止服务: docker compose down"
    echo "=========================================="
else
    echo "服务启动失败，请检查日志:"
    docker compose logs
    exit 1
fi
