import os
import sys

# 确保项目根目录在 Python 路径中
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask
from dotenv import load_dotenv

from logger import setup_logging, get_logger
from web_query import register_routes
from neo4j_tools import Neo4jConnection

# 加载环境变量
load_dotenv()

# 初始化日志
setup_logging()
logger = get_logger(__name__)

# 创建 Flask 应用
app = Flask(__name__)

# 注册路由
register_routes(app)


def main():
    """应用主入口"""
    logger.info("=" * 50)
    logger.info("知识图谱查询系统")
    logger.info("=" * 50)
    
    # 验证 Neo4j 连接
    if not Neo4jConnection.verify_connection():
        logger.warning("Neo4j 连接失败，部分功能可能不可用")
    
    try:
        # 启动 Flask 服务
        logger.info("启动 Flask 服务 (0.0.0.0:5000)")
        app.run(host="0.0.0.0", port=5000, debug=True)
    finally:
        # 关闭数据库连接
        Neo4jConnection.close()


if __name__ == "__main__":
    main()
