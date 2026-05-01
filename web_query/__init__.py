# web_query/__init__.py
"""
Web 查询模块
提供 Flask 路由用于前端查询
"""

from .routes import register_routes

__all__ = [
    "register_routes",
]
