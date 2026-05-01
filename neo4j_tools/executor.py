# neo4j_tools/executor.py
"""
Cypher 执行器
执行 LLM 生成的非固定 Cypher 语句
"""

import os
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from .connection import Neo4jConnection


@dataclass
class CypherResult:
    """Cypher 执行结果"""
    success: bool
    records: List[Dict[str, Any]]
    error: Optional[str] = None
    nodes: List[Dict[str, Any]] = None
    links: List[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.nodes is None:
            self.nodes = []
        if self.links is None:
            self.links = []


class CypherExecutor:
    """
    Cypher 执行器
    执行 LLM 生成的 Cypher 语句并格式化结果
    """
    
    def __init__(self):
        """初始化执行器"""
        pass
    
    def execute(self, cypher: str) -> CypherResult:
        """
        执行 Cypher 语句
        
        Args:
            cypher: Cypher 查询语句
            
        Returns:
            CypherResult 对象
        """
        if not cypher or not cypher.strip():
            return CypherResult(
                success=False,
                records=[],
                error="Empty Cypher statement",
            )
        
        try:
            with Neo4jConnection.get_session() as session:
                result = session.run(cypher)
                records = [dict(record) for record in result]
                
            # 解析节点和关系用于可视化
            nodes, links = self._extract_graph_data(records)
            
            return CypherResult(
                success=True,
                records=records,
                nodes=nodes,
                links=links,
            )
            
        except Exception as e:
            return CypherResult(
                success=False,
                records=[],
                error=str(e),
            )
    
    def execute_batch(self, statements: List[str]) -> List[CypherResult]:
        """
        批量执行 Cypher 语句
        
        Args:
            statements: Cypher 语句列表
            
        Returns:
            CypherResult 列表
        """
        return [self.execute(stmt) for stmt in statements]
    
    def execute_write(self, cypher: str, parameters: Dict[str, Any] = None) -> CypherResult:
        """
        执行写入操作的 Cypher 语句
        
        Args:
            cypher: Cypher 写入语句
            
        Returns:
            CypherResult 对象
        """
        if not cypher or not cypher.strip():
            return CypherResult(
                success=False,
                records=[],
                error="Empty Cypher statement",
            )
        
        try:
            with Neo4jConnection.get_session() as session:
                result = session.run(cypher, parameters or {})
                summary = result.consume()
                
            return CypherResult(
                success=True,
                records=[{
                    "nodes_created": summary.counters.nodes_created,
                    "relationships_created": summary.counters.relationships_created,
                    "properties_set": summary.counters.properties_set,
                }],
            )
            
        except Exception as e:
            return CypherResult(
                success=False,
                records=[],
                error=str(e),
            )
    
    def _extract_graph_data(
        self,
        records: List[Dict[str, Any]],
    ) -> tuple:
        """
        从查询结果中提取节点和关系用于可视化
        
        Args:
            records: 查询结果记录
            
        Returns:
            (nodes, links) 元组
        """
        nodes_dict = {}
        links = []
        links_set = set()  # 用于关系去重 (source_id, target_id)
        
        def add_node(node) -> Optional[int]:
            """添加节点到字典"""
            if node is None:
                return None
            
            # 获取节点属性
            properties = dict(node)
            # 兼容逻辑：优先用 name 作为 ID，如果没有则尝试 element_id
            node_id = properties.get("name")
            if not node_id:
                if hasattr(node, "element_id"):  # Neo4j 5.x
                     node_id = str(node.element_id)
                elif hasattr(node, "id"):  # Neo4j 4.x
                     node_id = str(node.id)
                else:
                    node_id = "Unknown_" + str(len(nodes_dict))
            
            if node_id in nodes_dict:
                return nodes_dict[node_id]["id"]
            
            # 获取标签
            labels = list(node.labels) if hasattr(node, "labels") else ["Unknown"]
            
            # 分配新 ID
            new_id = len(nodes_dict)
            nodes_dict[node_id] = {
                "id": new_id,
                "name": properties.get("name", node_id),
                "value": properties.get("name", node_id),
                "label": labels[0] if labels else "Unknown",  # 单数形式，兼容 graph.py
                "labels": labels,  # 复数形式，保留完整信息
            }
            return new_id
        
        for record in records:
            for key, value in record.items():
                # 处理路径对象
                if hasattr(value, "nodes") and hasattr(value, "relationships"):
                    # 添加路径中的所有节点
                    for node in value.nodes:
                        add_node(node)
                    
                    # 添加路径中的所有关系（去重）
                    for rel in value.relationships:
                        start_id = add_node(rel.start_node)
                        end_id = add_node(rel.end_node)
                        
                        if start_id is not None and end_id is not None:
                            link_key = (start_id, end_id)
                            if link_key not in links_set:
                                links_set.add(link_key)
                                links.append({
                                    "source": start_id,
                                    "target": end_id,
                                    "value": rel.type,
                                })
                
                # 处理单个节点
                elif hasattr(value, "labels"):
                    add_node(value)
                
                # 处理单个关系
                elif hasattr(value, "type") and hasattr(value, "start_node"):
                    start_id = add_node(value.start_node)
                    end_id = add_node(value.end_node)
                    
                    if start_id is not None and end_id is not None:
                        link_key = (start_id, end_id)
                        if link_key not in links_set:
                            links_set.add(link_key)
                            links.append({
                                "source": start_id,
                                "target": end_id,
                                "value": value.type,
                            })
                
                # 处理列表（如 collect() 聚合结果）
                elif isinstance(value, list):
                    for item in value:
                        if item is None:
                            continue
                        # 列表中的路径对象（collect(p) 返回路径列表）
                        if hasattr(item, "nodes") and hasattr(item, "relationships"):
                            for node in item.nodes:
                                add_node(node)
                            for rel in item.relationships:
                                start_id = add_node(rel.start_node)
                                end_id = add_node(rel.end_node)
                                if start_id is not None and end_id is not None:
                                    link_key = (start_id, end_id)
                                    if link_key not in links_set:
                                        links_set.add(link_key)
                                        links.append({
                                            "source": start_id,
                                            "target": end_id,
                                            "value": rel.type,
                                        })
                        # 列表中的节点
                        elif hasattr(item, "labels"):
                            add_node(item)
                        # 列表中的关系
                        elif hasattr(item, "type") and hasattr(item, "start_node"):
                            start_id = add_node(item.start_node)
                            end_id = add_node(item.end_node)
                            if start_id is not None and end_id is not None:
                                link_key = (start_id, end_id)
                                if link_key not in links_set:
                                    links_set.add(link_key)
                                    links.append({
                                        "source": start_id,
                                        "target": end_id,
                                        "value": item.type,
                                    })
        
        # 方案C：关系补全查询
        # 如果提取到节点但关系较少，执行额外查询获取这些节点之间的所有关系
        if nodes_dict and len(links) < len(nodes_dict):
            try:
                node_names = list(nodes_dict.keys())
                with Neo4jConnection.get_session() as session:
                    result = session.run("""
                        MATCH (a)-[r:LINKS]->(b)
                        WHERE a.name IN $names AND b.name IN $names
                        RETURN a.name as source, b.name as target, type(r) as rel_type
                    """, names=node_names)
                    
                    # 用集合去重
                    existing_links = {(l["source"], l["target"]) for l in links}
                    
                    for record in result:
                        src_name = record["source"]
                        tgt_name = record["target"]
                        if src_name in nodes_dict and tgt_name in nodes_dict:
                            src_id = nodes_dict[src_name]["id"]
                            tgt_id = nodes_dict[tgt_name]["id"]
                            if (src_id, tgt_id) not in existing_links:
                                links.append({
                                    "source": src_id,
                                    "target": tgt_id,
                                    "value": record["rel_type"],
                                })
                                existing_links.add((src_id, tgt_id))
            except Exception as e:
                # logger.warning(f"关系补全查询失败: {e}")
                pass
        
        return list(nodes_dict.values()), links
