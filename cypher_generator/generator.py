import os
import re
from typing import Optional, Tuple, List

from openai import OpenAI

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import CYPHER_LLM
from logger import get_logger
import importlib
from . import prompts

logger = get_logger(__name__)


class CypherGenerator:
    """
    Cypher generator.
    Converts natural language into Cypher queries.
    """
    DEFAULT_FALLBACK = "MATCH p = ()-[:LINKS]->() RETURN p LIMIT 100"
    
    # 缓存
    _labels_cache: Optional[str] = None
    _samples_cache: Optional[str] = None
    
    def __init__(self):
        """初始化生成器"""
        self.client = OpenAI(
            base_url=CYPHER_LLM.base_url,
            api_key=CYPHER_LLM.api_key,
        )
        self.model = CYPHER_LLM.model
        logger.info(f"初始化 Cypher 生成器，使用模型: {self.model}")
    
    def generate(
        self,
        query: str,
        available_labels: Optional[str] = None,
        sample_data: Optional[str] = None,
        original_query: Optional[str] = None,
    ) -> str:
        """
        将自然语言转换为 Cypher 查询
        
        Args:
            query: 当前步骤的自然语言描述 (或完整问题)
            available_labels: 可用标签列表字符串
            sample_data: 图谱示例数据字符串
            original_query: 原始用户问题 (用于 Agent 模式提供全局上下文)
            
        Returns:
            Cypher 查询语句
        """
        # 获取上下文信息（使用缓存）
        if available_labels is None:
            available_labels = self._get_available_labels()
        
        if sample_data is None:
            sample_data = self._get_sample_data()
        
        # 构建 Prompt
        # 如果提供了原始问题，将其整合到 prompt 中
        prompt_query = query
        if original_query and original_query != query:
            prompt_query = f"当前查询步骤: {query}\n\n原始用户问题 (Context): {original_query}"

        prompt = prompts.CYPHER_GENERATION_PROMPT.format(
            available_labels=available_labels,
            sample_data=sample_data,
            user_query=prompt_query,
        )
        
        # 调用 LLM
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0,  # 零温度以获得确定性输出
            )
            
            cypher = response.choices[0].message.content.strip()
            
            # 清理输出
            cypher = self._clean_cypher(cypher)

            if not cypher:
                return self.DEFAULT_FALLBACK

            return cypher
            
        except Exception as e:
            logger.error(f"Cypher 生成失败: {e}")
            # 返回默认查询
            return self.DEFAULT_FALLBACK
    
    def generate_with_retry(
        self,
        query: str,
        error_message: str,
        previous_cypher: str,
        max_retries: int = 2,
    ) -> str:
        """
        根据执行错误重新生成 Cypher
        
        Args:
            query: 原始用户问题
            error_message: Cypher 执行错误信息
            previous_cypher: 之前生成的 Cypher
            max_retries: 最大重试次数
            
        Returns:
            修正后的 Cypher 查询语句
        """
        retry_prompt = f"""之前生成的 Cypher 执行失败，请修正。

## 原始问题
{query}

## 之前生成的 Cypher
{previous_cypher}

## 执行错误
{error_message}

## 请求
请分析错误原因，生成修正后的 Cypher 语句。
只输出纯 Cypher 语句，不要加注释或解释。
如果无法修正，返回一个简单的查询：MATCH (n) RETURN n LIMIT 10
"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": retry_prompt}
                ],
                temperature=0.0,
            )
            
            cypher = response.choices[0].message.content.strip()
            cypher = self._clean_cypher(cypher)
            return cypher if cypher else "MATCH (n) RETURN n LIMIT 10"
            
        except Exception as e:
            logger.error(f"重试生成 Cypher 失败: {e}")
            return "MATCH (n) RETURN n LIMIT 10"
    
    def _get_available_labels(self) -> str:
        """
        获取可用标签
        
        Returns:
            标签列表字符串
            
        Note:
            从 labels.json 读取，由第一阶段 kg_extractor 维护
        """
        # 使用缓存
        if CypherGenerator._labels_cache is not None:
            return CypherGenerator._labels_cache
        
        try:
            from kg_extractor import LabelManager
            label_manager = LabelManager()
            labels = label_manager.get_labels()
            result = ", ".join(sorted(labels)) if labels else self._get_default_labels()
            CypherGenerator._labels_cache = result
            return result
        except Exception as e:
            logger.warning(f"从 labels.json 获取标签失败: {e}")
            return self._get_default_labels()
    
    def _get_sample_data(self) -> str:
        """
        获取示例数据
        
        Returns:
            示例数据字符串
            
        Note:
            从 Neo4j 数据库中查询前 10 个节点的名称和标签，
            作为上下文提供给 LLM，帮助其更好地生成 Cypher
        """
        # 使用缓存
        if CypherGenerator._samples_cache is not None:
            return CypherGenerator._samples_cache
        
        try:
            from neo4j_tools.connection import Neo4jConnection
            samples = Neo4jConnection.get_sample_data(limit=5)
            if not samples:
                return "暂无数据"
            
            lines = []
            for s in samples:
                lines.append(f"- {s['name']} ({s['label']})")
            result = "\n".join(lines)
            CypherGenerator._samples_cache = result
            return result
        except Exception as e:
            logger.warning(f"从 Neo4j 获取示例数据失败: {e}")
            return "暂无数据"
    
    def _get_default_labels(self) -> str:
        """获取默认标签"""
        try:
            from kg_extractor import LabelManager
            return ", ".join(sorted(LabelManager.DEFAULT_LABELS))
        except ImportError:
            return "zeolite, propertyCategory, propertyNode, activeSite, reactionNode, molecular"
    @staticmethod
    def split_statements(cypher_text: str) -> List[str]:
        """
        Clean LLM output and split into individual Cypher statements.
        """
        if not cypher_text:
            return []

        text = cypher_text.strip()

        # Remove code fences
        text = re.sub(r'^```\w*\s*', '', text)
        text = re.sub(r'```\s*$', '', text)

        # Remove full-line comments
        lines = []
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith('//') or stripped.startswith('#') or stripped.startswith('--'):
                continue
            lines.append(line)

        text = '\n'.join(lines)

        statements = []
        current = []
        in_single = False
        in_double = False
        escape = False

        for ch in text:
            if ch == '\\' and (in_single or in_double):
                escape = not escape
                current.append(ch)
                continue
            if ch == "'" and not in_double and not escape:
                in_single = not in_single
            elif ch == '"' and not in_single and not escape:
                in_double = not in_double

            if ch == ';' and not in_single and not in_double:
                stmt = ''.join(current).strip()
                if stmt:
                    statements.append(stmt)
                current = []
            else:
                current.append(ch)

            if escape and ch != '\\':
                escape = False

        last = ''.join(current).strip()
        if last:
            statements.append(last)

        return statements

    def _clean_cypher(self, cypher: str) -> str:
        """
        Clean LLM output and return a single Cypher string.
        """
        statements = self.split_statements(cypher)
        return '; '.join(statements).strip()

    def refresh_cache(self) -> None:
        # Cache Prompt ??
        importlib.reload(prompts)
        CypherGenerator._labels_cache = None
        CypherGenerator._samples_cache = None
        logger.info("??????Prompts ???")

    def generate_with_context(self, query: str) -> dict:
        """
        ?? Cypher ????????

        Args:
            query: ????

        Returns:
            ?? cypher, labels, samples ???
        """
        labels = self._get_available_labels()
        samples = self._get_sample_data()
        cypher = self.generate(query, labels, samples)

        return {
            "cypher": cypher,
            "available_labels": labels,
            "sample_data": samples,
            "user_query": query,
        }
