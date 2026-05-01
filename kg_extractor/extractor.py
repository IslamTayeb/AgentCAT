# kg_extractor/extractor.py
import asyncio
import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass

from openai import OpenAI, APIError, APIConnectionError, RateLimitError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
from tqdm import tqdm

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import EXTRACTOR_LLM
from logger import get_logger
from .prompts import EXTRACTION_PROMPT
from .label_manager import LabelManager
from .molecular_normalizer import MolecularNormalizer
from .pdf_id_manager import PdfIdManager, ZeoliteRegistry

logger = get_logger(__name__)


@dataclass
class Relation:
    """关系数据类"""
    source_entity: str
    source_label: str
    target_entity: str
    target_label: str
    
    def to_line(self) -> str:
        """转换为行格式"""
        return f"{self.source_entity}:{self.source_label},{self.target_entity}:{self.target_label}"
    
    @classmethod
    def from_line(cls, line: str) -> Optional["Relation"]:
        """从行格式解析"""
        line = line.strip()
        if not line or line.startswith("NEW_LABEL:"):
            return None
            
        parts = line.split(",", 1)
        if len(parts) != 2:
            return None
            
        source_parts = parts[0].rsplit(":", 1)
        target_parts = parts[1].rsplit(":", 1)
        
        if len(source_parts) != 2 or len(target_parts) != 2:
            return None
            
        return cls(
            source_entity=source_parts[0].strip(),
            source_label=source_parts[1].strip(),
            target_entity=target_parts[0].strip(),
            target_label=target_parts[1].strip(),
        )


@dataclass
class ExtractionResult:
    """提取结果数据类"""
    json_path: str
    output_path: Optional[str]
    relations_count: int
    new_labels: List[str]
    warnings: List[str]
    zeolites: List[str]
    pdf_id: Optional[int]
    success: bool
    error: Optional[str] = None


class ReactionNetworkExtractor:
    """
    反应网络提取器
    使用 LLM 从 JSON 中提取反应网络关系
    
    增强功能：
    - 错误重试机制（指数退避）
    - 批量处理进度条
    - 异步并发处理
    - PDF ID 管理
    """
    
    # 默认重试配置
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_MIN_WAIT = 2  # 秒
    DEFAULT_MAX_WAIT = 30  # 秒
    
    def __init__(
        self,
        label_manager: Optional[LabelManager] = None,
        normalizer: Optional[MolecularNormalizer] = None,
        pdf_id_manager: Optional[PdfIdManager] = None,
        zeolite_registry: Optional[ZeoliteRegistry] = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ):
        """
        初始化提取器
        
        Args:
            label_manager: Label 管理器实例
            normalizer: 分子校验器实例
            pdf_id_manager: PDF ID 管理器实例
            zeolite_registry: Zeolite 注册表实例
            max_retries: LLM 调用最大重试次数
        """
        self.label_manager = label_manager or LabelManager()
        self.normalizer = normalizer or MolecularNormalizer()
        self.pdf_id_manager = pdf_id_manager or PdfIdManager()
        self.zeolite_registry = zeolite_registry or ZeoliteRegistry()
        self.max_retries = max_retries
        
        # 初始化 LLM 客户端
        self.client = OpenAI(
            base_url=EXTRACTOR_LLM.base_url,
            api_key=EXTRACTOR_LLM.api_key,
        )
        self.model = EXTRACTOR_LLM.model
        logger.info(f"初始化提取器，使用模型: {self.model}")
    
    def _call_llm_with_retry(self, prompt: str) -> str:
        """
        带重试机制的 LLM 调用
        
        使用 tenacity 实现指数退避重试策略
        """
        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=self.DEFAULT_MIN_WAIT, max=self.DEFAULT_MAX_WAIT),
            retry=retry_if_exception_type((APIError, APIConnectionError, RateLimitError, ConnectionError)),
            before_sleep=before_sleep_log(logger, log_level=30),  # WARNING level
            reraise=True,
        )
        def _call():
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
            )
            return response.choices[0].message.content.strip()
        
        return _call()
    
    def extract_from_json(self, json_content: str) -> Tuple[List[Relation], List[str], List[str]]:
        """
        从 JSON 内容中提取关系
        
        Args:
            json_content: JSON 字符串
            
        Returns:
            (关系列表, 新标签列表, 化学式校验警告列表)
        """
        # 获取现有标签
        existing_labels = self.label_manager.get_labels_str()
        
        # 构建 Prompt
        prompt = EXTRACTION_PROMPT.format(
            existing_labels=existing_labels,
            json_content=json_content,
        )
        
        logger.debug(f"Prompt 长度: {len(prompt)} 字符")
        
        # 调用 LLM（带重试）
        try:
            logger.info("调用 LLM 进行关系提取...")
            output_text = self._call_llm_with_retry(prompt)
            logger.debug(f"LLM 响应长度: {len(output_text)} 字符")
            
        except Exception as e:
            logger.error(f"调用 LLM 失败（已重试 {self.max_retries} 次）: {e}")
            return [], [], []
        
        # 解析输出
        return self._parse_output(output_text)
    
    def extract_from_file(self, json_path: str) -> Tuple[List[Relation], List[str], List[str]]:
        """
        从 JSON 文件中提取关系
        
        Args:
            json_path: JSON 文件路径
            
        Returns:
            (关系列表, 新标签列表, 化学式校验警告列表)
        """
        with open(json_path, "r", encoding="utf-8") as f:
            json_content = f.read()
        
        return self.extract_from_json(json_content)
    
    def _parse_output(self, output: str) -> Tuple[List[Relation], List[str], List[str]]:
        """
        Parse LLM output into relations, new labels, and formula warnings.
        """
        relations = []
        new_labels = []
        formula_warnings = []
        seen = set()

        existing_labels = set(self.label_manager.get_labels())

        def _strip_quotes(value: str) -> str:
            value = value.strip()
            if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
                return value[1:-1].strip()
            return value

        for line in output.strip().split("\n"):
            line = line.strip()

            if not line:
                continue

            # ???????????
            if line.startswith("NEW_LABEL:"):
                parts = line.split(":", 2)
                if len(parts) >= 2:
                    label_name = parts[1].strip()
                    if label_name and label_name not in new_labels:
                        new_labels.append(label_name)
                continue

            # ??????
            relation = Relation.from_line(line)
            if relation:
                relation.source_entity = _strip_quotes(relation.source_entity)
                relation.target_entity = _strip_quotes(relation.target_entity)

                # ????
                src_label = self.label_manager.normalize_label(relation.source_label)
                tgt_label = self.label_manager.normalize_label(relation.target_label)
                if not src_label or not tgt_label:
                    continue
                relation.source_label = src_label
                relation.target_label = tgt_label

                # ??????????????? labels.json
                for lbl in (src_label, tgt_label):
                    if lbl not in existing_labels and lbl not in new_labels:
                        new_labels.append(lbl)
                        existing_labels.add(lbl)

                # ???????????
                if relation.source_label == "molecular":
                    relation.source_entity = self.normalizer.normalize_name(relation.source_entity)
                    is_valid, warning = self.normalizer.validate_and_warn(relation.source_entity)
                    if not is_valid and warning:
                        formula_warnings.append(warning)

                if relation.target_label == "molecular":
                    relation.target_entity = self.normalizer.normalize_name(relation.target_entity)
                    is_valid, warning = self.normalizer.validate_and_warn(relation.target_entity)
                    if not is_valid and warning:
                        formula_warnings.append(warning)

                key = relation.to_line()
                if key not in seen:
                    seen.add(key)
                    relations.append(relation)

        return relations, new_labels, formula_warnings


    def _extract_zeolites(self, relations: List[Relation]) -> List[str]:
        """从关系列表中提取所有 zeolite 实体名称"""
        zeolites = set()
        for rel in relations:
            if rel.source_label == "zeolite":
                zeolites.add(rel.source_entity)
            if rel.target_label == "zeolite":
                zeolites.add(rel.target_entity)
        return list(zeolites)
    
    def extract_and_save(
        self,
        json_path: str,
        output_path: Optional[str] = None,
        add_new_labels: bool = True,
    ) -> ExtractionResult:
        """
        提取关系并保存到文件
        
        Args:
            json_path: 输入 JSON 文件路径
            output_path: 输出文件路径，默认与输入同目录
            add_new_labels: 是否自动添加新标签
            
        Returns:
            ExtractionResult 对象
        """
        json_path_obj = Path(json_path)
        
        # 从路径提取 PDF 名称（使用父文件夹名）
        pdf_name = json_path_obj.parent.name
        
        try:
            # 获取或创建 PDF ID
            pdf_id = self.pdf_id_manager.get_or_create_id(pdf_name)
            
            # 提取关系
            relations, new_labels, warnings = self.extract_from_file(json_path)
            
            if not relations:
                return ExtractionResult(
                    json_path=json_path,
                    output_path=None,
                    relations_count=0,
                    new_labels=new_labels,
                    warnings=warnings,
                    zeolites=[],
                    pdf_id=pdf_id,
                    success=False,
                    error="未提取到任何关系",
                )
            
            # 输出化学式校验警告
            if warnings:
                logger.warning(f"化学式校验警告 ({len(warnings)} 条)")
                for w in warnings[:5]:
                    logger.warning(f"  - {w}")
                if len(warnings) > 5:
                    logger.warning(f"  ... 还有 {len(warnings) - 5} 条警告")
            
            # 处理新标签
            if add_new_labels and new_labels:
                for label in new_labels:
                    if self.label_manager.check_and_add(label):
                        logger.info(f"新增标签: {label}")
            
            # 提取并注册 zeolites
            zeolites = self._extract_zeolites(relations)
            for zeolite_name in zeolites:
                self.zeolite_registry.register(
                    zeolite_name=zeolite_name,
                    pdf_id=pdf_id,
                    source_file=json_path_obj.name,
                )
            
            # 确定输出路径
            if output_path is None:
                output_path = json_path_obj.parent / f"{json_path_obj.stem}_relations.txt"
            
            # 保存关系
            with open(output_path, "w", encoding="utf-8") as f:
                for relation in relations:
                    f.write(relation.to_line() + "\n")
            
            logger.info(f"提取 {len(relations)} 条关系，保存到 {output_path}")
            
            return ExtractionResult(
                json_path=json_path,
                output_path=str(output_path),
                relations_count=len(relations),
                new_labels=new_labels,
                warnings=warnings,
                zeolites=zeolites,
                pdf_id=pdf_id,
                success=True,
            )
            
        except Exception as e:
            logger.error(f"提取失败: {json_path}: {e}")
            return ExtractionResult(
                json_path=json_path,
                output_path=None,
                relations_count=0,
                new_labels=[],
                warnings=[],
                zeolites=[],
                pdf_id=None,
                success=False,
                error=str(e),
            )
    
    def batch_extract_from_dir(
        self, 
        base_dir: str,
        output_dir: Optional[str] = None,
        show_progress: bool = True,
    ) -> Dict[str, ExtractionResult]:
        """
        批量提取：遍历 base_dir 下的 {pdf name} 文件夹，
        提取其中的 {pdf name}_extract.json
        
        Args:
            base_dir: 基础目录（如 output/）
            output_dir: 输出目录，默认与输入同目录
            show_progress: 是否显示进度条
            
        Returns:
            {输入文件路径: ExtractionResult} 映射
        """
        base_dir = Path(base_dir)
        results = {}
        
        # 收集所有待处理的 JSON 文件
        json_files = []
        for folder in base_dir.iterdir():
            if not folder.is_dir():
                continue
            
            folder_name = folder.name
            json_file = folder / f"{folder_name}_extract.json"
            
            if json_file.exists():
                json_files.append((folder_name, json_file))
        
        if not json_files:
            logger.warning(f"在 {base_dir} 中未找到任何待处理的 JSON 文件")
            return results
        
        logger.info(f"找到 {len(json_files)} 个待处理文件")
        
        # 使用 tqdm 显示进度条
        iterator = tqdm(json_files, desc="提取进度", unit="文件") if show_progress else json_files
        
        for folder_name, json_file in iterator:
            if show_progress:
                iterator.set_postfix({"当前": folder_name[:20]})
            
            # 确定输出路径
            if output_dir:
                out_path = Path(output_dir) / f"{folder_name}_relations.txt"
            else:
                out_path = None
            
            result = self.extract_and_save(str(json_file), out_path)
            results[str(json_file)] = result
        
        # 统计结果
        success_count = sum(1 for r in results.values() if r.success)
        logger.info(f"批量处理完成: {success_count}/{len(results)} 成功")
        
        return results
    
    async def batch_extract_async(
        self, 
        base_dir: str,
        output_dir: Optional[str] = None,
        max_workers: int = 5,
        show_progress: bool = True,
    ) -> Dict[str, ExtractionResult]:
        """
        异步批量提取：使用线程池并发处理多个文件
        
        Args:
            base_dir: 基础目录
            output_dir: 输出目录
            max_workers: 最大并发数
            show_progress: 是否显示进度条
            
        Returns:
            {输入文件路径: ExtractionResult} 映射
        """
        base_dir = Path(base_dir)
        
        # 收集所有待处理的 JSON 文件
        json_files = []
        for folder in base_dir.iterdir():
            if not folder.is_dir():
                continue
            
            folder_name = folder.name
            json_file = folder / f"{folder_name}_extract.json"
            
            if json_file.exists():
                out_path = None
                if output_dir:
                    out_path = str(Path(output_dir) / f"{folder_name}_relations.txt")
                json_files.append((str(json_file), out_path))
        
        if not json_files:
            logger.warning(f"在 {base_dir} 中未找到任何待处理的 JSON 文件")
            return {}
        
        logger.info(f"找到 {len(json_files)} 个待处理文件，使用 {max_workers} 个并发线程")
        
        results = {}
        
        # 使用 ThreadPoolExecutor 进行并发处理
        # 兼容 Python 3.10+ 的事件循环获取方式
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 创建所有任务
            futures = [
                loop.run_in_executor(
                    executor,
                    self.extract_and_save,
                    json_path,
                    out_path,
                )
                for json_path, out_path in json_files
            ]
            
            # 使用 tqdm 显示进度
            if show_progress:
                pbar = tqdm(total=len(futures), desc="异步提取", unit="文件")
                for future, (json_path, _) in zip(asyncio.as_completed(futures), json_files):
                    try:
                        result = await future
                        results[json_path] = result
                    except Exception as e:
                        results[json_path] = ExtractionResult(
                            json_path=json_path,
                            output_path=None,
                            relations_count=0,
                            new_labels=[],
                            warnings=[],
                            zeolites=[],
                            pdf_id=None,
                            success=False,
                            error=str(e),
                        )
                    pbar.update(1)
                pbar.close()
            else:
                gathered = await asyncio.gather(*futures, return_exceptions=True)
                for (json_path, _), result in zip(json_files, gathered):
                    if isinstance(result, Exception):
                        results[json_path] = ExtractionResult(
                            json_path=json_path,
                            output_path=None,
                            relations_count=0,
                            new_labels=[],
                            warnings=[],
                            zeolites=[],
                            pdf_id=None,
                            success=False,
                            error=str(result),
                        )
                    else:
                        results[json_path] = result
        
        # 统计结果
        success_count = sum(1 for r in results.values() if r.success)
        logger.info(f"异步批量处理完成: {success_count}/{len(results)} 成功")
        
        return results


def main():
    """命令行入口"""
    import argparse
    
    parser = argparse.ArgumentParser(description="从 JSON 提取反应网络关系")
    parser.add_argument("input", help="输入 JSON 文件或目录")
    parser.add_argument("-o", "--output", help="输出文件/目录路径")
    parser.add_argument("--batch", action="store_true", help="批量处理目录")
    parser.add_argument("--async", dest="use_async", action="store_true", help="使用异步并发处理")
    parser.add_argument("--workers", type=int, default=5, help="异步处理时的最大并发数")
    parser.add_argument("--no-progress", action="store_true", help="不显示进度条")
    
    args = parser.parse_args()
    
    extractor = ReactionNetworkExtractor()
    
    if args.batch or os.path.isdir(args.input):
        if args.use_async:
            # 异步批量提取
            results = asyncio.run(
                extractor.batch_extract_async(
                    args.input,
                    args.output,
                    max_workers=args.workers,
                    show_progress=not args.no_progress,
                )
            )
        else:
            # 同步批量提取
            results = extractor.batch_extract_from_dir(
                args.input,
                args.output,
                show_progress=not args.no_progress,
            )
        
        success = sum(1 for r in results.values() if r.success)
        logger.info(f"处理完成: {success}/{len(results)} 成功")
    else:
        # 单文件提取
        result = extractor.extract_and_save(args.input, args.output)
        if result.success:
            logger.info(f"输出保存到: {result.output_path}")
        else:
            logger.error(f"提取失败: {result.error}")


if __name__ == "__main__":
    main()
