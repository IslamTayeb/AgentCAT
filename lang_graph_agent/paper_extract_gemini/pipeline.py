"""
Paper Extract Pipeline 模块

串联规划、Schema 演化和数据抽取流程，用于从学术论文中提取结构化信息。
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from .config import PipelineConfig, load_config
from .client import APIClient
from .planning import DomainFramework, run_planning
from .schema_evo import run_schema_evolution
from .extract import run_extraction
from .review import run_review, run_review_standalone, save_evaluation_log
from .review import run_review, run_review_standalone, save_evaluation_log
from .utils import (
    collect_pdfs,
    collect_pdfs_from_dir,
    strip_introduced_in_round,
    calculate_schema_evolution_stats,
)
from .logger import get_logger
import csv

logger = get_logger("paper_extract.pipeline")


def _batch_items(items: list, batch_size: int) -> list[list]:
    """
    将列表分成固定大小的批次。
    
    Args:
        items: 待分批的列表
        batch_size: 每批的大小
        
    Returns:
        批次列表，每个批次是一个子列表
    """
    if batch_size <= 0:
        batch_size = 1
    return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]


class PaperExtractPipeline:
    """论文信息抽取 Pipeline"""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._planning_client: Optional[APIClient] = None
        self._schema_evo_client: Optional[APIClient] = None
        self._extraction_client: Optional[APIClient] = None
        self._review_client: Optional[APIClient] = None
        self._framework: Optional[DomainFramework] = None

    @classmethod
    def from_config_file(cls, config_path: str) -> "PaperExtractPipeline":
        """从配置文件创建 Pipeline"""
        logger.debug(f"Loading config from: {config_path}")
        config = load_config(config_path)
        return cls(config)

    @property
    def planning_client(self) -> APIClient:
        """规划阶段 API 客户端（延迟初始化）"""
        if self._planning_client is None:
            api_config = self.config.get_planning_api()
            logger.debug(f"Initializing planning client: model={api_config.model}")
            self._planning_client = APIClient(api_config, rate_limit_rpm=self.config.concurrency.rate_limit_rpm)
        return self._planning_client

    @property
    def schema_evo_client(self) -> APIClient:
        """Schema 演化 API 客户端（延迟初始化）"""
        if self._schema_evo_client is None:
            api_config = self.config.get_schema_evo_api()
            logger.debug(f"Initializing schema evolution client: model={api_config.model}")
            self._schema_evo_client = APIClient(api_config, rate_limit_rpm=self.config.concurrency.rate_limit_rpm)
        return self._schema_evo_client

    @property
    def extraction_client(self) -> APIClient:
        """数据抽取 API 客户端（延迟初始化）"""
        if self._extraction_client is None:
            api_config = self.config.get_extraction_api()
            logger.debug(f"Initializing extraction client: model={api_config.model}")
            self._extraction_client = APIClient(api_config, rate_limit_rpm=self.config.concurrency.rate_limit_rpm)
        return self._extraction_client

    @property
    def framework(self) -> Optional[DomainFramework]:
        """当前使用的领域框架"""
        return self._framework

    @property
    def review_client(self) -> APIClient:
        """审核 API 客户端（延迟初始化）"""
        if self._review_client is None:
            api_config = self.config.get_review_api()
            logger.debug(f"Initializing review client: model={api_config.model}")
            self._review_client = APIClient(api_config, rate_limit_rpm=self.config.concurrency.rate_limit_rpm)
        return self._review_client

    def create_extraction_client(self) -> APIClient:
        """创建新的抽取 API 客户端（用于并发执行，每个线程需要独立的 client）"""
        api_config = self.config.get_extraction_api()
        return APIClient(api_config, rate_limit_rpm=self.config.concurrency.rate_limit_rpm)

    def create_review_client(self) -> APIClient:
        """创建新的审核 API 客户端（用于并发执行）"""
        api_config = self.config.get_review_api()
        return APIClient(api_config, rate_limit_rpm=self.config.concurrency.rate_limit_rpm)

    @framework.setter
    def framework(self, value: DomainFramework):
        self._framework = value

    def run_planning_stage(self) -> DomainFramework:
        """
        运行交互式规划阶段。

        Returns:
            确认后的 DomainFramework
        """
        framework = run_planning(
            client=self.planning_client,
            config=self.config.planning,
            output_dir=self.config.output.dir,
        )
        self._framework = framework
        return framework

    def load_framework(self, framework_path: str) -> DomainFramework:
        """
        从文件加载框架。

        Args:
            framework_path: 框架文件路径

        Returns:
            加载的 DomainFramework
        """
        framework = DomainFramework.load(framework_path)
        self._framework = framework
        return framework

    def run_schema_evo_batch(self, pdf_paths: list[str], framework: DomainFramework) -> dict:
        """
        对多个 PDF 批量执行 Schema 演化。

        Args:
            pdf_paths: PDF 文件路径列表
            framework: 领域要素框架

        Returns:
            处理结果字典
        """
        result = {
            "pdf_paths": pdf_paths,
            "schema": None,
            "error": None,
            "file_ids": [],
        }

        logger.info(f"[Schema Evolution] Processing {len(pdf_paths)} PDF(s)")
        try:
            schema, schema_json, file_ids = run_schema_evolution(
                client=self.schema_evo_client,
                config=self.config.schema_evolution,
                pdf_paths=pdf_paths,
                framework=framework,
                file_ids=None,
            )

            # 保存 schema 到输出根目录
            schema_path = os.path.join(self.config.output.dir, self.config.output.schema_file)
            with open(schema_path, "w", encoding="utf-8") as f:
                f.write(schema_json)
            logger.debug(f"Schema saved to: {schema_path}")

            result["schema"] = schema
            result["file_ids"] = file_ids
            logger.info(f"[Schema Evolution] Done. Rounds: {self.config.schema_evolution.rounds}")

            # 统计并保存 schema 演化数据
            try:
                stats = calculate_schema_evolution_stats(schema)
                stats_csv_path = os.path.join(self.config.output.dir, "schema_stats.csv")
                with open(stats_csv_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(["Round", "New_Items_Count"])
                    for r in sorted(stats.keys()):
                        writer.writerow([r, stats[r]])
                logger.info(f"[Schema Evolution] Stats saved to: {stats_csv_path}")
            except Exception as e:
                logger.error(f"[Schema Evolution] Failed to save stats: {e}")

        except Exception as e:
            result["error"] = f"Schema evolution failed: {e}"
            logger.error(f"[Schema Evolution] FAILED: {e}", exc_info=True)

        return result

    def run_extraction_single(
        self,
        pdf_path: str,
        schema: dict,
        role_description: str,
        file_id: str = "",
    ) -> dict:
        """
        对单个 PDF 执行数据抽取。

        Args:
            pdf_path: PDF 文件路径
            schema: 已有的 schema 字典
            role_description: 角色描述
            file_id: 已上传的文件 ID（可选）

        Returns:
            处理结果字典
        """
        pdf_path = str(Path(pdf_path).resolve())
        pdf_basename = Path(pdf_path).stem

        # 输出目录
        output_dir = os.path.join(self.config.output.dir, pdf_basename)
        os.makedirs(output_dir, exist_ok=True)

        result = {
            "pdf_path": pdf_path,
            "output_dir": output_dir,
            "schema": schema,
            "extracted": None,
            "error": None,
        }

        logger.info(f"[Extraction] Processing: {pdf_basename}")
        try:
            extracted, file_id = run_extraction(
                client=self.extraction_client,
                config=self.config.extraction,
                pdf_path=pdf_path,
                schema=schema,
                output_dir=output_dir,
                role_description=role_description,
                file_id=file_id,
            )

            result["extracted"] = extracted
            result["file_id"] = file_id  # 保存 file_id 供后续复用
            logger.info(f"[Extraction] Done. Output: {output_dir}")

        except Exception as e:
            result["error"] = f"Extraction failed: {e}"
            logger.error(f"[Extraction] FAILED: {e}", exc_info=True)

        return result

    def run_review_single(
        self,
        pdf_path: str,
        file_id: str,
        extracted: dict,
        schema: dict,
        output_dir: str,
        role_description: str,
    ) -> dict:
        """
        对单个 PDF 的抽取结果进行审核。

        Args:
            pdf_path: PDF 文件路径
            file_id: 已上传的 file_id
            extracted: 抽取结果
            schema: Schema 字典
            output_dir: 输出目录
            role_description: 角色描述

        Returns:
            处理结果字典
        """
        pdf_basename = Path(pdf_path).stem
        
        result = {
            "pdf_path": pdf_path,
            "output_dir": output_dir,
            "extracted": extracted,
            "reviewed": None,
            "evaluation": None,
            "error": None,
        }

        if not self.config.review.enabled:
            result["reviewed"] = extracted
            return result

        logger.info(f"[Review] Processing: {pdf_basename}")
        try:
            reviewed, eval_log = run_review(
                client=self.review_client,
                extract_client=self.extraction_client,
                config=self.config.review,
                extraction_config=self.config.extraction,
                pdf_path=pdf_path,
                file_id=file_id,
                extracted=extracted,
                schema=schema,
                output_dir=output_dir,
                role_description=role_description,
            )

            result["reviewed"] = reviewed
            result["evaluation"] = eval_log
            
            # 保存评价日志
            log_path = os.path.join(self.config.output.dir, self.config.review.evaluation_log)
            save_evaluation_log(
                log_path=log_path,
                pdf_name=Path(pdf_path).name,
                model=self.config.get_review_api().model,
                eval_log=eval_log,
            )
            
            # 更新抽取结果文件
            final_path = os.path.join(output_dir, f"{pdf_basename}_extract.json")
            with open(final_path, "w", encoding="utf-8") as f:
                json.dump(reviewed, f, ensure_ascii=False, indent=2)
            
            logger.info(f"[Review] Done. Output: {output_dir}")

        except Exception as e:
            result["error"] = f"Review failed: {e}"
            result["reviewed"] = extracted  # 审核失败时保留原结果
            logger.error(f"[Review] FAILED: {e}", exc_info=True)

        return result

    def _process_single_pdf(
        self,
        pdf_path: str,
        schema: dict,
        role_description: str,
    ) -> dict:
        """
        处理单个 PDF：extract + review（如果启用）。
        
        此方法封装了单个 PDF 的完整处理流程，用于并发执行。
        每次调用会创建独立的 API client，避免多线程共享导致的连接问题。
        
        Args:
            pdf_path: PDF 文件路径
            schema: Schema 字典
            role_description: 角色描述
            
        Returns:
            处理结果字典
        """
        pdf_path = str(Path(pdf_path).resolve())
        pdf_basename = Path(pdf_path).stem
        logger.info(f"[Process] Starting: {pdf_basename}")
        
        # 创建独立的 client（避免多线程共享导致的连接问题）
        extraction_client = self.create_extraction_client()
        
        # 输出目录
        output_dir = os.path.join(self.config.output.dir, pdf_basename)
        os.makedirs(output_dir, exist_ok=True)
        
        result = {
            "pdf_path": pdf_path,
            "output_dir": output_dir,
            "schema": schema,
            "extracted": None,
            "error": None,
        }
        
        logger.info(f"[Extraction] Processing: {pdf_basename}")
        try:
            extracted, file_id = run_extraction(
                client=extraction_client,
                config=self.config.extraction,
                pdf_path=pdf_path,
                schema=schema,
                output_dir=output_dir,
                role_description=role_description,
            )
            result["extracted"] = extracted
            result["file_id"] = file_id
            logger.info(f"[Extraction] Done. Output: {output_dir}")
        except Exception as e:
            result["error"] = f"Extraction failed: {e}"
            logger.error(f"[Extraction] FAILED: {e}", exc_info=True)
            logger.info(f"[Process] Completed: {pdf_basename}")
            return result
        
        # 执行审核（如果启用且抽取成功）
        if result.get("extracted") and self.config.review.enabled:
            review_client = self.create_review_client()
            file_id = result.get("file_id", "")
            
            logger.info(f"[Review] Processing: {pdf_basename}")
            try:
                reviewed, eval_log = run_review(
                    client=review_client,
                    extract_client=extraction_client,
                    config=self.config.review,
                    extraction_config=self.config.extraction,
                    pdf_path=pdf_path,
                    file_id=file_id,
                    extracted=result["extracted"],
                    schema=schema,
                    output_dir=output_dir,
                    role_description=role_description,
                )
                result["reviewed"] = reviewed
                result["evaluation"] = eval_log
                
                # 保存评价日志
                log_path = os.path.join(self.config.output.dir, self.config.review.evaluation_log)
                save_evaluation_log(log_path, Path(pdf_path).name, self.config.get_review_api().model, eval_log)
                
                # 更新抽取结果文件
                final_path = os.path.join(output_dir, f"{pdf_basename}_extract.json")
                with open(final_path, "w", encoding="utf-8") as f:
                    json.dump(reviewed, f, ensure_ascii=False, indent=2)
                logger.info(f"[Review] Done. Output: {output_dir}")
            except Exception as e:
                result["review_error"] = f"Review failed: {e}"
                result["reviewed"] = result["extracted"]  # 审核失败时保留原结果
                logger.error(f"[Review] FAILED: {e}", exc_info=True)
        
        logger.info(f"[Process] Completed: {pdf_basename}")
        return result

    def run_extraction_batch(
        self,
        pdf_paths: list[str],
        schema: dict,
        role_description: str,
    ) -> list[dict]:
        """
        批量并发执行 Extract + Review（如果启用）。
        
        按 max_workers 分批并发执行，每批内的 PDF 并发处理，
        等待当前批次全部完成后再执行下一批。
        
        Args:
            pdf_paths: PDF 文件路径列表
            schema: Schema 字典
            role_description: 角色描述
            
        Returns:
            处理结果列表
        """
        max_workers = self.config.concurrency.max_workers
        batches = _batch_items(pdf_paths, max_workers)
        results = []
        
        total_batches = len(batches)
        total_pdfs = len(pdf_paths)
        
        logger.info(f"[Batch] Starting batch processing: {total_pdfs} PDF(s) in {total_batches} batch(es), max_workers={max_workers}")
        
        for batch_idx, batch in enumerate(batches, 1):
            batch_size = len(batch)
            logger.info("=" * 60)
            logger.info(f"[Batch {batch_idx}/{total_batches}] Processing {batch_size} PDF(s)")
            for pdf in batch:
                logger.info(f"  - {Path(pdf).name}")
            logger.info("=" * 60)
            
            # 批次内并发执行
            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                futures = {
                    executor.submit(self._process_single_pdf, pdf, schema, role_description): pdf
                    for pdf in batch
                }
                for future in as_completed(futures):
                    pdf = futures[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"[Batch] Failed to process {Path(pdf).name}: {e}", exc_info=True)
                        results.append({
                            "pdf_path": pdf,
                            "error": str(e),
                        })
            
            logger.info(f"[Batch {batch_idx}/{total_batches}] Completed")
        
        logger.info(f"[Batch] All batches completed: {len(results)} PDF(s) processed")
        return results

    def _review_single_standalone(
        self,
        review_item: dict,
        schema_file: str,
        role_description: str,
    ) -> dict:
        """
        处理单个独立 Review 任务。
        
        Args:
            review_item: 包含 pdf_path, extract_json_path, output_dir 的字典
            schema_file: Schema 文件路径
            role_description: 角色描述
            
        Returns:
            处理结果字典
        """
        pdf_path = review_item["pdf_path"]
        extract_json_path = review_item["extract_json_path"]
        output_dir = review_item["output_dir"]
        pdf_name = Path(pdf_path).name
        
        logger.info(f"[Review] Starting: {pdf_name}")
        
        try:
            reviewed, eval_log = run_review_standalone(
                client=self.review_client,
                extract_client=self.extraction_client,
                config=self.config.review,
                extraction_config=self.config.extraction,
                pdf_path=pdf_path,
                extract_json_path=extract_json_path,
                schema_path=schema_file,
                role_description=role_description,
                output_dir=output_dir,
            )
            
            # 保存评价日志
            log_path = os.path.join(self.config.output.dir, self.config.review.evaluation_log)
            save_evaluation_log(log_path, pdf_name, self.config.get_review_api().model, eval_log)
            
            logger.info(f"[Review] Completed: {pdf_name}")
            return {
                "pdf_path": pdf_path,
                "reviewed": reviewed,
                "evaluation": eval_log,
            }
        except Exception as e:
            logger.error(f"[Review] FAILED for {pdf_name}: {e}", exc_info=True)
            return {"pdf_path": pdf_path, "error": str(e)}

    def run_review_batch(
        self,
        review_items: list[dict],
        schema_file: str,
        role_description: str,
    ) -> list[dict]:
        """
        批量并发执行独立 Review。
        
        按 max_workers 分批并发执行，每批内的任务并发处理，
        等待当前批次全部完成后再执行下一批。
        
        Args:
            review_items: Review 任务列表，每项包含 pdf_path, extract_json_path, output_dir
            schema_file: Schema 文件路径
            role_description: 角色描述
            
        Returns:
            处理结果列表
        """
        max_workers = self.config.concurrency.max_workers
        batches = _batch_items(review_items, max_workers)
        results = []
        
        total_batches = len(batches)
        total_items = len(review_items)
        
        logger.info(f"[Review Batch] Starting batch processing: {total_items} item(s) in {total_batches} batch(es), max_workers={max_workers}")
        
        for batch_idx, batch in enumerate(batches, 1):
            batch_size = len(batch)
            logger.info("=" * 60)
            logger.info(f"[Review Batch {batch_idx}/{total_batches}] Processing {batch_size} item(s)")
            for item in batch:
                logger.info(f"  - {Path(item['pdf_path']).name}")
            logger.info("=" * 60)
            
            # 批次内并发执行
            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                futures = {
                    executor.submit(self._review_single_standalone, item, schema_file, role_description): item
                    for item in batch
                }
                for future in as_completed(futures):
                    item = futures[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"[Review Batch] Failed to process {Path(item['pdf_path']).name}: {e}", exc_info=True)
                        results.append({
                            "pdf_path": item["pdf_path"],
                            "error": str(e),
                        })
            
            logger.info(f"[Review Batch {batch_idx}/{total_batches}] Completed")
        
        logger.info(f"[Review Batch] All batches completed: {len(results)} item(s) processed")
        return results

    def run(
        self,
        stage: Optional[str] = None,
        framework_path: Optional[str] = None,
    ) -> list[dict]:
        """
        根据配置运行 Pipeline。

        Args:
            stage: 指定运行的阶段 ("plan", "evo", "extract", None 表示全部)
            framework_path: 框架文件路径（跳过规划阶段时使用）

        Returns:
            处理结果列表
        """
        results = []
        schema = None
        framework = None

        # 确保输出目录存在
        os.makedirs(self.config.output.dir, exist_ok=True)

        # 阶段 0: 规划
        if stage is None or stage == "plan":
            if self.config.planning.enabled:
                logger.info("=" * 60)
                logger.info("[Planning] Starting interactive planning phase")
                logger.info("=" * 60)
                framework = self.run_planning_stage()
                logger.info("[Planning] Framework confirmed")

                if stage == "plan":
                    # 仅规划阶段，直接返回
                    return results

        # 加载框架（如果跳过了规划阶段）
        if framework is None:
            if framework_path:
                framework = self.load_framework(framework_path)
            elif self._framework:
                framework = self._framework
            else:
                # 尝试从默认位置加载
                default_framework_path = os.path.join(
                    self.config.output.dir, self.config.planning.framework_file
                )
                if os.path.exists(default_framework_path):
                    framework = self.load_framework(default_framework_path)
                else:
                    logger.error("[Pipeline] No framework available. Run planning stage first or provide --framework.")
                    return results

        # 阶段 1: Schema 演化
        if stage is None or stage == "evo":
            if self.config.schema_evolution.enabled:
                evo_input = self.config.get_schema_evo_input()
                evo_pdfs = collect_pdfs(evo_input.path)

                if not evo_pdfs:
                    logger.warning(f"[Schema Evolution] No PDF files configured")
                else:
                    logger.info("=" * 60)
                    logger.info(f"[Schema Evolution] Starting with {len(evo_pdfs)} PDF(s)")
                    logger.info("=" * 60)

                    result = self.run_schema_evo_batch(evo_pdfs, framework)
                    results.append(result)

                    if not result.get("error") and result.get("schema"):
                        schema = result["schema"]

        # 阶段 2: 数据抽取（含内联审核）
        if stage is None or stage == "extract":
            if self.config.extraction.enabled:
                # 如果没有 schema，尝试从文件加载
                if schema is None:
                    schema_file = self.config.extraction.schema_file
                    if not schema_file:
                        logger.error("[Extraction] No schema available. Provide schema_file or enable schema_evolution.")
                        return results

                    logger.info(f"[Extraction] Loading schema from: {schema_file}")
                    with open(schema_file, "r", encoding="utf-8") as f:
                        schema = json.load(f)
                    schema = strip_introduced_in_round(schema)

                ext_input = self.config.get_extraction_input()
                ext_pdfs = collect_pdfs_from_dir(ext_input.path, ext_input.pattern)

                if not ext_pdfs:
                    logger.warning(f"[Extraction] No PDF files found: {ext_input.path}")
                else:
                    logger.info(f"[Extraction] Found {len(ext_pdfs)} PDF(s)")
                    # 使用批量并发处理
                    batch_results = self.run_extraction_batch(
                        ext_pdfs, schema, framework.role_description
                    )
                    results.extend(batch_results)

        # 阶段 3: 独立审核（单独运行 review 阶段）
        if stage == "review":
            review_input = self.config.get_review_input()
            
            # 加载 schema
            schema_file = review_input.schema_file or self.config.extraction.schema_file
            if not schema_file:
                schema_file = os.path.join(self.config.output.dir, self.config.output.schema_file)
            
            if not os.path.exists(schema_file):
                logger.error(f"[Review] Schema file not found: {schema_file}")
                return results
            
            with open(schema_file, "r", encoding="utf-8") as f:
                schema = json.load(f)
            schema = strip_introduced_in_round(schema)
            
            # 单文件模式
            if review_input.pdf_file and review_input.extract_json:
                pdf_path = review_input.pdf_file
                extract_json_path = review_input.extract_json
                output_dir = os.path.dirname(extract_json_path)
                
                logger.info("=" * 60)
                logger.info(f"[Review] Single file: {Path(pdf_path).name}")
                logger.info("=" * 60)
                
                try:
                    reviewed, eval_log = run_review_standalone(
                        client=self.review_client,
                        extract_client=self.extraction_client,
                        config=self.config.review,
                        extraction_config=self.config.extraction,
                        pdf_path=pdf_path,
                        extract_json_path=extract_json_path,
                        schema_path=schema_file,
                        role_description=framework.role_description if framework else "",
                        output_dir=output_dir,
                    )
                    
                    # 保存评价日志
                    log_path = os.path.join(self.config.output.dir, self.config.review.evaluation_log)
                    save_evaluation_log(log_path, Path(pdf_path).name, self.config.get_review_api().model, eval_log)
                    
                    results.append({
                        "pdf_path": pdf_path,
                        "reviewed": reviewed,
                        "evaluation": eval_log,
                    })
                except Exception as e:
                    logger.error(f"[Review] FAILED: {e}", exc_info=True)
                    results.append({"pdf_path": pdf_path, "error": str(e)})
            
            # 批量模式（从目录发现）
            elif review_input.extract_result_dir:
                result_dir = review_input.extract_result_dir
                ext_input = self.config.get_extraction_input()
                
                # 收集所有有效的 review 任务
                review_items = []
                for subdir in os.listdir(result_dir):
                    subdir_path = os.path.join(result_dir, subdir)
                    if not os.path.isdir(subdir_path):
                        continue
                    
                    # 查找 extract.json
                    extract_json_path = os.path.join(subdir_path, f"{subdir}_extract.json")
                    if not os.path.exists(extract_json_path):
                        continue
                    
                    # 查找对应 PDF
                    pdf_path = os.path.join(ext_input.path, f"{subdir}.pdf")
                    if not os.path.exists(pdf_path):
                        logger.warning(f"[Review] PDF not found for {subdir}: {pdf_path}")
                        continue
                    
                    review_items.append({
                        "pdf_path": pdf_path,
                        "extract_json_path": extract_json_path,
                        "output_dir": subdir_path,
                    })
                
                # 使用批量并发处理
                if review_items:
                    role_desc = framework.role_description if framework else ""
                    batch_results = self.run_review_batch(review_items, schema_file, role_desc)
                    results.extend(batch_results)
                else:
                    logger.warning(f"[Review] No valid review items found in: {result_dir}")

        if not results and stage is None:
            logger.warning("No PDFs processed. Check input configuration.")

        return results

