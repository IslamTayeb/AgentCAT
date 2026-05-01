# kg_extractor/pdf_id_manager.py
"""
PDF ID 管理器
维护 pdf_id ↔ 文章名的双向映射，用于追踪 zeolite 来源
"""

import json
import os
from datetime import datetime
from typing import Dict, Optional

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from logger import get_logger

logger = get_logger(__name__)


class PdfIdManager:
    """
    PDF ID 管理器
    - 为每个新的 PDF/文章分配唯一递增 ID
    - 维护双向映射：pdf_id ↔ 文章名
    - 持久化到 JSON 文件
    """
    
    def __init__(self, mapping_file: Optional[str] = None):
        """
        初始化 PDF ID 管理器
        
        Args:
            mapping_file: 映射文件路径，默认使用项目根目录的 pdf_mapping.json
        """
        if mapping_file is None:
            from config import EXTRACTOR_PATHS
            mapping_file = EXTRACTOR_PATHS.pdf_mapping_file
        
        self.mapping_file = mapping_file
        self.id_to_name: Dict[int, str] = {}
        self.name_to_id: Dict[str, int] = {}
        self.next_id: int = 1
        self._load()
    
    def _load(self) -> None:
        """从文件加载映射"""
        if os.path.exists(self.mapping_file):
            try:
                with open(self.mapping_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    # JSON 的 key 只能是字符串，需要转换
                    self.id_to_name = {int(k): v for k, v in data.get("id_to_name", {}).items()}
                    self.name_to_id = data.get("name_to_id", {})
                    self.next_id = data.get("next_id", 1)
                logger.debug(f"加载 {len(self.id_to_name)} 条 PDF 映射")
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"加载 PDF 映射文件失败: {e}")
                self._reset()
        else:
            self._reset()
    
    def _reset(self) -> None:
        """重置为空状态"""
        self.id_to_name = {}
        self.name_to_id = {}
        self.next_id = 1
    
    def save(self) -> None:
        """保存映射到文件"""
        data = {
            "id_to_name": {str(k): v for k, v in self.id_to_name.items()},
            "name_to_id": self.name_to_id,
            "next_id": self.next_id,
            "last_updated": datetime.now().isoformat(),
            "description": "PDF ID 与文章名的双向映射"
        }
        
        # 确保目录存在（处理纯文件名的情况）
        dir_path = os.path.dirname(self.mapping_file)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        
        with open(self.mapping_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.debug(f"保存 {len(self.id_to_name)} 条 PDF 映射")
    
    def get_or_create_id(self, pdf_name: str) -> int:
        """
        获取或创建 PDF ID
        
        Args:
            pdf_name: PDF/文章名称（通常是文件夹名或文件名前缀）
            
        Returns:
            对应的 pdf_id（已存在则返回现有 ID，否则创建新 ID）
        """
        pdf_name = pdf_name.strip()
        
        # 已存在则直接返回
        if pdf_name in self.name_to_id:
            return self.name_to_id[pdf_name]
        
        # 创建新 ID
        new_id = self.next_id
        self.id_to_name[new_id] = pdf_name
        self.name_to_id[pdf_name] = new_id
        self.next_id += 1
        
        # 自动保存
        self.save()
        logger.info(f"创建新 PDF ID: {new_id} -> '{pdf_name}'")
        
        return new_id
    
    def get_name_by_id(self, pdf_id: int) -> Optional[str]:
        """
        通过 ID 查询文章名
        
        Args:
            pdf_id: PDF ID
            
        Returns:
            对应的文章名，不存在则返回 None
        """
        return self.id_to_name.get(pdf_id)
    
    def get_id_by_name(self, pdf_name: str) -> Optional[int]:
        """
        通过文章名查询 ID
        
        Args:
            pdf_name: 文章名
            
        Returns:
            对应的 pdf_id，不存在则返回 None
        """
        return self.name_to_id.get(pdf_name.strip())
    
    def get_all_mappings(self) -> Dict[int, str]:
        """获取所有映射（ID -> 名称）"""
        return self.id_to_name.copy()
    
    def get_count(self) -> int:
        """获取当前映射数量"""
        return len(self.id_to_name)
    
    def __contains__(self, pdf_name: str) -> bool:
        """检查文章名是否已存在"""
        return pdf_name.strip() in self.name_to_id
    
    def __len__(self) -> int:
        return len(self.id_to_name)


class ZeoliteRegistry:
    """
    Zeolite 注册表
    存储每个 zeolite 的属性信息（包括 pdf_id）
    """
    
    def __init__(self, registry_file: Optional[str] = None):
        """
        初始化 Zeolite 注册表
        
        Args:
            registry_file: 注册表文件路径，默认使用项目根目录的 zeolites.json
        """
        if registry_file is None:
            from config import EXTRACTOR_PATHS
            registry_file = EXTRACTOR_PATHS.zeolite_registry_file
        
        self.registry_file = registry_file
        self.zeolites: Dict[str, Dict] = {}
        self._load()
    
    def _load(self) -> None:
        """从文件加载"""
        if os.path.exists(self.registry_file):
            try:
                with open(self.registry_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.zeolites = data.get("zeolites", {})
                logger.debug(f"加载 {len(self.zeolites)} 个 zeolite 记录")
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"加载 zeolite 注册表失败: {e}")
                self.zeolites = {}
        else:
            self.zeolites = {}
    
    def save(self) -> None:
        """保存注册表"""
        data = {
            "zeolites": self.zeolites,
            "last_updated": datetime.now().isoformat(),
            "count": len(self.zeolites),
            "description": "Zeolite 属性注册表，包含 pdf_id 等信息"
        }
        
        # 确保目录存在（处理纯文件名的情况）
        dir_path = os.path.dirname(self.registry_file)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        
        with open(self.registry_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def register(self, zeolite_name: str, pdf_id: int, source_file: str) -> None:
        """
        注册 zeolite
        
        Args:
            zeolite_name: zeolite 名称（如 "Pt@Sn-MFI"）
            pdf_id: 来源 PDF 的 ID
            source_file: 来源文件名
        """
        zeolite_name = zeolite_name.strip()
        
        if zeolite_name not in self.zeolites:
            self.zeolites[zeolite_name] = {
                "pdf_id": pdf_id,
                "source_file": source_file,
                "created_at": datetime.now().isoformat(),
            }
            self.save()
            logger.debug(f"注册 zeolite: {zeolite_name} (pdf_id={pdf_id})")
        else:
            # 更新现有记录（可能从不同来源再次发现）
            existing = self.zeolites[zeolite_name]
            if existing.get("pdf_id") != pdf_id:
                logger.debug(f"zeolite '{zeolite_name}' 已存在 (原 pdf_id={existing.get('pdf_id')})")
    
    def get_pdf_id(self, zeolite_name: str) -> Optional[int]:
        """获取 zeolite 的 pdf_id"""
        entry = self.zeolites.get(zeolite_name.strip())
        return entry.get("pdf_id") if entry else None
    
    def get_all(self) -> Dict[str, Dict]:
        """获取所有 zeolite 记录"""
        return self.zeolites.copy()
    
    def __contains__(self, zeolite_name: str) -> bool:
        return zeolite_name.strip() in self.zeolites
    
    def __len__(self) -> int:
        return len(self.zeolites)


def main():
    """测试入口"""
    print("=== PdfIdManager 测试 ===")
    mgr = PdfIdManager("test_pdf_mapping.json")
    
    id1 = mgr.get_or_create_id("paper1_extract")
    id2 = mgr.get_or_create_id("paper2_extract")
    id1_again = mgr.get_or_create_id("paper1_extract")
    
    print(f"paper1 -> ID: {id1}")
    print(f"paper2 -> ID: {id2}")
    print(f"paper1 再次查询 -> ID: {id1_again}")
    print(f"ID 1 -> 名称: {mgr.get_name_by_id(1)}")
    
    assert id1 == 1 and id2 == 2 and id1_again == 1
    print("✓ PdfIdManager 测试通过")
    
    print("\n=== ZeoliteRegistry 测试 ===")
    registry = ZeoliteRegistry("test_zeolites.json")
    registry.register("Pt@Sn-MFI", 1, "paper1_extract.json")
    registry.register("H-ZSM-5", 2, "paper2_extract.json")
    
    print(f"Pt@Sn-MFI pdf_id: {registry.get_pdf_id('Pt@Sn-MFI')}")
    print(f"所有 zeolite: {list(registry.zeolites.keys())}")
    print("✓ ZeoliteRegistry 测试通过")
    
    # 清理测试文件
    import os
    os.remove("test_pdf_mapping.json")
    os.remove("test_zeolites.json")
    print("\n✓ 清理测试文件完成")


if __name__ == "__main__":
    main()
