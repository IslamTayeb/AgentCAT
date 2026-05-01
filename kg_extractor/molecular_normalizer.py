# kg_extractor/molecular_normalizer.py
"""
分子名称校验器
用于校验 LLM 生成的分子名称是否符合化学式格式
"""

import re
from typing import Tuple


class MolecularNormalizer:
    """
    分子校验器
    校验分子名称是否为有效的化学式格式
    """
    
    # 化学式正则表达式
    # 匹配模式：元素符号（首字母大写，可选小写）+ 可选数字，重复多次
    # 支持：C3H8, H2O, CO2, Si-OH, Pt-H, C3H8(g), C3H8*
    FORMULA_PATTERNS = [
        # 标准化学式：C3H8, H2O, CO2, NaCl
        r'^([A-Z][a-z]?\d*)+$',
        # 带状态标记：C3H8(g), H2O(l), CO2(aq)
        r'^([A-Z][a-z]?\d*)+\([glsaq]+\)$',
        # 吸附态：C3H8*, H*, CH3*
        r'^([A-Z][a-z]?\d*)+\*$',
        # 带下标的化学式：H₂O, CO₂
        r'^([A-Z][a-z]?[₀₁₂₃₄₅₆₇₈₉]*)+$',
        # 带连接符的物种：Si-OH, Pt-H, Sn-OH
        r'^[A-Z][a-z]?(-[A-Z][a-z]?\d*)+$',
        # 离子：H+, OH-, O2-
        r'^([A-Z][a-z]?\d*)+[+-]+$',
        # 复杂物种：[SiO]3-Sn-O-
        r'^\[?[A-Za-z\d\[\]]+\]?(-[A-Za-z\d\[\]]+)*-?$',
    ]
    
    # 已知的特殊物种（不符合化学式但应该保留）
    KNOWN_SPECIES = {
        '*',           # 空位
        'H*',          # 吸附氢
        'vacancy',     # 空位
    }
    # ???? -> ??????
    COMMON_NAME_MAP = {
        'propane': 'C3H8(g)',
        'propene': 'C3H6(g)',
        'propylene': 'C3H6(g)',
        'ethane': 'C2H6(g)',
        'ethene': 'C2H4(g)',
        'ethylene': 'C2H4(g)',
        'methane': 'CH4(g)',
        'hydrogen': 'H2(g)',
        'water': 'H2O(g)',
        'carbon monoxide': 'CO(g)',
        'carbon dioxide': 'CO2(g)',
    }

    ADSORBED_KEYWORDS = ('adsorbed', 'adsorption', 'adsorb')

    
    def __init__(self):
        """初始化校验器"""
        self._compiled_patterns = [re.compile(p) for p in self.FORMULA_PATTERNS]
    
    def is_valid_formula(self, name: str) -> bool:
        """
        校验是否为有效的化学式格式
        
        Args:
            name: 分子名称
            
        Returns:
            是否为有效化学式
        """
        if not name:
            return False

    def normalize_name(self, name: str) -> str:
        """
        ????????????????
        """
        if not name:
            return name

        raw = name.strip()
        if not raw:
            return raw

        lower = raw.lower()

        # ????????????
        if self.is_valid_formula(raw):
            return raw

        if lower in self.COMMON_NAME_MAP:
            return self.COMMON_NAME_MAP[lower]

        # ???????
        if any(k in lower for k in self.ADSORBED_KEYWORDS):
            base = lower
            for k in self.ADSORBED_KEYWORDS:
                base = base.replace(k, '')
            base = base.replace('adsorbed on', '').replace('adsorbed', '').strip()
            base_norm = self.COMMON_NAME_MAP.get(base, base)
            base_norm = re.sub(r'\([^)]*\)$', '', base_norm)  # ??????
            if base_norm and not base_norm.endswith('*'):
                return base_norm + '*'

        return raw
        
        name = name.strip()
        
        # 检查已知特殊物种
        if name in self.KNOWN_SPECIES:
            return True
        
        # 检查所有正则模式
        for pattern in self._compiled_patterns:
            if pattern.match(name):
                return True
        
        return False
    
    def validate_and_warn(self, name: str) -> Tuple[bool, str]:
        """
        校验并返回警告信息
        
        Args:
            name: 分子名称
            
        Returns:
            (是否有效, 警告信息)
        """
        is_valid = self.is_valid_formula(name)
        
        if is_valid:
            return True, ""
        else:
            return False, f"'{name}' 可能不是有效的化学式格式"
    
    def filter_valid_formulas(self, names: list) -> Tuple[list, list]:
        """
        过滤有效的化学式
        
        Args:
            names: 分子名称列表
            
        Returns:
            (有效列表, 无效列表)
        """
        valid = []
        invalid = []
        
        for name in names:
            if self.is_valid_formula(name):
                valid.append(name)
            else:
                invalid.append(name)
        
        return valid, invalid


def main():
    """测试入口"""
    normalizer = MolecularNormalizer()
    
    test_cases = [
        # 应该有效
        "C3H8", "H2O", "CO2", "C3H8(g)", "C3H8*", "H*", "*",
        "Si-OH", "Pt-H", "Sn-OH", "[SiO]3-Sn-O-",
        # 可能无效
        "propane", "hydrogen", "methyl group", "active site",
    ]
    
    print("化学式校验测试：")
    for name in test_cases:
        is_valid = normalizer.is_valid_formula(name)
        status = "✓" if is_valid else "✗"
        print(f"  {status} {name}")


if __name__ == "__main__":
    main()
