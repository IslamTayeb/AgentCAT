# kg_extractor/prompts.py
"""
反应网络提取的 Prompt 模板
"""

EXTRACTION_PROMPT = '''你是催化反应网络提取专家。从结构化JSON中提取以基元反应为核心的反应网络关系。

## 输入说明
输入是由LLM从科研论文中提取的结构化JSON，可能存在以下情况：
- 某些字段可能缺失、为空数组 [] 或为 null
- 字段名称可能有细微差异（如 step_id 有时叫 reaction_id）
- elementary_steps 中的 step_id 格式不统一（可能是 "R1"、"k1" 或描述性文字）

请根据语义而非严格的字段名称进行提取。

## 输出格式(英文)
每行一个关系，格式为：
实体名:标签,实体名:标签

## 可用标签(英文)
当前系统中的标签：
{existing_labels}

如需新增标签，必须在最后单独一行写：NEW_LABEL:标签名:说明

## 提取规则

### 0. Direction rules (must follow)
- (molecular)->(reactionNode): reactant
- (reactionNode)->(molecular): product
- (activeSite)->(reactionNode): active site participates
- (zeolite)->(activeSite): catalyst owns active site
- (zeolite)->(propertyCategory)->(propertyNode): catalyst properties
- (reactionNode)->(propertyNode): energy/barrier/notes


### 1. 催化剂识别（优先级：高）
从 catalyst 部分识别催化剂：
- 优先使用 name 字段（如 "Pt@Sn-MFI"）
- 若无 name，则组合 framework 和 composition.metals（如 "H-ZSM-5"）
输出：催化剂名:zeolite,属性类别:propertyCategory

### 2. 催化剂属性（优先级：高）
遍历以下可能的属性字段：
- heteroatoms（杂原子）
- defects（缺陷）
- pore_system（孔道结构）
- composition（组成）
- morphology（形貌）

输出层级关系：
- 催化剂名:zeolite,属性类别名:propertyCategory
- 属性类别名:propertyCategory,具体属性值:propertyNode

### 3. 基元反应步骤（核心，优先级：最高）
从 elementary_steps 数组提取：
- 每个步骤统一编号为 R1、R2、R3...（按数组顺序）
- 忽略原始 step_id 的格式差异

对每个步骤提取：
a) 活性位点 -> 反应：{{active_site}}:activeSite,R{{n}}:reactionNode
b) 反应物 -> 反应：{{reactant}}:molecular,R{{n}}:reactionNode
c) 反应 -> 生成物：R{{n}}:reactionNode,{{product}}:molecular
d) 能量信息（如有）：R{{n}}:reactionNode,ΔE = {{value}} {{unit}}:propertyNode

### 4. 活性位点（优先级：高）
从 adsorption_sites 和 active_site 字段提取：
- 催化剂 -> 活性位点：催化剂名:zeolite,位点名:activeSite
- 活性位点 -> 反应节点（在哪一步起作用）

### 5. 分子/反应物/生成物命名（重要！）
将分子名称标准化为化学式：

| 原始名称 | 标准化 |
|----------|--------|
| propane | C3H8(g) |
| propene / propylene | C3H6(g) |
| ethane | C2H6(g) |
| ethene / ethylene | C2H4(g) |
| methane | CH4(g) |
| hydrogen / H2 | H2(g) |
| 吸附态物种（如 "propane adsorbed"） | C3H8* |
| 氢原子 H-atoms | H* |
| 空位/表面位点（*,vacancy等） | * |

保留特殊中间体原名（如 Si-OH, Sn-OH, Pt-H）。

### 6. 中间体（优先级：中）
从 intermediates 数组提取：
- 中间体 -> 结合位点关系
- 中间体参与的反应步骤

### 7. 副反应（优先级：低）
从 secondary_paths 提取副反应信息。

## 输出要求
1. 每行一个关系，不要空行
2. 不要输出任何解释、注释或 markdown 格式
3. 同一分子在不同反应中必须使用相同名称
4. 跳过空值、null、空数组
5. 如果 JSON 中某部分完全缺失，直接跳过，不要报错


### Output constraints (important)
- Only use available labels; declare any new label via NEW_LABEL
- reactionNode names must be R1, R2, R3 ... in elementary_steps order
- Output strictly as entity:label,entity:label (no quotes, no numbering)

## 输入JSON
{json_content}'''
