"""
Text2Cypher 接口性能评估脚本
- 运行 6 个 query，每个跑 1 次
- 统计每个 query 的运行时间
- 统计返回的 nodes/links 数量
- 统计总体成功率
- 结果输出到 CSV 文件
"""

import csv
import time
from datetime import datetime
from typing import Any

import requests

# ============================================================
# 配置
# ============================================================
API_URL = "http://127.0.0.1:5000/query/agent"  # 接口地址
RUNS_PER_QUERY = 10  # 每个 query 运行次数

# ============================================================
# 在这里定义你的 6 个测试 query
# ============================================================
QUERIES = [
    "查询反应R2[12]的催化剂",
    "查找C4H8(g)参与的反应及完整信息",
    "查找催化剂H-ZSM-5参与的反应及完整信息",
    "查询含有ZSM的催化剂的反应及完整信息",
    "查询从CH3CH2OH到CO的反应路径和每一步反应的详细信息",
    "溯源生成C2H4(g)的反应及完整信息",
]


# ============================================================
# 以下为测试运行逻辑
# ============================================================

def run_single_query(query: str) -> dict[str, Any]:
    """发送单个 query 到接口，返回结果统计"""
    start_time = time.perf_counter()
    
    try:
        response = requests.post(
            API_URL,
            json={"query": query},
        )
        elapsed = time.perf_counter() - start_time
        
        data = response.json()
        
        if response.status_code == 200:
            return {
                "success": True,
                "elapsed": elapsed,
                "node_count": data.get("node_count", 0),
                "link_count": data.get("link_count", 0),
                "steps": len(data.get("steps", [])),
                "error": None,
            }
        else:
            return {
                "success": False,
                "elapsed": elapsed,
                "node_count": 0,
                "link_count": 0,
                "steps": 0,
                "error": data.get("error", f"HTTP {response.status_code}"),
            }
    
    except requests.exceptions.ConnectionError:
        elapsed = time.perf_counter() - start_time
        return {
            "success": False,
            "elapsed": elapsed,
            "node_count": 0,
            "link_count": 0,
            "steps": 0,
            "error": "连接失败，请确保服务已启动 (python app.py)",
        }
    except Exception as e:
        elapsed = time.perf_counter() - start_time
        return {
            "success": False,
            "elapsed": elapsed,
            "node_count": 0,
            "link_count": 0,
            "steps": 0,
            "error": str(e),
        }
    



def run_benchmark():
    """运行所有 query 并统计结果"""
    results = []
    total = len(QUERIES)
    success_count = 0

    total_runs = total * RUNS_PER_QUERY
    
    print("=" * 70)
    print("Text2Cypher 接口性能评估")
    print(f"接口地址: {API_URL}")
    print(f"Query 数量: {total}")
    print(f"每个 Query 运行次数: {RUNS_PER_QUERY}")
    print(f"总运行次数: {total_runs}")
    print("=" * 70)

    for i, query in enumerate(QUERIES, 1):
        print(f"\n▶ Query {i}/{total}: {query[:50]}{'...' if len(query) > 50 else ''}")
        
        for run in range(1, RUNS_PER_QUERY + 1):
            print(f"  Run {run}/{RUNS_PER_QUERY}: ", end="", flush=True)
            
            result = run_single_query(query)
            result["query"] = query
            result["query_id"] = i
            result["run"] = run
            results.append(result)

            if result["success"]:
                success_count += 1
                print(f"✓ {result['elapsed']:.2f}s | "
                      f"节点: {result['node_count']} | 边: {result['link_count']}")
            else:
                print(f"✗ {result['elapsed']:.2f}s | {result['error']}")

    # 汇总统计
    print("\n" + "=" * 70)
    print("测试结果汇总")
    print("=" * 70)

    total_time = sum(r["elapsed"] for r in results)
    avg_time = total_time / total_runs if total_runs > 0 else 0
    success_rate = (success_count / total_runs) * 100 if total_runs > 0 else 0

    # 按 Query 分组统计
    print(f"\n{'Query':<6} {'成功率':<10} {'平均耗时(s)':<14} {'平均节点':<10} {'平均边':<10}")
    print("-" * 60)
    for i, query in enumerate(QUERIES, 1):
        query_results = [r for r in results if r["query_id"] == i]
        q_success = sum(1 for r in query_results if r["success"])
        q_avg_time = sum(r["elapsed"] for r in query_results) / len(query_results)
        q_avg_nodes = sum(r["node_count"] for r in query_results) / len(query_results)
        q_avg_links = sum(r["link_count"] for r in query_results) / len(query_results)
        print(f"Q{i:<5} {q_success}/{RUNS_PER_QUERY:<9} {q_avg_time:<14.2f} {q_avg_nodes:<10.1f} {q_avg_links:<10.1f}")

    print("-" * 60)
    print(f"\n📊 总体统计:")
    print(f"   - 总运行次数: {total_runs}")
    print(f"   - 总耗时: {total_time:.2f} 秒")
    print(f"   - 平均耗时: {avg_time:.2f} 秒")
    print(f"   - 成功: {success_count}/{total_runs}")
    print(f"   - 成功率: {success_rate:.1f}%")

    # 保存到 CSV
    save_to_csv(results, {
        "total_time": total_time,
        "avg_time": avg_time,
        "success_count": success_count,
        "total": total,
        "success_rate": success_rate,
        "total_nodes": total_nodes,
        "total_links": total_links,
    })


def save_to_csv(results: list, summary: dict):
    """保存结果到 CSV 文件"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"benchmark_{timestamp}.csv"
    
    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        
        # 写入详细结果
        writer.writerow(["Query#", "Run#", "Query", "状态", "耗时(s)", "节点数", "边数", "步骤数", "错误信息"])
        for r in results:
            writer.writerow([
                r["query_id"],
                r["run"],
                r["query"],
                "成功" if r["success"] else "失败",
                f"{r['elapsed']:.2f}",
                r["node_count"],
                r["link_count"],
                r["steps"],
                r["error"] or "",
            ])
        
        # 空行
        writer.writerow([])
        
        # 写入汇总统计
        writer.writerow(["汇总统计"])
        writer.writerow(["总耗时(s)", f"{summary['total_time']:.2f}"])
        writer.writerow(["平均耗时(s)", f"{summary['avg_time']:.2f}"])
        writer.writerow(["成功数", f"{summary['success_count']}/{summary['total']}"])
        writer.writerow(["成功率", f"{summary['success_rate']:.1f}%"])
        writer.writerow(["总节点数", summary["total_nodes"]])
        writer.writerow(["总边数", summary["total_links"]])
    
    print(f"\n📁 结果已保存到: {filename}")


if __name__ == "__main__":
    run_benchmark()
