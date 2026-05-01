# web_query/routes.py
"""
Flask routes for graph query UI.
Handles simple query, full graph preview, and agent (plan-and-execute) query.
"""

import os
import sys
from flask import Blueprint, render_template, request, jsonify

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cypher_generator import CypherGenerator, QueryAgent
from neo4j_tools import CypherExecutor
from logger import get_logger

logger = get_logger(__name__)

bp = Blueprint("web_query", __name__)

cypher_generator = CypherGenerator()
cypher_executor = CypherExecutor()
query_agent = None  # lazy init


def get_query_agent():
    """Lazy init QueryAgent."""
    global query_agent
    if query_agent is None:
        query_agent = QueryAgent(max_retries=3,max_steps=7)
    return query_agent


def _merge_graph_results(results):
    """Merge multiple CypherResult objects into a single nodes/links graph."""
    nodes = []
    links = []
    node_map = {}  # name -> node dict with new id
    link_set = set()

    def add_node(node):
        name = node.get("name")
        if not name:
            return None
        if name in node_map:
            return node_map[name]["id"]
        new_id = len(nodes)
        new_node = dict(node)
        new_node["id"] = new_id
        node_map[name] = new_node
        nodes.append(new_node)
        return new_id

    for result in results:
        local_nodes = result.nodes or []
        local_map = {n.get("id"): n.get("name") for n in local_nodes}

        for n in local_nodes:
            add_node(n)

        for link in result.links or []:
            src_name = local_map.get(link.get("source"))
            tgt_name = local_map.get(link.get("target"))
            if not src_name or not tgt_name:
                continue
            src_id = node_map[src_name]["id"]
            tgt_id = node_map[tgt_name]["id"]
            key = (src_id, tgt_id, link.get("value"))
            if key in link_set:
                continue
            link_set.add(key)
            links.append({
                "source": src_id,
                "target": tgt_id,
                "value": link.get("value"),
            })

    return nodes, links


def _execute_statements(statements):
    """Execute multiple Cypher statements and merge results."""
    results = []
    errors = []

    for stmt in statements:
        res = cypher_executor.execute(stmt)
        if res.success:
            results.append(res)
        else:
            errors.append(res.error or "unknown error")

    nodes, links = _merge_graph_results(results)
    return len(errors) == 0, nodes, links, errors


@bp.route("/")
def index():
    """Home page."""
    return render_template("kg_query.html")


@bp.route("/query", methods=["POST"])
def query():
    """Simple query endpoint (LLM -> Cypher)."""
    data = request.get_json(silent=True)
    user_query = data.get("query", "") if data else request.form.get("query", "")
    user_query = user_query.strip()

    if not user_query:
        return jsonify({"error": "Please input a query."}), 400

    logger.info(f"User query: {user_query}")

    cypher_text = cypher_generator.generate(user_query)
    statements = CypherGenerator.split_statements(cypher_text)
    cypher = "; ".join(statements)
    logger.debug(f"Generated Cypher: {cypher}")

    if not statements:
        return jsonify({
            "error": "No valid Cypher was generated.",
            "raw_query": user_query,
        }), 400

    success, nodes, links, errors = _execute_statements(statements)
    retry_count = 0
    max_retries = 2

    while not success and retry_count < max_retries:
        retry_count += 1
        err_msg = "; ".join(errors) if errors else "unknown error"
        logger.warning(f"Cypher failed (retry {retry_count}/{max_retries}): {err_msg}")

        cypher_text = cypher_generator.generate_with_retry(
            query=user_query,
            error_message=err_msg,
            previous_cypher=cypher,
        )
        statements = CypherGenerator.split_statements(cypher_text)
        cypher = "; ".join(statements)
        logger.debug(f"Retry Cypher: {cypher}")

        success, nodes, links, errors = _execute_statements(statements)

    if not success:
        return jsonify({
            "error": f"Query failed: {'; '.join(errors) if errors else 'unknown error'}",
            "cypher": cypher,
            "raw_query": user_query,
            "retries": retry_count,
        }), 500

    if not nodes and not links:
        return jsonify({
            "error": "No relevant results.",
            "cypher": cypher,
            "raw_query": user_query,
        }), 404

    logger.info(f"Query success: {len(nodes)} nodes, {len(links)} links")

    return jsonify({
        "nodes": nodes,
        "links": links,
        "cypher": cypher,
        "raw_query": user_query,
        "node_count": len(nodes),
        "link_count": len(links),
        "retries": retry_count,
    })


@bp.route("/query/full", methods=["GET"])
def query_full_graph():
    """
    Fetch full graph overview.
    Executes a fixed Cypher without LLM.
    """
    logger.info("Request full graph (limit 2000)")

    cypher = "MATCH p = ()-[r:LINKS]->() RETURN p LIMIT 2000"

    try:
        result = cypher_executor.execute(cypher)

        if not result.success:
            return jsonify({"error": f"Load failed: {result.error}"}), 500

        nodes = result.nodes or []
        links = result.links or []

        return jsonify({
            "nodes": nodes,
            "links": links,
            "cypher": cypher,
            "node_count": len(nodes),
            "link_count": len(links),
            "raw_query": "FULL GRAPH",
        })
    except Exception as e:
        logger.error(f"Load full graph failed: {e}")
        return jsonify({"error": str(e)}), 500


@bp.route("/query/agent", methods=["POST"])
def query_with_agent():
    """Plan-and-execute query endpoint."""
    data = request.get_json(silent=True)
    user_query = data.get("query", "") if data else request.form.get("query", "")
    user_query = user_query.strip()

    if not user_query:
        return jsonify({"error": "Please input a query."}), 400

    logger.info(f"Agent query: {user_query}")

    try:
        agent = get_query_agent()
        result = agent.query(user_query)

        if result.get("success"):
            logger.info(f"Agent success: {len(result.get('nodes', []))} nodes")
            return jsonify({
                "nodes": result.get("nodes", []),
                "links": result.get("links", []),
                "steps": result.get("steps", []),
                "raw_query": user_query,
                "node_count": len(result.get("nodes", [])),
                "link_count": len(result.get("links", [])),
                "mode": "agent",
            })
        else:
            logger.warning(f"Agent failed: {result.get('error')}")
            return jsonify({
                "error": result.get("error", "Query failed"),
                "plan": result.get("plan", []),
                "raw_query": user_query,
            }), 404

    except Exception as e:
        logger.error(f"Agent exception: {e}")
        return jsonify({
            "error": f"Query execution failed: {str(e)}",
            "raw_query": user_query,
        }), 500


@bp.route("/schema", methods=["GET"])
def get_schema():
    """Return graph schema info."""
    try:
        from neo4j_tools.connection import Neo4jConnection
        schema = Neo4jConnection.get_schema_info()
        return jsonify(schema)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/health", methods=["GET"])
def health_check():
    """Health check."""
    try:
        from neo4j_tools.connection import Neo4jConnection
        connected = Neo4jConnection.verify_connection()
        return jsonify({
            "status": "healthy" if connected else "unhealthy",
            "neo4j_connected": connected,
        })
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "error": str(e),
        }), 500


@bp.route("/refresh-cache", methods=["POST"])
def refresh_cache():
    """Refresh Cypher generator cache."""
    cypher_generator.refresh_cache()
    return jsonify({"status": "ok", "message": "Cache refreshed"})


def register_routes(app):
    """Register routes into Flask app."""
    app.register_blueprint(bp)
