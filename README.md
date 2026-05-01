# AgentCAT (Mirror)

> **This is a mirror of the anonymous review repository at
> [anonymous.4open.science/r/agentcat-E69B](https://anonymous.4open.science/r/agentcat-E69B).**
>
> Original paper: Yang, W. et al. "AgentCAT: An LLM Agent for Extracting and
> Analyzing Catalytic Reaction Data from Chemical Engineering Literature."
> [arXiv:2602.18479](https://arxiv.org/abs/2602.18479), Feb 2026.
>
> Mirrored here for research comparison purposes only. All credit to the
> original authors. This repo will be replaced with a proper fork if/when
> the authors publish a public GitHub repository.

---

# Original README

## Overview
This repository contains agent implementations for a multi-stage pipeline:
1) Adaptive Information Extraction
2) Knowledge Graph Construction
3) General Querying and Graph Exploration

Detailed usage commands and run instructions are documented in `develop.md` inside each stage directory.

## Directory Structure

### 1) `lang_graph_agent/`
Agents for the **Adaptive Information Extraction** stage.
- Purpose: run extraction-focused agents and produce intermediate outputs for downstream processing.
- How to use: see `lang_graph_agent/develop.md`.

### 2) `PROJECT/`
Agents for the **Knowledge Graph Construction** stage and the **General Querying and Graph Exploration** stage.
- Purpose: build the knowledge graph from extracted information, and support querying / exploration workflows on the graph.
- How to use: see `PROJECT/develop.md`.

## LLM API Configuration
This codebase requires access to an LLM API. Please configure the corresponding API credentials/settings on your own (e.g., via environment variables or local config files as described in each directory’s `develop.md`).

## How to Run
Please follow the step-by-step commands in:
- `lang_graph_agent/develop.md`
- `PROJECT/develop.md`

These documents contain the most up-to-date instructions for environment setup, dependencies, and execution commands.
