# SAS Agentic Lineage Builder - Project Specification

## 🎯 Project Objectives

Build an agentic system to automatically reconstruct complete data lineage from a massive SAS codebase (1000+ files) on Databricks.

**Key Deliverables:**
1. Parsed dependencies (static extraction)
2. Resolved dependencies (LLM-powered dynamic resolution)
3. Business context enrichment
4. Complete lineage DAG with Unity Catalog integration
5. Interactive dashboard

---

## 📁 Project Structure

    sas_agentic_lineage_builder/
    ├── README.md
    ├── config.py
    ├── agents/
    │   ├── __init__.py
    │   ├── base_agent.py
    │   ├── parser_agent.py
    │   ├── resolver_agent.py
    │   ├── context_agent.py
    │   └── graph_builder_agent.py
    ├── orchestration/
    │   ├── __init__.py
    │   ├── workflow.py
    │   └── state_manager.py
    ├── utils/
    │   ├── __init__.py
    │   ├── sas_parser.py
    │   ├── llm_client.py
    │   ├── delta_helpers.py
    │   └── validation.py
    ├── notebooks/
    │   ├── 00_setup_environment.ipynb
    │   ├── 01_upload_sas_files.ipynb
    │   ├── 02_run_lineage_analysis.ipynb
    │   ├── 03_explore_results.ipynb
    │   └── 04_dashboard_queries.sql
    └── tests/
        ├── __init__.py
        ├── test_parser_agent.py
        ├── test_resolver_agent.py
        ├── test_context_agent.py
        └── test_integration.py

---

## 📦 Module Specifications

### 1. config.py

**Purpose:** Centralized configuration

**Must contain:**
- Unity Catalog paths: catalog name, schema name, volume path
- Delta table names: raw_dependencies, resolved_dependencies, expanded_includes, dataset_classifications, lineage_edges, lineage_nodes, file_metadata, workflow_state
- LLM settings: model name databricks-meta-llama-3-1-70b-instruct, temperature 0.1, max_tokens 4000
- Parsing rules: file patterns, size limits
- Spark configuration: parallelism, memory settings

---

### 2. agents/base_agent.py

**Purpose:** Abstract base class for all agents

**Must implement:**
- Constructor that accepts config parameter
- execute method as abstract (to be overridden in subclasses)
- log_execution method to write to workflow_state table
- handle_error method for standardized error handling
- Integration with state_manager and llm_client

---

### 3. agents/parser_agent.py

**Purpose:** Extract syntactic dependencies using Spark without LLM

**Input:** Volume path containing .sas files

**Output:** raw_dependencies Delta table with columns:
- file_path STRING
- file_name STRING
- input_datasets ARRAY of STRING
- output_datasets ARRAY of STRING
- macro_calls ARRAY of STRING
- include_files ARRAY of STRING
- has_dynamic_deps BOOLEAN
- proc_types ARRAY of STRING

**Key methods:**
- parse_repository accepting volume_path returning string
- extract_dependencies accepting code returning dict
- Uses regex patterns for: SET, MERGE, DATA, OUT=, %INCLUDE, %MACRO

**Performance requirement:** Process 1000+ files in under 10 minutes using Spark UDFs

---

### 4. agents/resolver_agent.py

**Purpose:** Resolve dynamic dependencies with LLM

**Input:** raw_dependencies table filtered for has_dynamic_deps equals True

**Output:** resolved_dependencies Delta table with columns:
- file_path STRING
- original_inputs ARRAY of STRING
- original_outputs ARRAY of STRING
- resolved_inputs ARRAY of STRING
- resolved_outputs ARRAY of STRING
- resolution_confidence FLOAT (0 to 1)
- macro_definitions MAP of STRING to STRING
- resolution_notes STRING

**Key methods:**
- resolve_dynamic_dependencies returning string
- resolve_with_llm accepting code and raw_deps returning dict
- extract_macro_definitions accepting code returning dict
- expand_includes returning string (recursive)

**LLM Prompt Pattern:**

    Resolve macro variables in SAS code.
    Code: {snippet}
    Macros: {definitions}
    Raw deps: {inputs/outputs}
    Return JSON: {"resolved_inputs": [...], "confidence": 0.95}

---

### 5. agents/context_agent.py

**Purpose:** Enrich with business context

**Input:** resolved_dependencies table

**Output:** dataset_classifications Delta table with columns:
- dataset_name STRING
- business_domain STRING (Sales, Finance, HR, etc.)
- data_category STRING (Transactional, Master, Reference)
- sensitivity_level STRING (Public, Internal, Confidential, PII)
- description STRING
- unity_catalog_match STRING
- match_confidence FLOAT

**Key methods:**
- classify_datasets returning string
- classify_with_llm accepting dataset_name returning dict
- match_to_unity_catalog returning string (fuzzy matching)

---

### 6. agents/graph_builder_agent.py

**Purpose:** Build lineage DAG

**Input:** All previous tables

**Output:**
- lineage_nodes table with columns: node_id, node_type, centrality metrics
- lineage_edges table with columns: source, target, edge_type, confidence
- JSON export for visualization

**Key methods:**
- build_lineage_graph returning string
- calculate_metrics accepting graph returning dict (centrality, PageRank)
- detect_cycles accepting graph returning List
- integrate_with_unity_catalog returning string

**Uses NetworkX for graph operations**

---

### 7. orchestration/workflow.py

**Purpose:** Orchestrate all agents

**Workflow stages:**

    1. PARSE    → ParserAgent
    2. RESOLVE  → ResolverAgent
    3. ENRICH   → ContextAgent
    4. BUILD    → GraphBuilderAgent
    5. VALIDATE → Quality checks

**Key methods:**
- run accepting optional resume_from parameter returning dict
- execute_stage accepting stage and agent returning dict
- handle_stage_failure accepting stage and error
- resume_workflow accepting from_stage returning dict

**Integrates with MLflow for tracking**

---

### 8. orchestration/state_manager.py

**Purpose:** Workflow state persistence

**Key methods:**
- create_workflow accepting workflow_id and config returning string
- update_stage_status accepting workflow_id, stage, and status
- save_checkpoint accepting workflow_id, stage, and data
- load_checkpoint accepting workflow_id and stage returning dict

**Uses workflow_state Delta table**

---

### 9. utils/sas_parser.py

**Purpose:** Reusable SAS parsing utilities

**Contains:**
- Comprehensive regex patterns for SAS syntax
- Helper functions: extract_input_datasets, extract_output_datasets, etc.
- Dataset name normalization

---

### 10. utils/llm_client.py

**Purpose:** LLM API wrapper

**Key methods:**
- generate accepting prompt returning string
- generate_json accepting prompt and optional schema returning dict
- Retry logic: 3 attempts with exponential backoff
- Token usage tracking

---

### 11. utils/delta_helpers.py

**Purpose:** Delta table operations

**Key functions:**
- create_table accepting name and schema
- write_dataframe accepting df, table, mode
- read_table accepting name returning DataFrame
- table_exists accepting name returning bool

---

### 12. utils/validation.py

**Purpose:** Data quality checks

**Key functions:**
- validate_parsing_results accepting table returning dict
- validate_resolution_quality accepting table returning dict
- validate_graph_integrity accepting nodes and edges returning dict

---

## 📓 Notebook Specifications

### 00_setup_environment.ipynb

**Purpose:** One-time setup

**Cells:**
- Install dependencies: networkx, mlflow, databricks-genai
- Create catalog, schema, volume
- Create all Delta tables
- Validate setup

---

### 01_upload_sas_files.ipynb

**Purpose:** Upload SAS files

**Cells:**
- Upload .sas files to Volume
- Create file metadata table
- Validate upload

---

### 02_run_lineage_analysis.ipynb

**Purpose:** Main execution

**Cells:**
- Import workflow
- Execute: workflow.run()
- Display summary report

---

### 03_explore_results.ipynb

**Purpose:** Interactive analysis

**Cells:**
- Load results tables
- Show statistics
- Visualize graph using networkx
- Export for external tools

---

### 04_dashboard_queries.sql

**Purpose:** Dashboard SQL

**Queries:**
- Overview metrics
- Top critical files
- Domain breakdown
- Dependency analysis

---

## 🔄 Workflow Execution Flow

    User runs: notebooks/02_run_lineage_analysis.ipynb
        ↓
    workflow.py initializes all agents
        ↓
    Stage 1: parser_agent.parse_repository()
        → Spark reads all .sas files
        → Parallel parsing with UDFs
        → Write to raw_dependencies
        ↓
    Stage 2: resolver_agent.resolve_dynamic_dependencies()
        → Filter has_dynamic_deps=True
        → LLM resolves &variables
        → Write to resolved_dependencies
        ↓
    Stage 3: context_agent.classify_datasets()
        → Extract unique datasets
        → LLM classification
        → Match to Unity Catalog
        → Write to dataset_classifications
        ↓
    Stage 4: graph_builder_agent.build_lineage_graph()
        → Build NetworkX graph
        → Calculate metrics
        → Write to lineage_nodes/edges
        → Export JSON
        ↓
    Stage 5: Validation & Report
        → Run quality checks
        → Generate summary
        → Log to MLflow

---

## 🎯 Key Design Principles

1. **Separation of Concerns:** Each agent has one clear responsibility
2. **Spark for Scale:** Use Spark UDFs for parallel processing
3. **LLM Only When Needed:** Parser uses regex, LLM only for dynamic resolution
4. **State Persistence:** Checkpoint after each stage for resume capability
5. **Delta as Source of Truth:** All intermediate results in Delta tables
6. **Fail Gracefully:** Fallback strategies for LLM failures

---

## 📊 Expected Output Tables

| Table | Purpose | Rows (for 1000 files) |
|-------|---------|----------------------|
| raw_dependencies | Parsed deps | ~1,000 |
| resolved_dependencies | Resolved dynamic | ~150 |
| dataset_classifications | Business context | ~500 |
| lineage_nodes | Graph nodes | ~1,500 |
| lineage_edges | Graph edges | ~5,000 |
| workflow_state | Execution tracking | ~5 (per run) |

---

## 🚀 Getting Started (for LLM Code Generation)

**To generate code for a specific module, provide:**
1. This README.md
2. Module name (e.g., "agents/parser_agent.py")
3. Any specific requirements or constraints

**Example prompt:**

    Using the specification in README.md, generate complete code for agents/parser_agent.py 
    with all methods implemented, including Spark UDFs for parallel processing and 
    comprehensive regex patterns.