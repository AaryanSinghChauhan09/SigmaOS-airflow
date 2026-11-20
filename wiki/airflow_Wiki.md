# SigmaOS-airflow Component Wiki

## Overview
SigmaOS-airflow provides Apache Airflow workflow orchestration and pipeline management support for the SigmaOS operating system. It demonstrates the workflow orchestration subsystem and pipeline management within the SigmaOS ecosystem, following the principles of clear abstraction and deterministic workflow initialization.

## Principles
- Single Responsibility
- Workflow Abstraction
- Fail-Fast on Workflow Initialization Errors
- Deterministic Workflow Setup
- Clear Abstraction Over Workflow APIs
- Composition over Inheritance for workflow extensions

## Architecture
SigmaOS-airflow follows a workflow pattern:
- **Workflow Core**: Provides DAG management, task scheduling, and pipeline orchestration
- **Airflow Integration**: Supports Airflow DAG definitions, task dependencies, and pipeline scheduling
- **Task Management**: Supports task execution, retries, and workflow monitoring
- **Error Handling**: Fail-fast on invalid workflow configurations or unsupported workflow types

## API
The component provides the following public APIs:
- `workflow::dag::create(name, definition)` - Create a new workflow DAG with given name and definition
- `workflow::task::execute(task)` - Execute a single task within a workflow
- `workflow::schedule::cron(expression)` - Schedule a workflow with cron expression
- `workflow::status::pipeline()` - Get workflow pipeline status and task progress
- Error types for workflow failure scenarios or unsupported workflow types

## Test Structure (Arrange-Act-ASSERT)
All tests follow the AAA pattern:
1. **Arrange**: Set up test environment with specific workflow configuration
2. **Act**: Execute the code under test (DAG creation, task execution)
3. **Assert**: Verify the expected workflow results

Example:
```rust
#[test]
fn test_workflow_dag_aaa() {
    // Arrange
    let definition = r#"{
        "nodes": ["task1", "task2"],
        "edges": [["task1", "task2"]]
    }"#;
    let dag_name = "daily-pipeline";
    
    // Act
    let result = workflow::dag::create(dag_name, definition);
    
    // Assert
    assert!(result.is_ok());
    let dag = result.unwrap();
    assert!(dag.is_valid());
}
```

## Development
- Follows Rust best practices with `#![forbid(unsafe_code)]`
- Workflow initialization is best-effort; gracefully degrades on unsupported configurations
- Comprehensive error types for all workflow failure scenarios
- Unit tests provided for DAG management and task execution
- Integration tests for full workflow system with actual pipeline orchestration

## TODO
- Add support for additional workflow types (Kubernetes workflows, containerized pipelines)
- Expand scheduling capabilities (cron expressions, dependency resolution, resource allocation)
- Add monitoring features (task progress, failure detection, automated recovery)
- Create integration tests with actual workflow orchestration systems
- Implement workflow persistence and state management