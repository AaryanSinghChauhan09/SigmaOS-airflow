//! SigmaOS-airflow — comprehensive workflow orchestration component.
//!
//! Provides workflow orchestration functionality with DAG-based workflows,
//! task management, scheduling, and execution monitoring.

pub mod dag;
pub mod task;
pub mod workflow;
pub mod scheduler;
pub mod executor;
pub mod manager;
pub mod options;

pub use dag::{DAG, DAGNode, DAGEdge, NodeType};
pub use task::{Task, TaskType, TaskStatus, TaskDependency};
pub use workflow::{Workflow, WorkflowStatus, WorkflowRun, WorkflowRunStatus};
pub use scheduler::{Scheduler, ScheduleType, ScheduleStatus};
pub use executor::{Executor, ExecutionResult, ExecutionStatus};
pub use manager::{WorkflowManager, ManagerOptions, ManagerStatistics};
pub use options::{Mode, Options};

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_node_type() {
        let types = vec![
            NodeType::Task,
            NodeType::Operator,
            NodeType::Sensor,
            NodeType::Trigger,
            NodeType::Branch,
            NodeType::Join,
        ];
        assert_eq!(types.len(), 6);
    }
    
    #[test]
    fn test_task_type() {
        let types = vec![
            TaskType::Python,
            TaskType::Bash,
            TaskType::SQL,
            TaskType::Email,
            TaskType::HTTP,
            TaskType::File,
            TaskType::Custom,
        ];
        assert_eq!(types.len(), 7);
    }
    
    #[test]
    fn test_task_status() {
        let statuses = vec![
            TaskStatus::Pending,
            TaskStatus::Running,
            TaskStatus::Success,
            TaskStatus::Failed,
            TaskStatus::Skipped,
            TaskStatus::UpstreamFailed,
        ];
        assert_eq!(statuses.len(), 6);
    }
    
    #[test]
    fn test_workflow_status() {
        let statuses = vec![
            WorkflowStatus::Draft,
            WorkflowStatus::Active,
            WorkflowStatus::Paused,
            WorkflowStatus::Archived,
        ];
        assert_eq!(statuses.len(), 4);
    }
    
    #[test]
    fn test_workflow_run_status() {
        let statuses = vec![
            WorkflowRunStatus::Queued,
            WorkflowRunStatus::Running,
            WorkflowRunStatus::Success,
            WorkflowRunStatus::Failed,
            WorkflowRunStatus::Cancelled,
        ];
        assert_eq!(statuses.len(), 5);
    }
    
    #[test]
    fn test_schedule_type() {
        let types = vec![
            ScheduleType::Manual,
            ScheduleType::Cron,
            ScheduleType::Interval,
            ScheduleType::Event,
            ScheduleType::Trigger,
        ];
        assert_eq!(types.len(), 5);
    }
    
    #[test]
    fn test_schedule_status() {
        let statuses = vec![
            ScheduleStatus::Active,
            ScheduleStatus::Paused,
            ScheduleStatus::Disabled,
        ];
        assert_eq!(statuses.len(), 3);
    }
    
    #[test]
    fn test_execution_status() {
        let statuses = vec![
            ExecutionStatus::Pending,
            ExecutionStatus::Running,
            ExecutionStatus::Success,
            ExecutionStatus::Failed,
            ExecutionStatus::Cancelled,
        ];
        assert_eq!(statuses.len(), 5);
    }
    
    #[test]
    fn test_dag_node_creation() {
        let node = DAGNode::new(
            "task-1".into(),
            "Process Data".into(),
            NodeType::Task,
        );
        
        assert_eq!(node.id, "task-1");
        assert_eq!(node.name, "Process Data");
        assert_eq!(node.node_type, NodeType::Task);
    }
    
    #[test]
    fn test_dag_edge_creation() {
        let edge = DAGEdge::new("task-1".into(), "task-2".into());
        
        assert_eq!(edge.source, "task-1");
        assert_eq!(edge.target, "task-2");
    }
    
    #[test]
    fn test_task_creation() {
        let task = Task::new(
            "task-1".into(),
            "Process Data".into(),
            TaskType::Python,
        );
        
        assert_eq!(task.id, "task-1");
        assert_eq!(task.name, "Process Data");
        assert_eq!(task.task_type, TaskType::Python);
    }
    
    #[test]
    fn test_workflow_creation() {
        let workflow = Workflow::new(
            "workflow-1".into(),
            "Data Pipeline".into(),
        );
        
        assert_eq!(workflow.id, "workflow-1");
        assert_eq!(workflow.name, "Data Pipeline");
        assert_eq!(workflow.status, WorkflowStatus::Draft);
    }
    
    #[test]
    fn test_workflow_run_creation() {
        let run = WorkflowRun::new(
            "run-1".into(),
            "workflow-1".into(),
        );
        
        assert_eq!(run.id, "run-1");
        assert_eq!(run.workflow_id, "workflow-1");
        assert_eq!(run.status, WorkflowRunStatus::Queued);
    }
    
    #[test]
    fn test_scheduler_creation() {
        let scheduler = Scheduler::new();
        assert!(scheduler.list_schedules().is_empty());
    }
    
    #[test]
    fn test_create_schedule() {
        let mut scheduler = Scheduler::new();
        let schedule_id = scheduler.create_schedule(
            "Daily Job".into(),
            ScheduleType::Cron,
        );
        
        assert!(scheduler.get_schedule(&schedule_id).is_some());
        assert_eq!(scheduler.list_schedules().len(), 1);
    }
    
    #[test]
    fn test_executor_creation() {
        let executor = Executor::new();
        assert!(executor.get_executions().is_empty());
    }
    
    #[test]
    fn test_execute_task() {
        let mut executor = Executor::new();
        let task = Task::new("task-1".into(), "Test Task".into(), TaskType::Python);
        
        let execution_id = executor.execute_task(task);
        
        assert!(executor.get_execution(&execution_id).is_some());
        assert_eq!(executor.get_executions().len(), 1);
    }
    
    #[test]
    fn test_manager_creation() {
        let mut manager = WorkflowManager::new();
        assert!(manager.list_workflows().is_empty());
    }
    
    #[test]
    fn test_create_workflow() {
        let mut manager = WorkflowManager::new();
        let workflow_id = manager.create_workflow("Data Pipeline".into());
        
        assert!(manager.get_workflow(&workflow_id).is_some());
        assert_eq!(manager.list_workflows().len(), 1);
    }
    
    #[test]
    fn test_run_workflow() {
        let mut manager = WorkflowManager::new();
        let workflow_id = manager.create_workflow("Data Pipeline".into());
        
        let run_id = manager.run_workflow(workflow_id.clone());
        
        assert!(manager.get_workflow_run(&run_id).is_some());
        assert_eq!(manager.list_workflow_runs().len(), 1);
    }
    
    #[test]
    fn test_export_import() {
        let mut manager = WorkflowManager::new();
        
        let workflow_id = manager.create_workflow("Data Pipeline".into());
        manager.run_workflow(workflow_id);
        
        let export = manager.export_data();
        
        let mut manager2 = WorkflowManager::new();
        manager2.import_data(export);
        
        assert_eq!(manager2.list_workflows().len(), 1);
        assert_eq!(manager2.list_workflow_runs().len(), 1);
    }
    
    #[test]
    fn test_manager_statistics() {
        let mut manager = WorkflowManager::new();
        
        manager.create_workflow("Pipeline1".into());
        manager.create_workflow("Pipeline2".into());
        
        let stats = manager.get_statistics();
        assert_eq!(stats.total_workflows, 2);
        assert_eq!(stats.total_runs, 0);
    }
}
