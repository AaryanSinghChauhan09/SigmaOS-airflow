//! Workflow definitions for airflow.

use crate::dag::DAG;
use crate::task::{Task, TaskStatus};

/// Workflow status
#[derive(Debug, Clone, PartialEq)]
pub enum WorkflowStatus {
    /// Draft
    Draft,
    /// Active
    Active,
    /// Paused
    Paused,
    /// Archived
    Archived,
}

/// Workflow run status
#[derive(Debug, Clone, PartialEq)]
pub enum WorkflowRunStatus {
    /// Queued
    Queued,
    /// Running
    Running,
    /// Success
    Success,
    /// Failed
    Failed,
    /// Cancelled
    Cancelled,
}

/// Workflow run
#[derive(Debug, Clone)]
pub struct WorkflowRun {
    /// Run ID
    pub id: String,
    /// Workflow ID
    pub workflow_id: String,
    /// Run status
    pub status: WorkflowRunStatus,
    /// Run started timestamp
    pub started_at: u64,
    /// Run finished timestamp
    pub finished_at: Option<u64>,
    /// Run duration in seconds
    pub duration: Option<u64>,
    /// Run error message
    pub error_message: Option<String>,
    /// Run task executions
    pub task_executions: std::collections::HashMap<String, TaskExecution>,
    /// Run triggered by
    pub triggered_by: String,
}

/// Task execution
#[derive(Debug, Clone)]
pub struct TaskExecution {
    /// Task ID
    pub task_id: String,
    /// Execution status
    pub status: TaskStatus,
    /// Execution started timestamp
    pub started_at: Option<u64>,
    /// Execution finished timestamp
    pub finished_at: Option<u64>,
    /// Execution duration in seconds
    pub duration: Option<u64>,
    /// Execution error message
    pub error_message: Option<String>,
    /// Execution log output
    pub log_output: String,
}

impl WorkflowRun {
    /// Create new workflow run
    pub fn new(id: String, workflow_id: String) -> Self {
        Self {
            id,
            workflow_id,
            status: WorkflowRunStatus::Queued,
            started_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            finished_at: None,
            duration: None,
            error_message: None,
            task_executions: std::collections::HashMap::new(),
            triggered_by: "manual".into(),
        }
    }
    
    /// Start run
    pub fn start(&mut self) {
        self.status = WorkflowRunStatus::Running;
        self.started_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
    
    /// Mark run as success
    pub fn mark_success(&mut self) {
        self.status = WorkflowRunStatus::Success;
        self.finished_at = Some(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs());
        self.duration = self.finished_at.map(|f| f - self.started_at);
    }
    
    /// Mark run as failed
    pub fn mark_failed(&mut self, error_message: String) {
        self.status = WorkflowRunStatus::Failed;
        self.finished_at = Some(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs());
        self.duration = self.finished_at.map(|f| f - self.started_at);
        self.error_message = Some(error_message);
    }
    
    /// Mark run as cancelled
    pub fn mark_cancelled(&mut self) {
        self.status = WorkflowRunStatus::Cancelled;
        self.finished_at = Some(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs());
        self.duration = self.finished_at.map(|f| f - self.started_at);
    }
    
    /// Add task execution
    pub fn add_task_execution(&mut self, task_id: String, status: TaskStatus) {
        let execution = TaskExecution {
            task_id: task_id.clone(),
            status,
            started_at: None,
            finished_at: None,
            duration: None,
            error_message: None,
            log_output: String::new(),
        };
        
        self.task_executions.insert(task_id, execution);
    }
    
    /// Get task execution
    pub fn get_task_execution(&self, task_id: &str) -> Option<&TaskExecution> {
        self.task_executions.get(task_id)
    }
    
    /// Get task execution mutably
    pub fn get_task_execution_mut(&mut self, task_id: &str) -> Option<&mut TaskExecution> {
        self.task_executions.get_mut(task_id)
    }
    
    /// Update task execution status
    pub fn update_task_execution(&mut self, task_id: &str, status: TaskStatus) {
        if let Some(execution) = self.task_executions.get_mut(task_id) {
            let status_clone = status.clone();
            execution.status = status;
            
            if status_clone == TaskStatus::Running && execution.started_at.is_none() {
                execution.started_at = Some(std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs());
            } else if matches!(status_clone, TaskStatus::Success | TaskStatus::Failed | TaskStatus::Skipped | TaskStatus::UpstreamFailed) {
                execution.finished_at = Some(std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs());
                execution.duration = execution.finished_at.map(|f| f - execution.started_at.unwrap_or(f));
            }
        }
    }
    
    /// Check if run is running
    pub fn is_running(&self) -> bool {
        self.status == WorkflowRunStatus::Running
    }
    
    /// Check if run is finished
    pub fn is_finished(&self) -> bool {
        matches!(self.status, WorkflowRunStatus::Success | WorkflowRunStatus::Failed | WorkflowRunStatus::Cancelled)
    }
    
    /// Get run age in seconds
    pub fn get_age_seconds(&self) -> u64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        if let Some(finished) = self.finished_at {
            finished - self.started_at
        } else {
            now - self.started_at
        }
    }
    
    /// Get task execution count
    pub fn get_task_execution_count(&self) -> usize {
        self.task_executions.len()
    }
    
    /// Get successful task count
    pub fn get_successful_task_count(&self) -> usize {
        self.task_executions.values()
            .filter(|e| e.status == TaskStatus::Success)
            .count()
    }
    
    /// Get failed task count
    pub fn get_failed_task_count(&self) -> usize {
        self.task_executions.values()
            .filter(|e| e.status == TaskStatus::Failed)
            .count()
    }
    
    /// Get task execution success rate
    pub fn get_task_success_rate(&self) -> f64 {
        let total = self.task_executions.len();
        if total == 0 {
            return 0.0;
        }
        
        let successful = self.get_successful_task_count();
        (successful as f64 / total as f64) * 100.0
    }
}

/// Workflow
#[derive(Debug, Clone)]
pub struct Workflow {
    /// Workflow ID
    pub id: String,
    /// Workflow name
    pub name: String,
    /// Workflow description
    pub description: String,
    /// Workflow status
    pub status: WorkflowStatus,
    /// Workflow DAG
    pub dag: Option<DAG>,
    /// Workflow tasks
    pub tasks: std::collections::HashMap<String, Task>,
    /// Workflow runs
    pub runs: Vec<WorkflowRun>,
    /// Workflow created timestamp
    pub created_at: u64,
    /// Workflow updated timestamp
    pub updated_at: u64,
    /// Workflow owner
    pub owner: String,
    /// Workflow tags
    pub tags: Vec<String>,
}

impl Workflow {
    /// Create new workflow
    pub fn new(id: String, name: String) -> Self {
        Self {
            id,
            name,
            description: String::new(),
            status: WorkflowStatus::Draft,
            dag: None,
            tasks: std::collections::HashMap::new(),
            runs: vec![],
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            updated_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            owner: String::new(),
            tags: vec![],
        }
    }
    
    /// Set description
    pub fn set_description(&mut self, description: String) {
        self.description = description;
        self.update_timestamp();
    }
    
    /// Set DAG
    pub fn set_dag(&mut self, dag: DAG) {
        self.dag = Some(dag);
        self.update_timestamp();
    }
    
    /// Get DAG
    pub fn get_dag(&self) -> Option<&DAG> {
        self.dag.as_ref()
    }
    
    /// Get DAG mutably
    pub fn get_dag_mut(&mut self) -> Option<&mut DAG> {
        self.dag.as_mut()
    }
    
    /// Add task
    pub fn add_task(&mut self, task: Task) -> String {
        let id = task.id.clone();
        self.tasks.insert(id.clone(), task);
        self.update_timestamp();
        id
    }
    
    /// Remove task
    pub fn remove_task(&mut self, task_id: &str) -> bool {
        if self.tasks.remove(task_id).is_some() {
            self.update_timestamp();
            return true;
        }
        false
    }
    
    /// Get task
    pub fn get_task(&self, task_id: &str) -> Option<&Task> {
        self.tasks.get(task_id)
    }
    
    /// Get task mutably
    pub fn get_task_mut(&mut self, task_id: &str) -> Option<&mut Task> {
        self.tasks.get_mut(task_id)
    }
    
    /// Get all tasks
    pub fn get_tasks(&self) -> Vec<&Task> {
        self.tasks.values().collect()
    }
    
    /// Add run
    pub fn add_run(&mut self, run: WorkflowRun) {
        self.runs.push(run);
        self.update_timestamp();
    }
    
    /// Get run
    pub fn get_run(&self, run_id: &str) -> Option<&WorkflowRun> {
        self.runs.iter().find(|r| r.id == run_id)
    }
    
    /// Get run mutably
    pub fn get_run_mut(&mut self, run_id: &str) -> Option<&mut WorkflowRun> {
        self.runs.iter_mut().find(|r| r.id == run_id)
    }
    
    /// Get all runs
    pub fn get_runs(&self) -> &[WorkflowRun] {
        &self.runs
    }
    
    /// Get latest run
    pub fn get_latest_run(&self) -> Option<&WorkflowRun> {
        self.runs.last()
    }
    
    /// Activate workflow
    pub fn activate(&mut self) {
        self.status = WorkflowStatus::Active;
        self.update_timestamp();
    }
    
    /// Pause workflow
    pub fn pause(&mut self) {
        self.status = WorkflowStatus::Paused;
        self.update_timestamp();
    }
    
    /// Archive workflow
    pub fn archive(&mut self) {
        self.status = WorkflowStatus::Archived;
        self.update_timestamp();
    }
    
    /// Set owner
    pub fn set_owner(&mut self, owner: String) {
        self.owner = owner;
        self.update_timestamp();
    }
    
    /// Add tag
    pub fn add_tag(&mut self, tag: String) {
        if !self.tags.contains(&tag) {
            self.tags.push(tag);
            self.update_timestamp();
        }
    }
    
    /// Remove tag
    pub fn remove_tag(&mut self, tag: &str) -> bool {
        if let Some(pos) = self.tags.iter().position(|t| t == tag) {
            self.tags.remove(pos);
            self.update_timestamp();
            return true;
        }
        false
    }
    
    /// Update timestamp
    fn update_timestamp(&mut self) {
        self.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
    
    /// Check if workflow is active
    pub fn is_active(&self) -> bool {
        self.status == WorkflowStatus::Active
    }
    
    /// Check if workflow is paused
    pub fn is_paused(&self) -> bool {
        self.status == WorkflowStatus::Paused
    }
    
    /// Check if workflow is archived
    pub fn is_archived(&self) -> bool {
        self.status == WorkflowStatus::Archived
    }
    
    /// Get workflow age in seconds
    pub fn get_age_seconds(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() - self.created_at
    }
    
    /// Get time since update in seconds
    pub fn get_time_since_update(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() - self.updated_at
    }
    
    /// Get task count
    pub fn get_task_count(&self) -> usize {
        self.tasks.len()
    }
    
    /// Get run count
    pub fn get_run_count(&self) -> usize {
        self.runs.len()
    }
    
    /// Get successful run count
    pub fn get_successful_run_count(&self) -> usize {
        self.runs.iter()
            .filter(|r| r.status == WorkflowRunStatus::Success)
            .count()
    }
    
    /// Get failed run count
    pub fn get_failed_run_count(&self) -> usize {
        self.runs.iter()
            .filter(|r| r.status == WorkflowRunStatus::Failed)
            .count()
    }
    
    /// Get workflow success rate
    pub fn get_success_rate(&self) -> f64 {
        let total = self.runs.len();
        if total == 0 {
            return 0.0;
        }
        
        let successful = self.get_successful_run_count();
        (successful as f64 / total as f64) * 100.0
    }
    
    /// Get average run duration
    pub fn get_average_run_duration(&self) -> Option<u64> {
        let durations: Vec<u64> = self.runs.iter()
            .filter_map(|r| r.duration)
            .collect();
        
        if durations.is_empty() {
            None
        } else {
            Some(durations.iter().sum::<u64>() / durations.len() as u64)
        }
    }
}
