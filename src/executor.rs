//! Executor for airflow.

use crate::task::{Task, TaskStatus};

/// Execution status
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionStatus {
    /// Pending
    Pending,
    /// Running
    Running,
    /// Success
    Success,
    /// Failed
    Failed,
    /// Cancelled
    Cancelled,
}

/// Execution result
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    /// Execution ID
    pub id: String,
    /// Task ID
    pub task_id: String,
    /// Execution status
    pub status: ExecutionStatus,
    /// Execution started timestamp
    pub started_at: u64,
    /// Execution finished timestamp
    pub finished_at: Option<u64>,
    /// Execution duration in seconds
    pub duration: Option<u64>,
    /// Execution output
    pub output: String,
    /// Execution error message
    pub error_message: Option<String>,
    /// Execution exit code
    pub exit_code: Option<i32>,
    /// Execution retries
    pub retries: u32,
    /// Execution logs
    pub logs: Vec<String>,
}

impl ExecutionResult {
    /// Create new execution result
    pub fn new(id: String, task_id: String) -> Self {
        Self {
            id,
            task_id,
            status: ExecutionStatus::Pending,
            started_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            finished_at: None,
            duration: None,
            output: String::new(),
            error_message: None,
            exit_code: None,
            retries: 0,
            logs: vec![],
        }
    }
    
    /// Start execution
    pub fn start(&mut self) {
        self.status = ExecutionStatus::Running;
        self.started_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
    
    /// Mark execution as success
    pub fn mark_success(&mut self, output: String, exit_code: i32) {
        self.status = ExecutionStatus::Success;
        self.finished_at = Some(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs());
        self.duration = self.finished_at.map(|f| f - self.started_at);
        self.output = output;
        self.exit_code = Some(exit_code);
    }
    
    /// Mark execution as failed
    pub fn mark_failed(&mut self, error_message: String, exit_code: i32) {
        self.status = ExecutionStatus::Failed;
        self.finished_at = Some(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs());
        self.duration = self.finished_at.map(|f| f - self.started_at);
        self.error_message = Some(error_message);
        self.exit_code = Some(exit_code);
    }
    
    /// Mark execution as cancelled
    pub fn mark_cancelled(&mut self) {
        self.status = ExecutionStatus::Cancelled;
        self.finished_at = Some(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs());
        self.duration = self.finished_at.map(|f| f - self.started_at);
    }
    
    /// Add log
    pub fn add_log(&mut self, log: String) {
        self.logs.push(log);
    }
    
    /// Set output
    pub fn set_output(&mut self, output: String) {
        self.output = output;
    }
    
    /// Increment retries
    pub fn increment_retries(&mut self) {
        self.retries += 1;
    }
    
    /// Check if execution is running
    pub fn is_running(&self) -> bool {
        self.status == ExecutionStatus::Running
    }
    
    /// Check if execution is finished
    pub fn is_finished(&self) -> bool {
        matches!(self.status, ExecutionStatus::Success | ExecutionStatus::Failed | ExecutionStatus::Cancelled)
    }
    
    /// Check if execution is successful
    pub fn is_success(&self) -> bool {
        self.status == ExecutionStatus::Success
    }
    
    /// Check if execution is failed
    pub fn is_failed(&self) -> bool {
        self.status == ExecutionStatus::Failed
    }
    
    /// Get execution age in seconds
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
}

/// Executor
#[derive(Debug, Clone)]
pub struct Executor {
    /// Executions
    pub executions: std::collections::HashMap<String, ExecutionResult>,
    /// Executor running
    pub running: bool,
    /// Max concurrent executions
    pub max_concurrent: usize,
    /// Current concurrent executions
    pub current_concurrent: usize,
}

impl Default for Executor {
    fn default() -> Self {
        Self {
            executions: std::collections::HashMap::new(),
            running: false,
            max_concurrent: 4,
            current_concurrent: 0,
        }
    }
}

impl Executor {
    /// Create new executor
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Start executor
    pub fn start(&mut self) {
        self.running = true;
    }
    
    /// Stop executor
    pub fn stop(&mut self) {
        self.running = false;
    }
    
    /// Execute task
    pub fn execute_task(&mut self, task: Task) -> String {
        let execution_id = format!("exec-{}", self.executions.len() + 1);
        let mut execution = ExecutionResult::new(execution_id.clone(), task.id.clone());
        
        // Simulate task execution
        execution.start();
        
        // Simulate execution based on task type
        match task.task_type {
            crate::task::TaskType::Python => {
                execution.add_log("Running Python script...".into());
                execution.add_log("Python script completed successfully".into());
                execution.mark_success("Python output".into(), 0);
            }
            crate::task::TaskType::Bash => {
                execution.add_log("Running Bash command...".into());
                execution.add_log("Bash command completed successfully".into());
                execution.mark_success("Bash output".into(), 0);
            }
            crate::task::TaskType::SQL => {
                execution.add_log("Executing SQL query...".into());
                execution.add_log("SQL query completed successfully".into());
                execution.mark_success("SQL results".into(), 0);
            }
            crate::task::TaskType::Email => {
                execution.add_log("Sending email...".into());
                execution.add_log("Email sent successfully".into());
                execution.mark_success("Email sent".into(), 0);
            }
            crate::task::TaskType::HTTP => {
                execution.add_log("Making HTTP request...".into());
                execution.add_log("HTTP request completed successfully".into());
                execution.mark_success("HTTP response".into(), 0);
            }
            crate::task::TaskType::File => {
                execution.add_log("Processing file...".into());
                execution.add_log("File processed successfully".into());
                execution.mark_success("File processed".into(), 0);
            }
            crate::task::TaskType::Custom => {
                execution.add_log("Running custom task...".into());
                execution.add_log("Custom task completed successfully".into());
                execution.mark_success("Custom output".into(), 0);
            }
        }
        
        self.executions.insert(execution_id.clone(), execution);
        execution_id
    }
    
    /// Get execution
    pub fn get_execution(&self, execution_id: &str) -> Option<&ExecutionResult> {
        self.executions.get(execution_id)
    }
    
    /// Get execution mutably
    pub fn get_execution_mut(&mut self, execution_id: &str) -> Option<&mut ExecutionResult> {
        self.executions.get_mut(execution_id)
    }
    
    /// Get all executions
    pub fn get_executions(&self) -> Vec<&ExecutionResult> {
        self.executions.values().collect()
    }
    
    /// Delete execution
    pub fn delete_execution(&mut self, execution_id: &str) -> bool {
        self.executions.remove(execution_id).is_some()
    }
    
    /// Cancel execution
    pub fn cancel_execution(&mut self, execution_id: &str) -> bool {
        if let Some(execution) = self.executions.get_mut(execution_id) {
            if execution.is_running() {
                execution.mark_cancelled();
                return true;
            }
        }
        false
    }
    
    /// Retry execution
    pub fn retry_execution(&mut self, execution_id: &str) -> bool {
        if let Some(execution) = self.executions.get_mut(execution_id) {
            if execution.is_failed() {
                execution.increment_retries();
                execution.status = ExecutionStatus::Pending;
                execution.finished_at = None;
                execution.duration = None;
                execution.error_message = None;
                return true;
            }
        }
        false
    }
    
    /// Get executions by status
    pub fn get_executions_by_status(&self, status: &ExecutionStatus) -> Vec<&ExecutionResult> {
        self.executions.values()
            .filter(|e| e.status == *status)
            .collect()
    }
    
    /// Get executions by task
    pub fn get_executions_by_task(&self, task_id: &str) -> Vec<&ExecutionResult> {
        self.executions.values()
            .filter(|e| e.task_id == task_id)
            .collect()
    }
    
    /// Get running executions
    pub fn get_running_executions(&self) -> Vec<&ExecutionResult> {
        self.get_executions_by_status(&ExecutionStatus::Running)
    }
    
    /// Get successful executions
    pub fn get_successful_executions(&self) -> Vec<&ExecutionResult> {
        self.get_executions_by_status(&ExecutionStatus::Success)
    }
    
    /// Get failed executions
    pub fn get_failed_executions(&self) -> Vec<&ExecutionResult> {
        self.get_executions_by_status(&ExecutionStatus::Failed)
    }
    
    /// Get executor statistics
    pub fn get_statistics(&self) -> ExecutorStatistics {
        ExecutorStatistics {
            total_executions: self.executions.len(),
            running_executions: self.executions.values().filter(|e| e.is_running()).count(),
            successful_executions: self.executions.values().filter(|e| e.is_success()).count(),
            failed_executions: self.executions.values().filter(|e| e.is_failed()).count(),
            cancelled_executions: self.executions.values().filter(|e| e.status == ExecutionStatus::Cancelled).count(),
            total_retries: self.executions.values().map(|e| e.retries).sum(),
        }
    }
}

/// Executor statistics
#[derive(Debug, Clone)]
pub struct ExecutorStatistics {
    /// Total executions
    pub total_executions: usize,
    /// Running executions
    pub running_executions: usize,
    /// Successful executions
    pub successful_executions: usize,
    /// Failed executions
    pub failed_executions: usize,
    /// Cancelled executions
    pub cancelled_executions: usize,
    /// Total retries
    pub total_retries: u32,
}
