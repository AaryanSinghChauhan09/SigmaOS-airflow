//! Task definitions for airflow.

/// Task type
#[derive(Debug, Clone, PartialEq)]
pub enum TaskType {
    /// Python task
    Python,
    /// Bash task
    Bash,
    /// SQL task
    SQL,
    /// Email task
    Email,
    /// HTTP task
    HTTP,
    /// File task
    File,
    /// Custom task
    Custom,
}

/// Task status
#[derive(Debug, Clone, PartialEq)]
pub enum TaskStatus {
    /// Pending
    Pending,
    /// Running
    Running,
    /// Success
    Success,
    /// Failed
    Failed,
    /// Skipped
    Skipped,
    /// Upstream failed
    UpstreamFailed,
}

/// Task dependency
#[derive(Debug, Clone)]
pub struct TaskDependency {
    /// Dependency task ID
    pub task_id: String,
    /// Dependency type
    pub dependency_type: DependencyType,
}

/// Dependency type
#[derive(Debug, Clone, PartialEq)]
pub enum DependencyType {
    /// Success dependency
    Success,
    /// Failure dependency
    Failure,
    /// Always dependency
    Always,
    /// One success dependency
    OneSuccess,
    /// All success dependency
    AllSuccess,
}

/// Task
#[derive(Debug, Clone)]
pub struct Task {
    /// Task ID
    pub id: String,
    /// Task name
    pub name: String,
    /// Task type
    pub task_type: TaskType,
    /// Task description
    pub description: String,
    /// Task status
    pub status: TaskStatus,
    /// Task dependencies
    pub dependencies: Vec<TaskDependency>,
    /// Task parameters
    pub parameters: std::collections::HashMap<String, String>,
    /// Task retries
    pub retries: u32,
    /// Task retry delay in seconds
    pub retry_delay: u64,
    /// Task timeout in seconds
    pub timeout: u64,
    /// Task created timestamp
    pub created_at: u64,
    /// Task started timestamp
    pub started_at: Option<u64>,
    /// Task finished timestamp
    pub finished_at: Option<u64>,
    /// Task error message
    pub error_message: Option<String>,
    /// Task log output
    pub log_output: String,
}

impl Task {
    /// Create new task
    pub fn new(id: String, name: String, task_type: TaskType) -> Self {
        Self {
            id,
            name,
            task_type,
            description: String::new(),
            status: TaskStatus::Pending,
            dependencies: vec![],
            parameters: std::collections::HashMap::new(),
            retries: 0,
            retry_delay: 60,
            timeout: 3600,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            started_at: None,
            finished_at: None,
            error_message: None,
            log_output: String::new(),
        }
    }
    
    /// Set description
    pub fn set_description(&mut self, description: String) {
        self.description = description;
    }
    
    /// Add dependency
    pub fn add_dependency(&mut self, task_id: String, dependency_type: DependencyType) {
        self.dependencies.push(TaskDependency {
            task_id,
            dependency_type,
        });
    }
    
    /// Remove dependency
    pub fn remove_dependency(&mut self, task_id: &str) -> bool {
        if let Some(pos) = self.dependencies.iter().position(|d| d.task_id == task_id) {
            self.dependencies.remove(pos);
            return true;
        }
        false
    }
    
    /// Set parameter
    pub fn set_parameter(&mut self, key: String, value: String) {
        self.parameters.insert(key, value);
    }
    
    /// Get parameter
    pub fn get_parameter(&self, key: &str) -> Option<&String> {
        self.parameters.get(key)
    }
    
    /// Set retries
    pub fn set_retries(&mut self, retries: u32) {
        self.retries = retries;
    }
    
    /// Set retry delay
    pub fn set_retry_delay(&mut self, delay: u64) {
        self.retry_delay = delay;
    }
    
    /// Set timeout
    pub fn set_timeout(&mut self, timeout: u64) {
        self.timeout = timeout;
    }
    
    /// Start task
    pub fn start(&mut self) -> bool {
        if self.status == TaskStatus::Pending {
            self.status = TaskStatus::Running;
            self.started_at = Some(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs());
            return true;
        }
        false
    }
    
    /// Mark task as success
    pub fn mark_success(&mut self) {
        self.status = TaskStatus::Success;
        self.finished_at = Some(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs());
    }
    
    /// Mark task as failed
    pub fn mark_failed(&mut self, error_message: String) {
        self.status = TaskStatus::Failed;
        self.finished_at = Some(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs());
        self.error_message = Some(error_message);
    }
    
    /// Mark task as skipped
    pub fn mark_skipped(&mut self) {
        self.status = TaskStatus::Skipped;
        self.finished_at = Some(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs());
    }
    
    /// Mark task as upstream failed
    pub fn mark_upstream_failed(&mut self) {
        self.status = TaskStatus::UpstreamFailed;
        self.finished_at = Some(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs());
    }
    
    /// Add log output
    pub fn add_log(&mut self, log: String) {
        self.log_output.push_str(&log);
        self.log_output.push('\n');
    }
    
    /// Set log output
    pub fn set_log_output(&mut self, log: String) {
        self.log_output = log;
    }
    
    /// Check if task is pending
    pub fn is_pending(&self) -> bool {
        self.status == TaskStatus::Pending
    }
    
    /// Check if task is running
    pub fn is_running(&self) -> bool {
        self.status == TaskStatus::Running
    }
    
    /// Check if task is successful
    pub fn is_success(&self) -> bool {
        self.status == TaskStatus::Success
    }
    
    /// Check if task is failed
    pub fn is_failed(&self) -> bool {
        self.status == TaskStatus::Failed
    }
    
    /// Check if task is finished
    pub fn is_finished(&self) -> bool {
        matches!(self.status, TaskStatus::Success | TaskStatus::Failed | TaskStatus::Skipped | TaskStatus::UpstreamFailed)
    }
    
    /// Get task duration in seconds
    pub fn get_duration_seconds(&self) -> Option<u64> {
        if let (Some(started), Some(finished)) = (self.started_at, self.finished_at) {
            Some(finished - started)
        } else {
            None
        }
    }
    
    /// Get task age in seconds
    pub fn get_age_seconds(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() - self.created_at
    }
    
    /// Check if task has dependencies
    pub fn has_dependencies(&self) -> bool {
        !self.dependencies.is_empty()
    }
    
    /// Get dependency count
    pub fn get_dependency_count(&self) -> usize {
        self.dependencies.len()
    }
    
    /// Check if task can run (all dependencies satisfied)
    pub fn can_run(&self, dependency_statuses: &std::collections::HashMap<String, TaskStatus>) -> bool {
        for dep in &self.dependencies {
            if let Some(status) = dependency_statuses.get(&dep.task_id) {
                let can_run = match dep.dependency_type {
                    DependencyType::Success => *status == TaskStatus::Success,
                    DependencyType::Failure => *status == TaskStatus::Failed,
                    DependencyType::Always => true,
                    DependencyType::OneSuccess => *status == TaskStatus::Success,
                    DependencyType::AllSuccess => *status == TaskStatus::Success,
                };
                if !can_run {
                    return false;
                }
            } else {
                // Dependency not found
                return false;
            }
        }
        true
    }
}
