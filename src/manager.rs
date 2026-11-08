//! Workflow manager for airflow.

use std::collections::HashMap;
use crate::dag::DAG;
use crate::task::{Task, TaskStatus};
use crate::workflow::{Workflow, WorkflowStatus, WorkflowRun, WorkflowRunStatus};
use crate::scheduler::{Scheduler, ScheduleType};
use crate::executor::{Executor, ExecutionStatus};

/// Manager options
#[derive(Debug, Clone)]
pub struct ManagerOptions {
    /// Auto-start scheduler
    pub auto_start_scheduler: bool,
    /// Auto-start executor
    pub auto_start_executor: bool,
    /// Max concurrent workflows
    pub max_concurrent_workflows: usize,
    /// Max workflow retries
    pub max_workflow_retries: u32,
    /// Workflow timeout in seconds
    pub workflow_timeout: u64,
    /// Enable workflow notifications
    pub enable_notifications: bool,
}

impl Default for ManagerOptions {
    fn default() -> Self {
        Self {
            auto_start_scheduler: true,
            auto_start_executor: true,
            max_concurrent_workflows: 4,
            max_workflow_retries: 3,
            workflow_timeout: 3600,
            enable_notifications: true,
        }
    }
}

/// Manager statistics
#[derive(Debug, Clone)]
pub struct ManagerStatistics {
    /// Total workflows
    pub total_workflows: usize,
    /// Total runs
    pub total_runs: usize,
    /// Active workflows
    pub active_workflows: usize,
    /// Paused workflows
    pub paused_workflows: usize,
    /// Archived workflows
    pub archived_workflows: usize,
    /// Successful runs
    pub successful_runs: usize,
    /// Failed runs
    pub failed_runs: usize,
    /// Running runs
    pub running_runs: usize,
    /// Total tasks
    pub total_tasks: usize,
    /// Successful tasks
    pub successful_tasks: usize,
    /// Failed tasks
    pub failed_tasks: usize,
}

/// Workflow manager
#[derive(Debug, Clone)]
pub struct WorkflowManager {
    /// Workflows
    workflows: HashMap<String, Workflow>,
    /// Scheduler
    pub scheduler: Scheduler,
    /// Executor
    pub executor: Executor,
    /// Options
    options: ManagerOptions,
    /// Statistics
    statistics: ManagerStatistics,
}

impl Default for WorkflowManager {
    fn default() -> Self {
        Self {
            workflows: HashMap::new(),
            scheduler: Scheduler::new(),
            executor: Executor::new(),
            options: ManagerOptions::default(),
            statistics: ManagerStatistics {
                total_workflows: 0,
                total_runs: 0,
                active_workflows: 0,
                paused_workflows: 0,
                archived_workflows: 0,
                successful_runs: 0,
                failed_runs: 0,
                running_runs: 0,
                total_tasks: 0,
                successful_tasks: 0,
                failed_tasks: 0,
            },
        }
    }
}

impl WorkflowManager {
    /// Create new workflow manager
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Create manager with options
    pub fn with_options(options: ManagerOptions) -> Self {
        let mut manager = Self::default();
        manager.options = options;
        manager
    }
    
    /// Initialize manager
    pub fn initialize(&mut self) {
        if self.options.auto_start_scheduler {
            self.scheduler.start();
        }
        if self.options.auto_start_executor {
            self.executor.start();
        }
        self.update_statistics();
    }
    
    /// Create workflow
    pub fn create_workflow(&mut self, name: String) -> String {
        let id = format!("workflow-{}", self.workflows.len() + 1);
        let workflow = Workflow::new(id.clone(), name);
        
        self.workflows.insert(id.clone(), workflow);
        self.update_statistics();
        id
    }
    
    /// Get workflow
    pub fn get_workflow(&self, workflow_id: &str) -> Option<&Workflow> {
        self.workflows.get(workflow_id)
    }
    
    /// Get workflow mutably
    pub fn get_workflow_mut(&mut self, workflow_id: &str) -> Option<&mut Workflow> {
        self.workflows.get_mut(workflow_id)
    }
    
    /// Get all workflows
    pub fn list_workflows(&self) -> Vec<&Workflow> {
        self.workflows.values().collect()
    }
    
    /// Delete workflow
    pub fn delete_workflow(&mut self, workflow_id: &str) -> bool {
        if self.workflows.remove(workflow_id).is_some() {
            // Remove schedules for this workflow
            self.scheduler.schedules.retain(|_, s| s.workflow_id != workflow_id);
            self.update_statistics();
            return true;
        }
        false
    }
    
    /// Activate workflow
    pub fn activate_workflow(&mut self, workflow_id: &str) -> bool {
        if let Some(workflow) = self.workflows.get_mut(workflow_id) {
            workflow.activate();
            self.update_statistics();
            return true;
        }
        false
    }
    
    /// Pause workflow
    pub fn pause_workflow(&mut self, workflow_id: &str) -> bool {
        if let Some(workflow) = self.workflows.get_mut(workflow_id) {
            workflow.pause();
            self.update_statistics();
            return true;
        }
        false
    }
    
    /// Archive workflow
    pub fn archive_workflow(&mut self, workflow_id: &str) -> bool {
        if let Some(workflow) = self.workflows.get_mut(workflow_id) {
            workflow.archive();
            self.update_statistics();
            return true;
        }
        false
    }
    
    /// Add task to workflow
    pub fn add_task_to_workflow(&mut self, workflow_id: &str, task: Task) -> bool {
        if let Some(workflow) = self.workflows.get_mut(workflow_id) {
            workflow.add_task(task);
            self.update_statistics();
            return true;
        }
        false
    }
    
    /// Remove task from workflow
    pub fn remove_task_from_workflow(&mut self, workflow_id: &str, task_id: &str) -> bool {
        if let Some(workflow) = self.workflows.get_mut(workflow_id) {
            if workflow.remove_task(task_id) {
                self.update_statistics();
                return true;
            }
        }
        false
    }
    
    /// Set DAG for workflow
    pub fn set_workflow_dag(&mut self, workflow_id: &str, dag: DAG) -> bool {
        if let Some(workflow) = self.workflows.get_mut(workflow_id) {
            workflow.set_dag(dag);
            return true;
        }
        false
    }
    
    /// Run workflow
    pub fn run_workflow(&mut self, workflow_id: String) -> String {
        let run_id = format!("run-{}", self.get_total_runs() + 1);
        let mut run = WorkflowRun::new(run_id.clone(), workflow_id.clone());
        run.triggered_by = "manual".into();
        
        // Add task executions for all tasks in workflow
        if let Some(workflow) = self.workflows.get(&workflow_id) {
            for task_id in workflow.tasks.keys() {
                run.add_task_execution(task_id.clone(), TaskStatus::Pending);
            }
        }
        
        // Start run
        run.start();
        
        // Execute tasks
        self.execute_workflow_run(&mut run, &workflow_id);
        
        // Add run to workflow
        if let Some(workflow) = self.workflows.get_mut(&workflow_id) {
            workflow.add_run(run);
        }
        
        self.update_statistics();
        run_id
    }
    
    /// Execute workflow run
    fn execute_workflow_run(&mut self, run: &mut WorkflowRun, workflow_id: &str) {
        // Get workflow tasks
        let tasks: Vec<Task> = if let Some(workflow) = self.workflows.get(workflow_id) {
            workflow.tasks.values().cloned().collect()
        } else {
            return;
        };
        
        // Execute tasks in topological order (simplified)
        for task in tasks {
            // Update task execution status
            run.update_task_execution(&task.id, TaskStatus::Running);
            
            // Execute task
            let execution_id = self.executor.execute_task(task.clone());
            
            // Update task execution status based on execution result
            if let Some(execution) = self.executor.get_execution(&execution_id) {
                let task_status = match execution.status {
                    ExecutionStatus::Success => TaskStatus::Success,
                    ExecutionStatus::Failed => TaskStatus::Failed,
                    ExecutionStatus::Cancelled => TaskStatus::Skipped,
                    _ => TaskStatus::Running,
                };
                run.update_task_execution(&task.id, task_status);
            }
        }
        
        // Mark run as finished
        let all_success = run.task_executions.values()
            .all(|e| e.status == TaskStatus::Success);
        
        if all_success {
            run.mark_success();
        } else {
            let failed_count = run.get_failed_task_count();
            run.mark_failed(format!("{} tasks failed", failed_count));
        }
    }
    
    /// Get workflow run
    pub fn get_workflow_run(&self, run_id: &str) -> Option<&WorkflowRun> {
        // Search all workflows for the run
        for workflow in self.workflows.values() {
            if let Some(run) = workflow.get_run(run_id) {
                return Some(run);
            }
        }
        None
    }
    
    /// Get workflow run mutably
    pub fn get_workflow_run_mut(&mut self, run_id: &str) -> Option<&mut WorkflowRun> {
        // Search all workflows for the run
        for workflow in self.workflows.values_mut() {
            if let Some(run) = workflow.get_run_mut(run_id) {
                return Some(run);
            }
        }
        None
    }
    
    /// Get all workflow runs
    pub fn list_workflow_runs(&self) -> Vec<&WorkflowRun> {
        let mut runs = vec![];
        for workflow in self.workflows.values() {
            runs.extend(workflow.get_runs());
        }
        runs
    }
    
    /// Cancel workflow run
    pub fn cancel_workflow_run(&mut self, run_id: &str) -> bool {
        if let Some(run) = self.get_workflow_run_mut(run_id) {
            if run.is_running() {
                run.mark_cancelled();
                return true;
            }
        }
        false
    }
    
    /// Create schedule for workflow
    pub fn create_schedule_for_workflow(&mut self, workflow_id: &str, name: String, schedule_type: ScheduleType) -> Option<String> {
        if self.workflows.contains_key(workflow_id) {
            let schedule_id = self.scheduler.create_schedule_for_workflow(name, schedule_type, workflow_id.to_string());
            Some(schedule_id)
        } else {
            None
        }
    }
    
    /// Search workflows
    pub fn search_workflows(&self, query: &str) -> Vec<&Workflow> {
        let query_lower = query.to_lowercase();
        self.workflows.values()
            .filter(|w| w.name.to_lowercase().contains(&query_lower) || 
                       w.description.to_lowercase().contains(&query_lower))
            .collect()
    }
    
    /// Get workflows by status
    pub fn get_workflows_by_status(&self, status: &WorkflowStatus) -> Vec<&Workflow> {
        self.workflows.values()
            .filter(|w| w.status == *status)
            .collect()
    }
    
    /// Get workflows by owner
    pub fn get_workflows_by_owner(&self, owner: &str) -> Vec<&Workflow> {
        self.workflows.values()
            .filter(|w| w.owner == owner)
            .collect()
    }
    
    /// Get workflows by tag
    pub fn get_workflows_by_tag(&self, tag: &str) -> Vec<&Workflow> {
        self.workflows.values()
            .filter(|w| w.tags.contains(&tag.to_string()))
            .collect()
    }
    
    /// Get total runs
    fn get_total_runs(&self) -> usize {
        self.workflows.values()
            .map(|w| w.get_run_count())
            .sum()
    }
    
    /// Update statistics
    fn update_statistics(&mut self) {
        self.statistics.total_workflows = self.workflows.len();
        self.statistics.total_runs = self.get_total_runs();
        
        self.statistics.active_workflows = self.workflows.values()
            .filter(|w| w.is_active())
            .count();
        
        self.statistics.paused_workflows = self.workflows.values()
            .filter(|w| w.is_paused())
            .count();
        
        self.statistics.archived_workflows = self.workflows.values()
            .filter(|w| w.is_archived())
            .count();
        
        // Count runs by status
        let (successful_runs, failed_runs, running_runs) = {
            let all_runs: Vec<&WorkflowRun> = self.list_workflow_runs();
            let successful = all_runs.iter()
                .filter(|r| r.status == WorkflowRunStatus::Success)
                .count();
            let failed = all_runs.iter()
                .filter(|r| r.status == WorkflowRunStatus::Failed)
                .count();
            let running = all_runs.iter()
                .filter(|r| r.is_running())
                .count();
            (successful, failed, running)
        };
        
        self.statistics.successful_runs = successful_runs;
        self.statistics.failed_runs = failed_runs;
        self.statistics.running_runs = running_runs;
        
        // Count tasks
        self.statistics.total_tasks = self.workflows.values()
            .map(|w| w.get_task_count())
            .sum();
        
        // Count task statuses from runs
        let mut successful_tasks = 0;
        let mut failed_tasks = 0;
        
        for workflow in self.workflows.values() {
            for run in &workflow.runs {
                for execution in run.task_executions.values() {
                    match execution.status {
                        TaskStatus::Success => successful_tasks += 1,
                        TaskStatus::Failed => failed_tasks += 1,
                        _ => {}
                    }
                }
            }
        }
        
        self.statistics.successful_tasks = successful_tasks;
        self.statistics.failed_tasks = failed_tasks;
    }
    
    /// Get statistics
    pub fn get_statistics(&self) -> &ManagerStatistics {
        &self.statistics
    }
    
    /// Export manager data
    pub fn export_data(&self) -> ManagerExport {
        ManagerExport {
            workflows: self.workflows.values().cloned().collect(),
            scheduler_schedules: self.scheduler.schedules.values().cloned().collect(),
            executor_executions: self.executor.executions.values().cloned().collect(),
        }
    }
    
    /// Import manager data
    pub fn import_data(&mut self, export: ManagerExport) {
        self.workflows.clear();
        for workflow in export.workflows {
            self.workflows.insert(workflow.id.clone(), workflow);
        }
        
        self.scheduler.schedules.clear();
        for schedule in export.scheduler_schedules {
            self.scheduler.schedules.insert(schedule.id.clone(), schedule);
        }
        
        self.executor.executions.clear();
        for execution in export.executor_executions {
            self.executor.executions.insert(execution.id.clone(), execution);
        }
        
        self.update_statistics();
    }
}

/// Manager export data
#[derive(Debug, Clone)]
pub struct ManagerExport {
    /// Workflows
    pub workflows: Vec<Workflow>,
    /// Scheduler schedules
    pub scheduler_schedules: Vec<crate::scheduler::Schedule>,
    /// Executor executions
    pub executor_executions: Vec<crate::executor::ExecutionResult>,
}
