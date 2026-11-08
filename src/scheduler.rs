//! Scheduler for airflow.

use crate::workflow::{Workflow, WorkflowRun};

/// Schedule type
#[derive(Debug, Clone, PartialEq)]
pub enum ScheduleType {
    /// Manual schedule
    Manual,
    /// Cron schedule
    Cron,
    /// Interval schedule
    Interval,
    /// Event schedule
    Event,
    /// Trigger schedule
    Trigger,
}

/// Schedule status
#[derive(Debug, Clone, PartialEq)]
pub enum ScheduleStatus {
    /// Active
    Active,
    /// Paused
    Paused,
    /// Disabled
    Disabled,
}

/// Schedule
#[derive(Debug, Clone)]
pub struct Schedule {
    /// Schedule ID
    pub id: String,
    /// Schedule name
    pub name: String,
    /// Schedule type
    pub schedule_type: ScheduleType,
    /// Schedule status
    pub status: ScheduleStatus,
    /// Schedule workflow ID
    pub workflow_id: String,
    /// Schedule cron expression
    pub cron_expression: Option<String>,
    /// Schedule interval in seconds
    pub interval: Option<u64>,
    /// Schedule next run timestamp
    pub next_run: Option<u64>,
    /// Schedule last run timestamp
    pub last_run: Option<u64>,
    /// Schedule run count
    pub run_count: u64,
    /// Schedule created timestamp
    pub created_at: u64,
}

impl Schedule {
    /// Create new schedule
    pub fn new(id: String, name: String, schedule_type: ScheduleType, workflow_id: String) -> Self {
        Self {
            id,
            name,
            schedule_type,
            status: ScheduleStatus::Active,
            workflow_id,
            cron_expression: None,
            interval: None,
            next_run: None,
            last_run: None,
            run_count: 0,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
    
    /// Set cron expression
    pub fn set_cron_expression(&mut self, expression: String) {
        self.cron_expression = Some(expression);
    }
    
    /// Set interval
    pub fn set_interval(&mut self, interval: u64) {
        self.interval = Some(interval);
    }
    
    /// Set next run
    pub fn set_next_run(&mut self, next_run: u64) {
        self.next_run = Some(next_run);
    }
    
    /// Set last run
    pub fn set_last_run(&mut self, last_run: u64) {
        self.last_run = Some(last_run);
    }
    
    /// Increment run count
    pub fn increment_run_count(&mut self) {
        self.run_count += 1;
    }
    
    /// Activate schedule
    pub fn activate(&mut self) {
        self.status = ScheduleStatus::Active;
    }
    
    /// Pause schedule
    pub fn pause(&mut self) {
        self.status = ScheduleStatus::Paused;
    }
    
    /// Disable schedule
    pub fn disable(&mut self) {
        self.status = ScheduleStatus::Disabled;
    }
    
    /// Check if schedule is active
    pub fn is_active(&self) -> bool {
        self.status == ScheduleStatus::Active
    }
    
    /// Check if schedule is paused
    pub fn is_paused(&self) -> bool {
        self.status == ScheduleStatus::Paused
    }
    
    /// Check if schedule is disabled
    pub fn is_disabled(&self) -> bool {
        self.status == ScheduleStatus::Disabled
    }
    
    /// Check if schedule is due
    pub fn is_due(&self) -> bool {
        if let Some(next_run) = self.next_run {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            now >= next_run
        } else {
            false
        }
    }
    
    /// Get schedule age in seconds
    pub fn get_age_seconds(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() - self.created_at
    }
    
    /// Get time until next run in seconds
    pub fn get_time_until_next_run(&self) -> Option<u64> {
        if let Some(next_run) = self.next_run {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            Some(next_run.saturating_sub(now))
        } else {
            None
        }
    }
}

/// Scheduler
#[derive(Debug, Clone)]
pub struct Scheduler {
    /// Schedules
    pub schedules: std::collections::HashMap<String, Schedule>,
    /// Scheduler running
    pub running: bool,
    /// Scheduler tick interval in seconds
    pub tick_interval: u64,
    /// Scheduler last tick timestamp
    pub last_tick: u64,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self {
            schedules: std::collections::HashMap::new(),
            running: false,
            tick_interval: 60,
            last_tick: 0,
        }
    }
}

impl Scheduler {
    /// Create new scheduler
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Start scheduler
    pub fn start(&mut self) {
        self.running = true;
        self.last_tick = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
    
    /// Stop scheduler
    pub fn stop(&mut self) {
        self.running = false;
    }
    
    /// Create schedule
    pub fn create_schedule(&mut self, name: String, schedule_type: ScheduleType) -> String {
        let id = format!("schedule-{}", self.schedules.len() + 1);
        let schedule = Schedule::new(id.clone(), name, schedule_type, String::new());
        
        self.schedules.insert(id.clone(), schedule);
        id
    }
    
    /// Create schedule for workflow
    pub fn create_schedule_for_workflow(&mut self, name: String, schedule_type: ScheduleType, workflow_id: String) -> String {
        let id = format!("schedule-{}", self.schedules.len() + 1);
        let schedule = Schedule::new(id.clone(), name, schedule_type, workflow_id);
        
        self.schedules.insert(id.clone(), schedule);
        id
    }
    
    /// Get schedule
    pub fn get_schedule(&self, schedule_id: &str) -> Option<&Schedule> {
        self.schedules.get(schedule_id)
    }
    
    /// Get schedule mutably
    pub fn get_schedule_mut(&mut self, schedule_id: &str) -> Option<&mut Schedule> {
        self.schedules.get_mut(schedule_id)
    }
    
    /// Get all schedules
    pub fn list_schedules(&self) -> Vec<&Schedule> {
        self.schedules.values().collect()
    }
    
    /// Delete schedule
    pub fn delete_schedule(&mut self, schedule_id: &str) -> bool {
        self.schedules.remove(schedule_id).is_some()
    }
    
    /// Activate schedule
    pub fn activate_schedule(&mut self, schedule_id: &str) -> bool {
        if let Some(schedule) = self.schedules.get_mut(schedule_id) {
            schedule.activate();
            return true;
        }
        false
    }
    
    /// Pause schedule
    pub fn pause_schedule(&mut self, schedule_id: &str) -> bool {
        if let Some(schedule) = self.schedules.get_mut(schedule_id) {
            schedule.pause();
            return true;
        }
        false
    }
    
    /// Disable schedule
    pub fn disable_schedule(&mut self, schedule_id: &str) -> bool {
        if let Some(schedule) = self.schedules.get_mut(schedule_id) {
            schedule.disable();
            return true;
        }
        false
    }
    
    /// Tick scheduler
    pub fn tick(&mut self) -> Vec<String> {
        let mut due_schedules = vec![];
        
        if !self.running {
            return due_schedules;
        }
        
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Check if enough time has passed since last tick
        if now - self.last_tick < self.tick_interval {
            return due_schedules;
        }
        
        self.last_tick = now;
        
        // Find due schedules
        for schedule in self.schedules.values_mut() {
            if schedule.is_active() && schedule.is_due() {
                schedule.increment_run_count();
                schedule.set_last_run(now);
                
                // Calculate next run time
                if let Some(interval) = schedule.interval {
                    schedule.set_next_run(now + interval);
                }
                
                due_schedules.push(schedule.id.clone());
            }
        }
        
        due_schedules
    }
    
    /// Get due schedules
    pub fn get_due_schedules(&self) -> Vec<&Schedule> {
        self.schedules.values()
            .filter(|s| s.is_active() && s.is_due())
            .collect()
    }
    
    /// Get schedules by workflow
    pub fn get_schedules_by_workflow(&self, workflow_id: &str) -> Vec<&Schedule> {
        self.schedules.values()
            .filter(|s| s.workflow_id == workflow_id)
            .collect()
    }
    
    /// Get schedules by type
    pub fn get_schedules_by_type(&self, schedule_type: &ScheduleType) -> Vec<&Schedule> {
        self.schedules.values()
            .filter(|s| s.schedule_type == *schedule_type)
            .collect()
    }
    
    /// Get active schedules
    pub fn get_active_schedules(&self) -> Vec<&Schedule> {
        self.schedules.values()
            .filter(|s| s.is_active())
            .collect()
    }
    
    /// Get schedule statistics
    pub fn get_statistics(&self) -> SchedulerStatistics {
        SchedulerStatistics {
            total_schedules: self.schedules.len(),
            active_schedules: self.schedules.values().filter(|s| s.is_active()).count(),
            paused_schedules: self.schedules.values().filter(|s| s.is_paused()).count(),
            disabled_schedules: self.schedules.values().filter(|s| s.is_disabled()).count(),
            due_schedules: self.get_due_schedules().len(),
            total_runs: self.schedules.values().map(|s| s.run_count).sum(),
        }
    }
}

/// Scheduler statistics
#[derive(Debug, Clone)]
pub struct SchedulerStatistics {
    /// Total schedules
    pub total_schedules: usize,
    /// Active schedules
    pub active_schedules: usize,
    /// Paused schedules
    pub paused_schedules: usize,
    /// Disabled schedules
    pub disabled_schedules: usize,
    /// Due schedules
    pub due_schedules: usize,
    /// Total runs
    pub total_runs: u64,
}
