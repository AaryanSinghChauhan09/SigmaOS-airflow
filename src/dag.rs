//! DAG definitions for airflow.

/// Node type
#[derive(Debug, Clone, PartialEq)]
pub enum NodeType {
    /// Task node
    Task,
    /// Operator node
    Operator,
    /// Sensor node
    Sensor,
    /// Trigger node
    Trigger,
    /// Branch node
    Branch,
    /// Join node
    Join,
}

/// DAG node
#[derive(Debug, Clone)]
pub struct DAGNode {
    /// Node ID
    pub id: String,
    /// Node name
    pub name: String,
    /// Node type
    pub node_type: NodeType,
    /// Node description
    pub description: String,
    /// Node dependencies
    pub dependencies: Vec<String>,
    /// Node dependents
    pub dependents: Vec<String>,
    /// Node configuration
    pub config: std::collections::HashMap<String, String>,
    /// Node enabled
    pub enabled: bool,
    /// Node created timestamp
    pub created_at: u64,
}

impl DAGNode {
    /// Create new DAG node
    pub fn new(id: String, name: String, node_type: NodeType) -> Self {
        Self {
            id,
            name,
            node_type,
            description: String::new(),
            dependencies: vec![],
            dependents: vec![],
            config: std::collections::HashMap::new(),
            enabled: true,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
    
    /// Set description
    pub fn set_description(&mut self, description: String) {
        self.description = description;
    }
    
    /// Add dependency
    pub fn add_dependency(&mut self, dependency_id: String) {
        if !self.dependencies.contains(&dependency_id) {
            self.dependencies.push(dependency_id);
        }
    }
    
    /// Remove dependency
    pub fn remove_dependency(&mut self, dependency_id: &str) -> bool {
        if let Some(pos) = self.dependencies.iter().position(|d| d == dependency_id) {
            self.dependencies.remove(pos);
            return true;
        }
        false
    }
    
    /// Add dependent
    pub fn add_dependent(&mut self, dependent_id: String) {
        if !self.dependents.contains(&dependent_id) {
            self.dependents.push(dependent_id);
        }
    }
    
    /// Remove dependent
    pub fn remove_dependent(&mut self, dependent_id: &str) -> bool {
        if let Some(pos) = self.dependents.iter().position(|d| d == dependent_id) {
            self.dependents.remove(pos);
            return true;
        }
        false
    }
    
    /// Set configuration value
    pub fn set_config(&mut self, key: String, value: String) {
        self.config.insert(key, value);
    }
    
    /// Get configuration value
    pub fn get_config(&self, key: &str) -> Option<&String> {
        self.config.get(key)
    }
    
    /// Enable node
    pub fn enable(&mut self) {
        self.enabled = true;
    }
    
    /// Disable node
    pub fn disable(&mut self) {
        self.enabled = false;
    }
    
    /// Check if node is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
    
    /// Get node age in seconds
    pub fn get_age_seconds(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() - self.created_at
    }
}

/// DAG edge
#[derive(Debug, Clone)]
pub struct DAGEdge {
    /// Source node ID
    pub source: String,
    /// Target node ID
    pub target: String,
    /// Edge label
    pub label: String,
    /// Edge condition
    pub condition: Option<String>,
}

impl DAGEdge {
    /// Create new DAG edge
    pub fn new(source: String, target: String) -> Self {
        Self {
            source,
            target,
            label: String::new(),
            condition: None,
        }
    }
    
    /// Set label
    pub fn set_label(&mut self, label: String) {
        self.label = label;
    }
    
    /// Set condition
    pub fn set_condition(&mut self, condition: String) {
        self.condition = Some(condition);
    }
    
    /// Check if edge has condition
    pub fn has_condition(&self) -> bool {
        self.condition.is_some()
    }
}

/// DAG (Directed Acyclic Graph)
#[derive(Debug, Clone)]
pub struct DAG {
    /// DAG nodes
    pub nodes: std::collections::HashMap<String, DAGNode>,
    /// DAG edges
    pub edges: Vec<DAGEdge>,
    /// DAG name
    pub name: String,
    /// DAG description
    pub description: String,
    /// DAG created timestamp
    pub created_at: u64,
}

impl DAG {
    /// Create new DAG
    pub fn new(name: String) -> Self {
        Self {
            nodes: std::collections::HashMap::new(),
            edges: vec![],
            name,
            description: String::new(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
    
    /// Set description
    pub fn set_description(&mut self, description: String) {
        self.description = description;
    }
    
    /// Add node
    pub fn add_node(&mut self, node: DAGNode) -> String {
        let id = node.id.clone();
        self.nodes.insert(id.clone(), node);
        id
    }
    
    /// Remove node
    pub fn remove_node(&mut self, node_id: &str) -> bool {
        if self.nodes.remove(node_id).is_some() {
            // Remove edges involving this node
            self.edges.retain(|e| e.source != node_id && e.target != node_id);
            // Remove dependencies/dependents references
            for node in self.nodes.values_mut() {
                node.dependencies.retain(|d| d != node_id);
                node.dependents.retain(|d| d != node_id);
            }
            return true;
        }
        false
    }
    
    /// Get node
    pub fn get_node(&self, node_id: &str) -> Option<&DAGNode> {
        self.nodes.get(node_id)
    }
    
    /// Get node mutably
    pub fn get_node_mut(&mut self, node_id: &str) -> Option<&mut DAGNode> {
        self.nodes.get_mut(node_id)
    }
    
    /// Get all nodes
    pub fn get_nodes(&self) -> Vec<&DAGNode> {
        self.nodes.values().collect()
    }
    
    /// Add edge
    pub fn add_edge(&mut self, edge: DAGEdge) -> bool {
        // Check if nodes exist
        if !self.nodes.contains_key(&edge.source) || !self.nodes.contains_key(&edge.target) {
            return false;
        }
        
        // Check for cycles (simple check)
        if self.would_create_cycle(&edge.source, &edge.target) {
            return false;
        }
        
        // Update node dependencies/dependents
        if let Some(source_node) = self.nodes.get_mut(&edge.source) {
            source_node.add_dependent(edge.target.clone());
        }
        if let Some(target_node) = self.nodes.get_mut(&edge.target) {
            target_node.add_dependency(edge.source.clone());
        }
        
        self.edges.push(edge);
        true
    }
    
    /// Remove edge
    pub fn remove_edge(&mut self, source: &str, target: &str) -> bool {
        if let Some(pos) = self.edges.iter().position(|e| e.source == source && e.target == target) {
            self.edges.remove(pos);
            
            // Update node dependencies/dependents
            if let Some(source_node) = self.nodes.get_mut(source) {
                source_node.remove_dependent(target);
            }
            if let Some(target_node) = self.nodes.get_mut(target) {
                target_node.remove_dependency(source);
            }
            
            return true;
        }
        false
    }
    
    /// Get edges
    pub fn get_edges(&self) -> &[DAGEdge] {
        &self.edges
    }
    
    /// Get edges from node
    pub fn get_edges_from(&self, node_id: &str) -> Vec<&DAGEdge> {
        self.edges.iter()
            .filter(|e| e.source == node_id)
            .collect()
    }
    
    /// Get edges to node
    pub fn get_edges_to(&self, node_id: &str) -> Vec<&DAGEdge> {
        self.edges.iter()
            .filter(|e| e.target == node_id)
            .collect()
    }
    
    /// Check if would create cycle
    fn would_create_cycle(&self, source: &str, target: &str) -> bool {
        // Simple cycle detection - in real implementation would use proper algorithm
        let mut visited = std::collections::HashSet::new();
        let mut stack = vec![target];
        
        while let Some(node) = stack.pop() {
            if node == source {
                return true;
            }
            
            if visited.contains(node) {
                continue;
            }
            visited.insert(node);
            
            // Add all dependencies of current node
            if let Some(node_obj) = self.nodes.get(node) {
                for dep in &node_obj.dependencies {
                    stack.push(dep);
                }
            }
        }
        
        false
    }
    
    /// Get topological order
    pub fn get_topological_order(&self) -> Vec<String> {
        // Simple topological sort
        let mut in_degree: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        let mut queue = std::collections::VecDeque::new();
        let mut result = vec![];
        
        // Initialize in-degree
        for node_id in self.nodes.keys() {
            in_degree.insert(node_id.clone(), 0);
        }
        
        // Calculate in-degree
        for edge in &self.edges {
            if let Some(degree) = in_degree.get_mut(&edge.target) {
                *degree += 1;
            }
        }
        
        // Add nodes with in-degree 0 to queue
        for (node_id, degree) in &in_degree {
            if *degree == 0 {
                queue.push_back(node_id.clone());
            }
        }
        
        // Process queue
        while let Some(node_id) = queue.pop_front() {
            result.push(node_id.clone());
            
            // Remove node and update in-degree of neighbors
            for edge in &self.edges {
                if edge.source == node_id {
                    if let Some(degree) = in_degree.get_mut(&edge.target) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push_back(edge.target.clone());
                        }
                    }
                }
            }
        }
        
        result
    }
    
    /// Validate DAG
    pub fn validate(&self) -> Vec<String> {
        let mut errors = vec![];
        
        // Check for nodes with no dependencies (should have at least one source)
        for node in self.nodes.values() {
            if node.dependencies.is_empty() && node.dependents.is_empty() {
                errors.push(format!("Node {} has no connections", node.name));
            }
        }
        
        // Check for cycles
        let order = self.get_topological_order();
        if order.len() != self.nodes.len() {
            errors.push("DAG contains cycles".to_string());
        }
        
        errors
    }
    
    /// Get node count
    pub fn get_node_count(&self) -> usize {
        self.nodes.len()
    }
    
    /// Get edge count
    pub fn get_edge_count(&self) -> usize {
        self.edges.len()
    }
    
    /// Get DAG age in seconds
    pub fn get_age_seconds(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() - self.created_at
    }
}
