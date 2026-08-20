// *******************************************************************************
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache License Version 2.0 which is available at
// <https://www.apache.org/licenses/LICENSE-2.0>
//
// SPDX-License-Identifier: Apache-2.0
// *******************************************************************************

use super::action::{ActionBaseMeta, ActionMeta, ActionResult, ActionTrait, ReusableBoxFutureResult};
use crate::actions::action::ActionExecError;
use crate::api::design::Design;
use crate::common::tag::Tag;
use ::core::future::Future;
use ::core::pin::Pin;
use ::core::task::{Context, Poll};
use kyron::futures::reusable_box_future::ReusableBoxFuturePool;
use kyron::futures::{FutureInternalReturn, FutureState};
#[cfg(any(test, feature = "runtime-api-mock"))]
use kyron::testing::mock::*;
#[cfg(not(any(test, feature = "runtime-api-mock")))]
use kyron::*;
use kyron_foundation::containers::growable_vec::GrowableVec;
use kyron_foundation::containers::reusable_objects::ReusableObject;
use kyron_foundation::containers::reusable_vec_pool::ReusableVecPool;
use kyron_foundation::not_recoverable_error;
use kyron_foundation::prelude::vector_extension::VectorExtension;
use kyron_foundation::prelude::*;

pub type NodeId = usize;

use std::sync::Arc;

/// A node in the graph representing an action and its dependencies.
struct Node {
    /// The action to be executed at this node.
    action: Box<dyn ActionTrait>,
    /// Number of dependencies this node has.
    indegree: usize,
    /// Nodes that depend on this node.
    edges: Option<Vec<NodeId>>, // Option: to move edges into array when building the graph action
}

/// Builder for creating a LocalGraphAction.
/// The graph is a Directed Acyclic Graph (DAG) where each node represents an action to be executed.
/// Edges represent dependencies between actions.
pub struct LocalGraphActionBuilder {
    next_node_id: NodeId,             // Next node ID (index)
    nodes: GrowableVec<Option<Node>>, // Option: to move nodes during sorting
}

impl LocalGraphActionBuilder {
    /// Creates a new LocalGraphActionBuilder.
    pub fn new() -> Self {
        Self {
            next_node_id: 0,
            nodes: GrowableVec::new(2),
        }
    }

    /// Adds a node with the given action to the graph, returning its NodeId.
    pub fn add_node(&mut self, action: Box<dyn ActionTrait>) -> NodeId {
        let id = self.next_node_id;
        let node = Node {
            action,
            indegree: 0,
            edges: None,
        };
        self.nodes.push(Some(node));
        self.next_node_id += 1;
        id
    }

    /// Adds directed edges from the node with `node_id` to each node in `edges`.
    /// Returns a mutable reference to self.
    /// Panics if `node_id` or any edge in `edges` is invalid, if there are duplicate edges,
    /// or if there are self-loop edges.
    pub fn add_edges(&mut self, node_id: NodeId, edges: &[NodeId]) -> &mut Self {
        let node_len = self.nodes.len();
        assert!(node_len > 1, "Graph requires at least two nodes to add edges.");
        // Validate node ID
        assert!(node_id < node_len, "Invalid node ID.");

        // Find invalid edge IDs, self-loop edges, and duplicated edges
        for i in 0..edges.len() {
            assert!(edges[i] < node_len, "Invalid edge ID.");
            assert!(edges[i] != node_id, "Self-loop edges are not allowed.");
            // Number of edges would be less, so O(n^2) is acceptable here
            for j in (i + 1)..edges.len() {
                assert!(edges[i] != edges[j], "Duplicate edges are not allowed.");
            }
        }

        // Add edges
        let mut temp = Vec::new_in_global(edges.len());
        temp.extend_from_slice(edges).unwrap();
        self.nodes[node_id].as_mut().unwrap().edges = Some(temp);

        // Update indegrees (number of dependencies) of edge nodes
        for &edge in edges {
            self.nodes[edge].as_mut().unwrap().indegree += 1;
        }

        self
    }

    /// Builds the LocalGraphAction from the added nodes and edges.
    /// Panics if there are no nodes or if the graph contains a cycle.
    pub fn build(&mut self, design: &Design) -> Box<LocalGraphAction> {
        assert!(!self.nodes.is_empty(), "No nodes in the graph.");
        let mut sorted_nodes =
            LocalGraphActionBuilder::sort(&mut self.nodes).expect("Graph contains a cycle, which is not allowed.");
        let num_of_nodes = sorted_nodes.len();
        let nodes_edges = LocalGraphActionBuilder::build_edges(&mut sorted_nodes);
        // Create and return the LocalGraphAction
        Box::new(LocalGraphAction {
            base: ActionBaseMeta {
                tag: super::action::next_unique_action_tag("orch::internal::graph"),
                reusable_future_pool: LocalGraphAction::create_reusable_future_pool(
                    design.config.max_concurrent_action_executions,
                ),
            },
            nodes: sorted_nodes,
            nodes_edges,
            futures_vec_pool: ReusableVecPool::<NodeFuture>::new(
                design.config.max_concurrent_action_executions,
                |_| Vec::new_in_global(num_of_nodes),
            ),
        })
    }

    /// Checks if the graph has a cycle using Kahn's algorithm and sorts the nodes topologically if acyclic.
    /// Returns Some(sorted_nodes) if the graph is acyclic, None if it contains a cycle.
    fn sort(nodes: &mut GrowableVec<Option<Node>>) -> Option<Vec<Node>> {
        let length = nodes.len();
        // Find cycle in the graph using Kahn's algorithm
        // 1. Collect indegree (number of dependencies) for each node and
        //    nodes with zero indegree i.e. root nodes.
        let mut indegree = Vec::new_in_global(length);
        let mut queue = Vec::new_in_global(length);
        for (i, node) in nodes.iter().enumerate() {
            let deg = node.as_ref().unwrap().indegree;
            indegree.push(deg).unwrap();
            // Collect root nodes.
            if deg == 0 {
                queue.push(i).unwrap();
            }
        }

        // 2. Repeatedly remove root node from the queue, reduce indegree of its children.
        //    If any child's indegree becomes zero, add it to the queue.
        //    Count the number of visited nodes.
        //    If the number of visited nodes is less than the total number of nodes, there is a cycle.
        let mut visited = 0;
        let mut sorted = Vec::new_in_global(length);
        while !queue.is_empty() {
            let node_index = queue.remove(0).unwrap();
            sorted.push(node_index).unwrap();
            visited += 1;

            if let Some(edges) = &nodes[node_index].as_ref().unwrap().edges {
                for &to in edges.iter() {
                    indegree[to] -= 1;
                    if indegree[to] == 0 {
                        queue.push(to).unwrap();
                    }
                }
            }
        }

        // 3. If not all nodes are visited, there is a cycle
        if visited != length {
            return None;
        }

        // 4. Return nodes in sorted order
        // Create mapping from old index to new index
        let mut new_index = Vec::new_in_global(length);
        new_index.resize(length, 0).unwrap(); // Initialize with zeros for indexed updates
        for (new_id, &old_id) in sorted.iter().enumerate() {
            new_index[old_id] = new_id;
        }

        // Reorder nodes according to topological order
        let mut new_nodes = Vec::new_in_global(length);
        for &old_id in sorted.iter() {
            new_nodes.push(::core::mem::take(&mut nodes[old_id]).unwrap()).unwrap();
            // Rewrite edges with new indices
            if let Some(edges) = &mut new_nodes.last_mut().unwrap().edges {
                for e in edges.iter_mut() {
                    *e = new_index[*e];
                }
            }
        }

        Some(new_nodes)
    }

    /// Builds the edges into an Arc of boxed slices to share across threads.
    /// Note: Vec cannot be used due to Sync/Send requirements.
    fn build_edges(nodes: &mut Vec<Node>) -> Arc<[Box<[NodeId]>]> {
        let mut vec_of_boxed_arr = Vec::new_in_global(nodes.len());

        for node in nodes.iter_mut() {
            // Convert Vec<usize> to Box<[usize]>
            let boxed_edges_arr: Box<[NodeId]> = if let Some(edges) = node.edges.take() {
                Box::from(edges.as_slice())
            } else {
                Box::from([])
            };
            vec_of_boxed_arr.push(boxed_edges_arr).unwrap();
        }

        // Convert Vec<Box<[usize]>> to Arc<[Box<[usize]>]>
        Arc::from(vec_of_boxed_arr.as_slice())
    }
}

/// Default implementation for LocalGraphActionBuilder.
impl Default for LocalGraphActionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// An action that executes a directed acyclic graph (DAG) of actions.
/// Each node in the graph represents an action to be executed, and edges represent dependencies between actions.
/// The action ensures that all dependencies are resolved before executing a node, allowing for concurrent execution of
/// independent nodes.
pub struct LocalGraphAction {
    base: ActionBaseMeta,
    nodes: Vec<Node>,
    nodes_edges: Arc<[Box<[NodeId]>]>,
    futures_vec_pool: ReusableVecPool<NodeFuture>,
}

struct NodeFuture {
    future: ActionMeta,
    indegree: usize,
}

impl LocalGraphAction {
    async fn execute_impl(
        meta: Tag,
        futures_vec: ReusableObject<Vec<NodeFuture>>,
        edges_arr: Arc<[Box<[NodeId]>]>,
    ) -> ActionResult {
        tracing_adapter!(graph = ?meta, "Before executing nodes");

        let executor = DagExecutor::spawn_graph(futures_vec, edges_arr);
        let res = executor.await;

        tracing_adapter!(graph = ?meta, ?res, "After executing nodes");
        res
    }

    fn create_reusable_future_pool(pool_size: usize) -> ReusableBoxFuturePool<ActionResult> {
        let mut futures_vec_pool = ReusableVecPool::<NodeFuture>::new(pool_size, |_| Vec::new_in_global(1));
        let futures_vec = futures_vec_pool.next_object().unwrap();
        let edges_arr = Arc::new([]);
        ReusableBoxFuturePool::<ActionResult>::for_value(
            pool_size,
            Self::execute_impl("dummy".into(), futures_vec, edges_arr),
        )
    }
}

impl ActionTrait for LocalGraphAction {
    fn try_execute(&mut self) -> ReusableBoxFutureResult {
        let mut futures_vec = self.futures_vec_pool.next_object()?;

        for node in self.nodes.iter_mut() {
            // Collect futures and indegrees for each node
            futures_vec.push(NodeFuture {
                future: ActionMeta::new(node.action.try_execute()?),
                indegree: node.indegree,
            });
        }

        self.base
            .reusable_future_pool
            .next(Self::execute_impl(self.base.tag, futures_vec, self.nodes_edges.clone()))
    }

    fn name(&self) -> &'static str {
        "LocalGraphAction"
    }

    fn dbg_fmt(&self, nest: usize, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
        let indent = " ".repeat(nest);
        writeln!(f, "{}|-{} - {:?}", indent, self.name(), self.base)?;
        for (i, node) in self.nodes.iter().enumerate() {
            // Print node info
            write!(f, "{} |node {} {{ indegree: {}, ", indent, i, node.indegree)?;
            // Print edges for this node
            if let Some(edges_arr) = self.nodes_edges.get(i) {
                write!(f, "edges: [",)?;
                for (j, &edge) in edges_arr.iter().enumerate() {
                    if j > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", edge)?;
                }
                writeln!(f, "] }}")?;
            } else {
                writeln!(f, "edges: [] }}")?;
            }
            // Print action
            node.action.dbg_fmt(nest + 1, f)?;
        }
        Ok(())
    }
}

/// Executor for the DAG that manages the execution of actions based on their dependencies.
struct DagExecutor {
    finished_node_index: usize, // all nodes before this index are done
    handles: ReusableObject<Vec<NodeFuture>>,
    state: FutureState,
    action_execution_result: (usize, ActionResult),
    edges_arr: Arc<[Box<[NodeId]>]>,
}

impl DagExecutor {
    /// Spawns the actions of all root nodes (nodes with zero indegree) and returns a DagExecutor.
    fn spawn_graph(mut futures_vec: ReusableObject<Vec<NodeFuture>>, edges_arr: Arc<[Box<[NodeId]>]>) -> DagExecutor {
        for node_fut in futures_vec.iter_mut() {
            if node_fut.indegree == 0 {
                if let Some(future) = node_fut.future.take_future() {
                    node_fut.future.assign_handle(safety::spawn_from_reusable(future));
                } else {
                    not_recoverable_error!("Future not available for root node!");
                }
            } else {
                // Since nodes are in topological order, we can break early
                break;
            }
        }
        Self {
            finished_node_index: 0,
            handles: futures_vec,
            state: FutureState::New,
            action_execution_result: (0, ActionResult::Ok(())),
            edges_arr,
        }
    }

    /// Spawns the actions of the nodes that are dependent on the given node index,
    /// if their indegree reaches zero.
    fn spawn_edge_nodes(&mut self, node_index: usize) {
        let edges = &self.edges_arr[node_index];
        for &to_node in edges.iter() {
            let node_handle = &mut self.handles[to_node];
            // Decrease indegree of dependent nodes
            node_handle.indegree -= 1;
            // If indegree reaches zero, spawn the action
            if node_handle.indegree == 0 {
                if let Some(future) = node_handle.future.take_future() {
                    node_handle.future.assign_handle(safety::spawn_from_reusable(future));
                } else {
                    not_recoverable_error!("Future not available for edge node!");
                }
            }
        }
    }

    /// Polls the join handles of the spawned actions and manages the execution flow.
    /// Spawns edge nodes only after the current node's action completes successfully.
    /// In case of an action failure, edge nodes are not spawned.
    /// Returns Poll::Ready when all spawned actions are completed, or Poll::Pending if there are still actions running.
    /// If any action fails, it captures the error and continues to poll other actions.
    /// The final result will be the error of the last failed action in the sorted order of nodes.
    fn poll_node_handles(&mut self, cx: &mut Context<'_>) -> Poll<ActionResult> {
        let result = match self.state {
            // Poll all handles and spawn edge nodes as their dependencies are resolved
            FutureState::New | FutureState::Polled => {
                // Assume all are done, if any one is pending, we will set it to false
                let mut is_done = true;

                for index in self.finished_node_index..self.handles.len() {
                    match &mut self.handles[index].future {
                        ActionMeta::Handle(handle) => {
                            let res = Pin::new(handle).poll(cx);
                            match res {
                                Poll::Ready(action_result) => {
                                    self.handles[index].future.clear(); // Clear the handle after polling
                                    if self.finished_node_index == index {
                                        self.finished_node_index += 1; // Move finished node index forward for next iteration
                                    }
                                    let execution_result = match action_result {
                                        Ok(Ok(_)) => {
                                            self.spawn_edge_nodes(index);
                                            continue; // No error, continue to next handle
                                        },
                                        // In case of error, edge nodes are not spawned
                                        Ok(Err(err)) => Err(err),

                                        // This a JoinResult error, not the future error
                                        Err(_) => Err(ActionExecError::Internal),
                                    };

                                    // Store the error of the last failed node in the registration order of nodes.
                                    if execution_result.is_err() && index >= self.action_execution_result.0 {
                                        self.action_execution_result = (index, execution_result);
                                    }
                                },
                                Poll::Pending => {
                                    is_done = false; // At least one handle is still pending
                                },
                            }
                        },
                        ActionMeta::Future(_) => {
                            // Future not yet spawned
                        },
                        ActionMeta::Empty => {
                            if self.state != FutureState::Polled {
                                not_recoverable_error!("Join handle not available for the spawned future!");
                            }
                        },
                    }
                }

                if is_done {
                    FutureInternalReturn::ready(self.action_execution_result.1)
                } else {
                    FutureInternalReturn::polled()
                }
            },
            // In the Finished state, polling is an error.
            FutureState::Finished => {
                not_recoverable_error!("Future polled after it finished!")
            },
        };
        self.state.assign_and_propagate(result)
    }
}

/// Implement Future for DagExecutor to allow it to be awaited.
impl Future for DagExecutor {
    type Output = ActionResult;

    /// Polls the `DagExecutor` future.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.poll_node_handles(cx)
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use crate::common::DesignConfig;
    use crate::testing::MockActionBuilder;

    #[test]
    #[should_panic(expected = "Invalid node ID.")]
    fn graph_builder_panics_for_invalid_node_id() {
        // Create mock actions
        let action_a = Box::new(MockActionBuilder::<()>::new().build());
        let action_b = Box::new(MockActionBuilder::<()>::new().build());
        let action_c = Box::new(MockActionBuilder::<()>::new().build());

        // Create a graph builder
        let mut builder = LocalGraphActionBuilder::new();
        // Add nodes to the graph
        let _node_a = builder.add_node(action_a);
        let node_b = builder.add_node(action_b);
        let node_c = builder.add_node(action_c);

        // Add edges to define dependencies
        builder.add_edges(100, &[node_b, node_c]);
    }

    #[test]
    #[should_panic(expected = "Self-loop edges are not allowed.")]
    fn graph_builder_panics_for_self_loop_edge() {
        // Create mock actions
        let action_a = Box::new(MockActionBuilder::<()>::new().build());
        let action_b = Box::new(MockActionBuilder::<()>::new().build());
        let action_c = Box::new(MockActionBuilder::<()>::new().build());

        // Create a graph builder
        let mut builder = LocalGraphActionBuilder::new();
        // Add nodes to the graph
        let node_a = builder.add_node(action_a);
        let node_b = builder.add_node(action_b);
        let _node_c = builder.add_node(action_c);

        // Add edges to define dependencies
        builder.add_edges(node_a, &[node_b, node_a]);
    }

    #[test]
    #[should_panic(expected = "Invalid edge ID.")]
    fn graph_builder_panics_for_invalid_edge_id() {
        // Create mock actions
        let action_a = Box::new(MockActionBuilder::<()>::new().build());
        let action_b = Box::new(MockActionBuilder::<()>::new().build());
        let action_c = Box::new(MockActionBuilder::<()>::new().build());

        // Create a graph builder
        let mut builder = LocalGraphActionBuilder::new();
        // Add nodes to the graph
        let node_a = builder.add_node(action_a);
        let node_b = builder.add_node(action_b);
        let _node_c = builder.add_node(action_c);

        // Add edges to define dependencies
        builder.add_edges(node_a, &[node_b, 100]);
    }

    #[test]
    #[should_panic(expected = "Duplicate edges are not allowed.")]
    fn graph_builder_panics_for_duplicate_edges() {
        // Create mock actions
        let action_a = Box::new(MockActionBuilder::<()>::new().build());
        let action_b = Box::new(MockActionBuilder::<()>::new().build());
        let action_c = Box::new(MockActionBuilder::<()>::new().build());

        // Create a graph builder
        let mut builder = LocalGraphActionBuilder::new();
        // Add nodes to the graph
        let node_a = builder.add_node(action_a);
        let node_b = builder.add_node(action_b);
        let node_c = builder.add_node(action_c);

        // Add edges to define dependencies
        builder.add_edges(node_a, &[node_b, node_c, node_b]);
    }

    #[test]
    #[should_panic(expected = "No nodes in the graph.")]
    fn graph_builder_panics_if_no_nodes() {
        // Create a graph builder
        let mut builder = LocalGraphActionBuilder::new();
        let design = Design::new("Design".into(), DesignConfig::default());
        builder.build(&design);
    }

    #[test]
    #[should_panic(expected = "Graph contains a cycle, which is not allowed.")]
    fn graph_builder_panics_if_graph_contains_cycle() {
        // Create mock actions
        let action_a = Box::new(MockActionBuilder::<()>::new().build());
        let action_b = Box::new(MockActionBuilder::<()>::new().build());
        let action_c = Box::new(MockActionBuilder::<()>::new().build());
        let action_d = Box::new(MockActionBuilder::<()>::new().build());
        let action_e = Box::new(MockActionBuilder::<()>::new().build());

        // Create a design with default config and a graph builder
        let design = Design::new("Design".into(), DesignConfig::default());
        let mut builder = LocalGraphActionBuilder::new();
        // Add nodes to the graph
        let node_a = builder.add_node(action_a); // Root node
        let node_b = builder.add_node(action_b); // Depends on A
        let node_c = builder.add_node(action_c); // Depends on A
        let node_d = builder.add_node(action_d); // Depends on B and C
        let node_e = builder.add_node(action_e); // Depends on D

        // Add edges to define dependencies
        builder.add_edges(node_a, &[node_b, node_c]); // A -> B, A -> C
        builder.add_edges(node_b, &[node_d]); // B -> D
        builder.add_edges(node_c, &[node_d]); // C -> D
        builder.add_edges(node_d, &[node_e]); // D -> E
        builder.add_edges(node_e, &[node_b]); // E -> B (creates a cycle)

        // Build the graph action
        builder.build(&design);
    }

    #[test]
    #[cfg(not(miri))]
    #[kyron_testing_macros::ensure_clear_mock_runtime]
    fn graph_action_execute_ok_actions() {
        use crate::testing::OrchTestingPoller;
        use ::core::task::Poll;
        use kyron::testing::mock;
        use kyron_testing::prelude::Sequence;
        let seq = Sequence::new();
        // Create mock actions
        // Note: We use `in_sequence` here to enforce a deterministic, top-down execution order in tests.
        // In practice, nodes 2 and 3 could run in parallel, so this sequence does not reflect actual concurrency.
        // This approach ensures predictable test results, even though real execution may differ.
        let action_1 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq)
                .build(),
        );
        let action_2 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq)
                .build(),
        );
        let action_3 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq)
                .build(),
        );
        let action_4 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq)
                .build(),
        );
        let action_5 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq)
                .build(),
        );

        // Create a design with default config and a graph builder
        let design = Design::new("Design".into(), DesignConfig::default());
        let mut builder = LocalGraphActionBuilder::new();
        // Add nodes to the graph
        let node_1 = builder.add_node(action_1);
        let node_2 = builder.add_node(action_2);
        let node_3 = builder.add_node(action_3);
        let node_4 = builder.add_node(action_4);
        let node_5 = builder.add_node(action_5);

        // Add edges to define dependencies
        // Graph structure from left to right:
        //       2
        //      / \
        //     1   4---> 5
        //      \ /
        //       3
        builder.add_edges(node_1, &[node_2, node_3]); // 1 -> 2, 1 -> 3
        builder.add_edges(node_2, &[node_4]); // 2 -> 4
        builder.add_edges(node_3, &[node_4]); // 3 -> 4
        builder.add_edges(node_4, &[node_5]); // 4 -> 5

        // Build the graph action
        let mut graph_action = builder.build(&design);

        // Execute the graph action
        let mut poller = OrchTestingPoller::new(graph_action.try_execute().unwrap());

        // Poll until completion
        let result = loop {
            let result = poller.poll();
            if result.is_ready() {
                break result;
            }
            mock::runtime::step();
        };
        assert_eq!(result, Poll::Ready(Ok(())));
    }

    #[test]
    #[cfg(not(miri))]
    #[kyron_testing_macros::ensure_clear_mock_runtime]
    fn graph_action_executed_twice() {
        use crate::testing::OrchTestingPoller;
        use ::core::task::Poll;
        use kyron::testing::mock;
        use kyron_testing::prelude::Sequence;
        let seq = Sequence::new();
        // Create mock actions
        // Note: We use `in_sequence` here to enforce a deterministic, top-down execution order in tests.
        // In practice, nodes 2 and 3 could run in parallel, so this sequence does not reflect actual concurrency.
        // This approach ensures predictable test results, even though real execution may differ.
        let action_1 = Box::new(MockActionBuilder::<()>::new().times(2).in_sequence(&seq).build());
        let action_2 = Box::new(MockActionBuilder::<()>::new().times(2).in_sequence(&seq).build());
        let action_3 = Box::new(MockActionBuilder::<()>::new().times(2).in_sequence(&seq).build());
        let action_4 = Box::new(MockActionBuilder::<()>::new().times(2).in_sequence(&seq).build());
        let action_5 = Box::new(MockActionBuilder::<()>::new().times(2).in_sequence(&seq).build());

        // Create a design with default config and a graph builder
        let design = Design::new("Design".into(), DesignConfig::default());
        let mut builder = LocalGraphActionBuilder::new();
        // Add nodes to the graph (random order)
        let node_5 = builder.add_node(action_5);
        let node_3 = builder.add_node(action_3);
        let node_1 = builder.add_node(action_1);
        let node_2 = builder.add_node(action_2);
        let node_4 = builder.add_node(action_4);

        // Add edges to define dependencies
        builder.add_edges(node_1, &[node_2, node_3]); // 1 -> 2, 1 -> 3
        builder.add_edges(node_2, &[node_4]); // 2 -> 4
        builder.add_edges(node_3, &[node_4]); // 3 -> 4
        builder.add_edges(node_4, &[node_5]); // 4 -> 5

        // Build the graph action
        let mut graph_action = builder.build(&design);
        for _ in 0..2 {
            // Execute the graph action
            let mut poller = OrchTestingPoller::new(graph_action.try_execute().unwrap());

            // Poll until completion
            let result = loop {
                let result = poller.poll();
                if result.is_ready() {
                    break result;
                }
                mock::runtime::step();
            };
            assert_eq!(result, Poll::Ready(Ok(())));
            // This should be called whenever the graph is executed in a loop testing scenario
            seq.verify_executed_order_and_prepare_for_next_iteration();
        }
    }

    #[test]
    #[cfg(not(miri))]
    #[kyron_testing_macros::ensure_clear_mock_runtime]
    fn graph_action_execute_ok_and_err_actions() {
        use crate::testing::OrchTestingPoller;
        use ::core::task::Poll;
        use kyron::testing::mock;
        use kyron_testing::prelude::Sequence;
        let seq = Sequence::new();
        // Create mock actions
        // Note: We use `in_sequence` here to enforce a deterministic, top-down execution order in tests.
        // In practice, nodes 2 and 3 could run in parallel, so this sequence does not reflect actual concurrency.
        // This approach ensures predictable test results, even though real execution may differ.
        let action_1 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq)
                .build(),
        );
        let action_2 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Err(ActionExecError::Internal))
                .in_sequence(&seq)
                .build(),
        );
        let action_3 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq)
                .build(),
        );
        // Action 4 and 5 should not be executed as action 2 fails for the graph below
        // Graph structure from left to right:
        //       2
        //      / \
        //     1   4---> 5
        //      \ /
        //       3
        let action_4 = Box::new(MockActionBuilder::<()>::new().in_sequence(&seq).build());
        let action_5 = Box::new(MockActionBuilder::<()>::new().in_sequence(&seq).build());

        // Create a design with default config and a graph builder
        let design = Design::new("Design".into(), DesignConfig::default());
        let mut builder = LocalGraphActionBuilder::new();
        // Add nodes to the graph
        let node_1 = builder.add_node(action_1);
        let node_2 = builder.add_node(action_2);
        let node_3 = builder.add_node(action_3);
        let node_4 = builder.add_node(action_4);
        let node_5 = builder.add_node(action_5);

        // Add edges to define dependencies
        builder.add_edges(node_1, &[node_2, node_3]); // 1 -> 2, 1 -> 3
        builder.add_edges(node_2, &[node_4]); // 2 -> 4
        builder.add_edges(node_3, &[node_4]); // 3 -> 4
        builder.add_edges(node_4, &[node_5]); // 4 -> 5

        // Build the graph action
        let mut graph_action = builder.build(&design);

        // Execute the graph action
        let mut poller = OrchTestingPoller::new(graph_action.try_execute().unwrap());

        // Poll until completion
        let result = loop {
            let result = poller.poll();
            if result.is_ready() {
                break result;
            }
            mock::runtime::step();
        };
        assert_eq!(result, Poll::Ready(Err(ActionExecError::Internal)));
    }

    #[test]
    #[cfg(not(miri))]
    #[kyron_testing_macros::ensure_clear_mock_runtime]
    fn graph_action_execute_ok_and_two_err_actions() {
        use crate::testing::OrchTestingPoller;
        use ::core::task::Poll;
        use kyron::testing::mock;
        use kyron_testing::prelude::Sequence;
        let seq = Sequence::new();
        // Create mock actions
        // Note: We use `in_sequence` here to enforce a deterministic, top-down execution order in tests.
        // In practice, nodes 2 and 3 could run in parallel, so this sequence does not reflect actual concurrency.
        // This approach ensures predictable test results, even though real execution may differ.
        let action_1 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq)
                .build(),
        );
        let action_2 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Err(ActionExecError::Internal))
                .in_sequence(&seq)
                .build(),
        );
        let action_3 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Err(ActionExecError::NonRecoverableFailure))
                .in_sequence(&seq)
                .build(),
        );
        // Action 4 and 5 should not be executed as action 2 & 3 fails for the graph below
        let action_4 = Box::new(MockActionBuilder::<()>::new().in_sequence(&seq).build());
        let action_5 = Box::new(MockActionBuilder::<()>::new().in_sequence(&seq).build());

        // Create a design with default config and a graph builder
        let design = Design::new("Design".into(), DesignConfig::default());
        let mut builder = LocalGraphActionBuilder::new();
        // Add nodes to the graph
        let node_1 = builder.add_node(action_1);
        let node_2 = builder.add_node(action_2);
        let node_3 = builder.add_node(action_3);
        let node_4 = builder.add_node(action_4);
        let node_5 = builder.add_node(action_5);

        // Add edges to define dependencies
        builder.add_edges(node_1, &[node_2, node_3]); // 1 -> 2, 1 -> 3
        builder.add_edges(node_2, &[node_4]); // 2 -> 4
        builder.add_edges(node_3, &[node_4]); // 3 -> 4
        builder.add_edges(node_4, &[node_5]); // 4 -> 5

        // Build the graph action
        let mut graph_action = builder.build(&design);

        // Execute the graph action
        let mut poller = OrchTestingPoller::new(graph_action.try_execute().unwrap());

        // Poll until completion
        let result = loop {
            let result = poller.poll();
            if result.is_ready() {
                break result;
            }
            mock::runtime::step();
        };
        assert_eq!(result, Poll::Ready(Err(ActionExecError::NonRecoverableFailure)));
    }

    #[test]
    #[cfg(not(miri))]
    #[kyron_testing_macros::ensure_clear_mock_runtime]
    #[should_panic]
    fn graph_action_panics_if_polled_after_future_reported_ready() {
        use crate::testing::OrchTestingPoller;
        use ::core::task::Poll;
        use kyron::testing::mock;
        use kyron_testing::prelude::Sequence;
        let seq = Sequence::new();
        // Create mock actions
        // Note: We use `in_sequence` here to enforce a deterministic, top-down execution order in tests.
        // In practice, nodes 2 and 3 could run in parallel, so this sequence does not reflect actual concurrency.
        // This approach ensures predictable test results, even though real execution may differ.
        let action_1 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq)
                .build(),
        );
        let action_2 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq)
                .build(),
        );
        let action_3 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq)
                .build(),
        );
        let action_4 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq)
                .build(),
        );
        let action_5 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq)
                .build(),
        );

        // Create a design with default config and a graph builder
        let design = Design::new("Design".into(), DesignConfig::default());
        let mut builder = LocalGraphActionBuilder::new();
        // Add nodes to the graph
        let node_1 = builder.add_node(action_1);
        let node_2 = builder.add_node(action_2);
        let node_3 = builder.add_node(action_3);
        let node_4 = builder.add_node(action_4);
        let node_5 = builder.add_node(action_5);

        // Add edges to define dependencies
        builder.add_edges(node_1, &[node_2, node_3]); // 1 -> 2, 1 -> 3
        builder.add_edges(node_2, &[node_4]); // 2 -> 4
        builder.add_edges(node_3, &[node_4]); // 3 -> 4
        builder.add_edges(node_4, &[node_5]); // 4 -> 5

        // Build the graph action
        let mut graph_action = builder.build(&design);

        // Execute the graph action
        let mut poller = OrchTestingPoller::new(graph_action.try_execute().unwrap());

        // Poll until completion
        let result = loop {
            let result = poller.poll();
            if result.is_ready() {
                break result;
            }
            mock::runtime::step();
        };
        assert_eq!(result, Poll::Ready(Ok(())));

        // Poll again after the future has reported ready, this causes a panic.
        let _ = poller.poll();
    }

    #[test]
    #[cfg(not(miri))]
    #[kyron_testing_macros::ensure_clear_mock_runtime]
    fn graph_action_fails_first_time_and_succeeds_second_time() {
        use crate::testing::OrchTestingPoller;
        use ::core::task::Poll;
        use kyron::testing::mock;
        use kyron_testing::prelude::Sequence;
        let seq = Sequence::new();
        // Create mock actions
        // Note: We use `in_sequence` here to enforce a deterministic, top-down execution order in tests.
        // In practice, nodes 2 and 3 could run in parallel, so this sequence does not reflect actual concurrency.
        // This approach ensures predictable test results, even though real execution may differ.
        let action_1 = Box::new(MockActionBuilder::<()>::new().times(2).in_sequence(&seq).build());
        let action_2 = Box::new(MockActionBuilder::<()>::new().times(2).in_sequence(&seq).build());
        let action_3 = Box::new(MockActionBuilder::<()>::new().times(2).in_sequence(&seq).build());
        let action_4 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Err(ActionExecError::Internal))
                .will_once_return(Ok(()))
                .in_sequence(&seq)
                .build(),
        );
        let action_5 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq)
                .build(),
        );

        // Create a design with default config and a graph builder
        let design = Design::new("Design".into(), DesignConfig::default());
        let mut builder = LocalGraphActionBuilder::new();
        // Add nodes to the graph
        let node_1 = builder.add_node(action_1);
        let node_2 = builder.add_node(action_2);
        let node_3 = builder.add_node(action_3);
        let node_4 = builder.add_node(action_4);
        let node_5 = builder.add_node(action_5);

        // Add edges to define dependencies
        builder.add_edges(node_1, &[node_2, node_3]); // 1 -> 2, 1 -> 3
        builder.add_edges(node_2, &[node_4]); // 2 -> 4
        builder.add_edges(node_3, &[node_4]); // 3 -> 4
        builder.add_edges(node_4, &[node_5]); // 4 -> 5

        // Build the graph action
        let mut graph_action = builder.build(&design);
        for count in 0..2 {
            // Execute the graph action
            let mut poller = OrchTestingPoller::new(graph_action.try_execute().unwrap());

            // Poll until completion
            let result = loop {
                let result = poller.poll();
                if result.is_ready() {
                    break result;
                }
                mock::runtime::step();
            };
            if count == 0 {
                // First execution should fail since action_4 returns Err on the first call
                assert_eq!(result, Poll::Ready(Err(ActionExecError::Internal)));
            } else {
                // Second execution should succeed since action_4 returns Ok on the second call
                assert_eq!(result, Poll::Ready(Ok(())));
            }
            // This should be called whenever the graph is executed in a loop testing scenario
            seq.verify_executed_order_and_prepare_for_next_iteration();
        }
    }

    #[test]
    #[cfg(not(miri))]
    #[kyron_testing_macros::ensure_clear_mock_runtime]
    fn graph_action_with_multiple_roots_and_sequence() {
        use crate::testing::OrchTestingPoller;
        use ::core::task::Poll;
        use kyron::testing::mock;
        use kyron_testing::prelude::Sequence;
        let seq1 = Sequence::new();
        let seq2 = Sequence::new();
        let seq3 = Sequence::new();
        // Create mock actions
        // Note: We use `in_sequence` here to enforce a deterministic, top-down execution order in tests.
        // In practice, few nodes run in parallel, so this sequence does not reflect actual concurrency.
        // This approach ensures predictable test results, even though real execution may differ.
        let action_1 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq1)
                .build(),
        );
        let action_2 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq2)
                .build(),
        );
        let action_3 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq3)
                .build(),
        );
        let action_4 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq1)
                .build(),
        );
        let action_5 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq2)
                .build(),
        );
        let action_6 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq3)
                .build(),
        );
        let action_7 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq1)
                .build(),
        );
        let action_8 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq1)
                .build(),
        );
        let action_9 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq3)
                .build(),
        );
        let action_10 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq1)
                .build(),
        );
        let action_11 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq3)
                .build(),
        );
        let action_12 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq1)
                .build(),
        );
        let action_13 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq1)
                .build(),
        );

        // Create a design with default config and a graph builder
        let design = Design::new("Design".into(), DesignConfig::default());
        let mut builder = LocalGraphActionBuilder::new();
        // Add nodes to the graph
        let node_1 = builder.add_node(action_1);
        let node_2 = builder.add_node(action_2);
        let node_3 = builder.add_node(action_3);
        let node_4 = builder.add_node(action_4);
        let node_5 = builder.add_node(action_5);
        let node_6 = builder.add_node(action_6);
        let node_7 = builder.add_node(action_7);
        let node_8 = builder.add_node(action_8);
        let node_9 = builder.add_node(action_9);
        let node_10 = builder.add_node(action_10);
        let node_11 = builder.add_node(action_11);
        let node_12 = builder.add_node(action_12);
        let node_13 = builder.add_node(action_13);

        // Add edges to define dependencies
        // Graph structure from left to right:
        //
        //     1 ---> 4 -      8 ---> 10--
        //               \    /            \
        //     2 ---> 5 --7---              12 ---> 13
        //               /    \            /
        //     3 ---> 6 -      9 ---> 11 --
        //
        builder.add_edges(node_1, &[node_4]);
        builder.add_edges(node_2, &[node_5]);
        builder.add_edges(node_3, &[node_6]);
        builder.add_edges(node_4, &[node_7]);
        builder.add_edges(node_5, &[node_7]);
        builder.add_edges(node_6, &[node_7]);
        builder.add_edges(node_7, &[node_8, node_9]);
        builder.add_edges(node_8, &[node_10]);
        builder.add_edges(node_9, &[node_11]);
        builder.add_edges(node_10, &[node_12]);
        builder.add_edges(node_11, &[node_12]);
        builder.add_edges(node_12, &[node_13]);

        // Build the graph action
        let mut graph_action = builder.build(&design);

        // Execute the graph action
        let mut poller = OrchTestingPoller::new(graph_action.try_execute().unwrap());

        // Poll until completion
        let result = loop {
            let result = poller.poll();
            if result.is_ready() {
                break result;
            }
            mock::runtime::step();
        };
        assert_eq!(result, Poll::Ready(Ok(())));
    }
}
