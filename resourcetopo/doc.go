/**
 * Copyright 2024 KusionStack Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package resourcetopo provides a resource topology management system for Kubernetes-like
// resources. It maintains relationships between different resource types and notifies
// handlers when resources or their relationships change.
//
// Lock Hierarchy
//
// To prevent deadlocks, locks must be acquired in the following order:
//
// Level 1: manager.configLock
//   - Protects: started flag
//   - Used during: topology configuration, Start()
//
// Level 2: manager.storagesLock
//   - Protects: storages map
//   - Lock type: RWMutex (RLock for reads, Lock for writes)
//
// Level 3: nodeStorage.storageLock
//   - Protects: clusterNodes map
//   - Note: Never acquire while holding any nodeInfo.lock
//
// Level 4: nodeInfo.lock
//   - Protects: PreOrders/PostOrders relation lists (directReferredPreOrders, etc.)
//   - Rule: Always acquire preNode.lock before postNode.lock
//
// Level 5: nodeInfo.relationsLock
//   - Protects: labelRelations
//   - Rule: Acquire before metaLock if both needed
//
// Level 6: nodeInfo.metaLock
//   - Protects: labels, ownerNodes, objectExisted
//   - This is the innermost lock - never acquire other locks while holding metaLock
//
// Level 7: nodeStorage.handlersLock
//   - Protects: nodeUpdateHandler, relationUpdateHandler
//   - Independent - can be acquired anytime
//
// Threading Model
//
// This package uses multiple goroutines for concurrent processing. Understanding the
// threading model is essential for avoiding race conditions and deadlocks.
//
// 1. Informer Threads (External):
//    - Source: Kubernetes informers (one per resource type)
//    - Entry points: nodeStorage.OnAdd, OnUpdate, OnDelete
//    - Purpose: Receives resource change events from API server
//    - Lock behavior: Acquires metaLock.Lock() during updateNodeMeta(), relationsLock during relation updates
//    - Note: These are the ONLY threads that write to metaLock (labels, ownerRefs, objectExisted)
//
// 2. Event Processor Threads (Internal):
//    - Source: Created by Start() -> startHandleEvent()
//    - Count: 2 goroutines
//      - handleNodeEvent(): Processes node add/update/delete/relatedUpdate events
//      - handleRelationEvent(): Processes relation add/delete events
//    - Trigger: newNodeEvent(), newRelationEvent() queue events via workqueue
//    - Lock behavior: Read-only access to handlers via RLock, no direct node locking
//    - Note: Reads handlers while holding no locks - relies on handlersLock for registration
//
// 3. User/Client Threads (External):
//    - Source: User code calling Manager methods
//    - Examples: GetNode(), GetTopoNodeStorage(), AddNodeHandler()
//    - Lock behavior: Uses storagesLock.RLock() for reads
//    - Note: These are typically short-lived operations
//
// 4. Callback Threads (External - via Event Processors):
//    - Source: User-provided NodeHandler and RelationHandler callbacks
//    - Entry: Called by handleNodeEvent/handleRelationEvent
//    - Lock behavior: No locks held during callback execution
//    - Safety: Handlers receive node references but should not cache them long-term
//
// Thread Safety Guidelines
//
// - NodeInfo references returned by GetNode() are safe for concurrent reads
// - Do NOT call AddTopologyConfig() after Start() - not thread safe
// - Handler registration (AddNodeHandler, AddRelationHandler) is thread safe
// - All callbacks (OnAdd, OnUpdate, etc.) are called synchronously - do not block
// - nodeInfo.lock can be held for extended periods during relation changes
// - metaLock is never held during callbacks or cross-node operations
//
package resourcetopo
