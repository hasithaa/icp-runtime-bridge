// Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com) All Rights Reserved.
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/log;

// ================================================================================
// WORKFLOW INTEGRATION
// ================================================================================
// When the integration uses ballerina/workflow, this bridge publishes the workflow
// metadata document (definitions, human tasks, activities, agents — with JSON
// schemas) in full heartbeats, and can execute management commands the ICP tunnels
// back in heartbeat responses. The wiring is registered by a glue file this
// package's compiler plugin generates into the user's package whenever it detects
// a ballerina/workflow import — importing this bridge is all a user does. The glue
// keeps this package free of any compile-time dependency on ballerina/workflow.

# Supplies the workflow metadata document to publish in full heartbeats.
# Typically `workflow.management.rest:getWorkflowMetadata`, adapted to json.
public type WorkflowMetadataProvider isolated function () returns map<json>|error;

# Executes one tunneled workflow management command and returns
# `{httpStatus, body}` exactly as the management REST API would have responded.
# Typically an adapter over `workflow.management.rest:executeManagementCommand`.
public type WorkflowCommandExecutor isolated function (map<json> command) returns map<json>|error;

isolated WorkflowMetadataProvider? workflowMetadataProvider = ();
isolated WorkflowCommandExecutor? workflowCommandExecutor = ();

# Registers the integration's workflow runtime with this bridge. Called at module
# init by the compiler-plugin-generated glue; may also be called directly by
# advanced integrations that assemble their own metadata.
#
# + metadataProvider - Supplies the metadata document for full heartbeats
# + commandExecutor - Executes tunneled management commands
# + return - `true`, so the generated glue can bind the call at module level
public isolated function registerWorkflowIntegration(WorkflowMetadataProvider metadataProvider,
        WorkflowCommandExecutor commandExecutor) returns boolean {
    lock {
        workflowMetadataProvider = metadataProvider;
    }
    lock {
        workflowCommandExecutor = commandExecutor;
    }
    log:printDebug("Workflow integration registered with the ICP bridge");
    return true;
}

# Returns the current workflow metadata document, or `()` when no workflow
# integration is registered, the document is empty, or the provider fails
# (failures are logged and never break heartbeating).
#
# + return - The metadata document, or `()`
isolated function currentWorkflowMetadata() returns map<json>? {
    WorkflowMetadataProvider? provider;
    lock {
        provider = workflowMetadataProvider;
    }
    if provider is () {
        return ();
    }
    map<json>|error metadata = provider();
    if metadata is error {
        log:printWarn("Failed to read workflow metadata for the heartbeat", metadata);
        return ();
    }
    return metadata.length() > 0 ? metadata : ();
}

# Returns the capabilities this runtime advertises to the ICP, or `()` when there
# are none. `workflowCommands` is advertised only when a workflow integration is
# registered AND the user opted in with `enableWorkflowManagement = true` — the
# server must never tunnel a workflow command to a runtime without this capability.
#
# + return - The capability names, or `()`
isolated function currentCapabilities() returns string[]? {
    boolean hasExecutor;
    lock {
        hasExecutor = workflowCommandExecutor !is ();
    }
    if hasExecutor && enableWorkflowManagement {
        return ["workflowCommands"];
    }
    return ();
}

// ── Tunneled command execution ───────────────────────────────────────────────

// Results of recently executed commands, kept so a redelivered commandId (e.g. the
// result was lost after execution) replays the cached result instead of executing the
// operation twice — this is what makes mutations like completeHumanTask safe against
// duplicate delivery. Insertion-ordered FIFO eviction. One record so a single lock
// covers both structures (a lock may access only one isolated module variable).
const int PROCESSED_COMMAND_CACHE_CAPACITY = 64;

type ProcessedCommandCache record {|
    map<WorkflowCommandResult> results = {};
    string[] insertionOrder = [];
|};

isolated ProcessedCommandCache processedCommands = {};

# Executes one tunneled workflow management command. Never panics or returns an
# error: every outcome — including "workflow management disabled" and executor
# failures — becomes a result the ICP can deliver to the waiting caller.
#
# + payload - The command payload from the `WORKFLOW_MGMT` control command
# + return - The result to post to `POST /icp/commandResult`
isolated function executeWorkflowCommand(WorkflowCommandPayload payload) returns WorkflowCommandResult {
    WorkflowCommandResult? cached = cachedCommandResult(payload.commandId);
    if cached is WorkflowCommandResult {
        log:printInfo(string `Replaying cached result for redelivered workflow command: ${payload.commandId}`);
        return cached;
    }

    WorkflowCommandExecutor? executor;
    lock {
        executor = workflowCommandExecutor;
    }

    WorkflowCommandResult result;
    if executor is () || !enableWorkflowManagement {
        // The capability is only advertised when both hold (currentCapabilities), so this
        // is a server-side gating bug or a config change since the last heartbeat.
        result = {
            runtimeId: currentRuntimeId,
            commandId: payload.commandId,
            status: "FAILED",
            httpStatus: 403,
            body: {"error": {"message": "Workflow management commands are not accepted by this runtime"}}
        };
    } else {
        map<json> command = {
            operation: payload.operation,
            params: payload.params,
            identity: {userId: payload.identity.userId, roles: payload.identity.roles}
        };
        map<json>|error outcome = executor(command);
        if outcome is error {
            log:printError(string `Workflow command execution failed: ${payload.commandId}`, outcome);
            result = {
                runtimeId: currentRuntimeId,
                commandId: payload.commandId,
                status: "FAILED",
                httpStatus: 500,
                body: {"error": {"message": outcome.message()}}
            };
        } else {
            json httpStatus = outcome["httpStatus"];
            result = {
                runtimeId: currentRuntimeId,
                commandId: payload.commandId,
                status: "COMPLETED",
                httpStatus: httpStatus is int ? httpStatus : 500,
                body: outcome["body"]
            };
        }
    }
    cacheCommandResult(result);
    return result;
}

isolated function cachedCommandResult(string commandId) returns WorkflowCommandResult? {
    lock {
        WorkflowCommandResult? cached = processedCommands.results[commandId];
        return cached is WorkflowCommandResult ? cached.clone() : ();
    }
}

isolated function cacheCommandResult(WorkflowCommandResult result) {
    lock {
        if processedCommands.results.hasKey(result.commandId) {
            return;
        }
        if processedCommands.insertionOrder.length() >= PROCESSED_COMMAND_CACHE_CAPACITY {
            string evicted = processedCommands.insertionOrder.shift();
            _ = processedCommands.results.removeIfHasKey(evicted);
        }
        processedCommands.insertionOrder.push(result.commandId);
        processedCommands.results[result.commandId] = result.clone();
    }
}
