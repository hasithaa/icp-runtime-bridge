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

// ============================================================================
// Command tunnel — unit tests
// ============================================================================
// The tunnel is the part of the bridge with no network of its own: a command
// arrives as data in a heartbeat response, an executor runs it in-process, and
// a result envelope goes back. These tests drive `executeTunneledCommand` with
// stub executors, so every branch of that contract is checked without an ICP
// server, a workflow runtime, or a Temporal server.
//
// The stubs are deliberate, not a shortcut: the bridge must not depend on the
// workflow module in any scope. Its executors are function pointers registered
// at run time and the tunnel only ever sees `map<json>` in and out, so testing
// it needs no workflow import — and adding one would put a dependency into a
// package that must stay free of it.
//
// What matters here is what the ICP receives. A command that ran reports
// COMPLETED with the executor's own status — including a 404, which is an
// answer, not a failure of the tunnel — while a command that could not run
// reports FAILED with a status of its own. And a redelivered commandId must
// never execute twice: the whole point of the reservation is that tunneled
// mutations survive duplicate delivery.
// ============================================================================

import ballerina/test;

// Executor call bookkeeping. Module-level and lock-guarded because executors are
// `isolated function`s and cannot close over test-local state; one record, because a lock
// statement may access only one such variable.
type ExecutorLog record {|
    int calls = 0;
    map<json> lastCommand = {};
|};

isolated ExecutorLog executorLog = {};

isolated function resetExecutorState() {
    lock {
        executorLog = {};
    }
}

isolated function executorCallCount() returns int {
    lock {
        return executorLog.calls;
    }
}

isolated function recordCall(map<json> command) {
    lock {
        executorLog.calls += 1;
        executorLog.lastCommand = command.clone();
    }
}

isolated function seenCommand() returns map<json> {
    lock {
        return executorLog.lastCommand.clone();
    }
}

// A management operation that succeeded.
isolated function okExecutor(map<json> command) returns map<json>|error {
    recordCall(command);
    return {httpStatus: 200, body: {"items": [], "hasMore": false}};
}

// A management operation the runtime executed and answered with a non-2xx status:
// the operation ran, so the tunnel reports COMPLETED and relays the status.
isolated function notFoundExecutor(map<json> command) returns map<json>|error {
    recordCall(command);
    return {httpStatus: 404, body: {"error": {"message": "No such instance"}}};
}

// An operation that could not run at all.
isolated function failingExecutor(map<json> command) returns map<json>|error {
    recordCall(command);
    return error("workflow runtime is not available");
}

// A result the tunnel cannot interpret — no int `httpStatus`.
isolated function malformedExecutor(map<json> command) returns map<json>|error {
    recordCall(command);
    return {body: {"items": []}};
}

isolated function commandPayload(string commandId, string operation = "instances.list",
        map<json> params = {}, CommandIdentity identity = {}) returns TunneledCommandPayload =>
    {commandId: commandId, operation: operation, params: params, identity: identity};

// ── Relaying what the executor answered ──────────────────────────────────────

@test:Config {}
function testExecutedCommandRelaysStatusAndBody() {
    resetExecutorState();
    TunneledCommandPayload payload = commandPayload("wfc-relay-1", "humanTasks.list",
            {'limit: 20}, {userId: "alice", roles: ["APPROVER", "OPS"]});

    TunneledCommandResult? result = executeTunneledCommand(payload, okExecutor, true);

    if result is () {
        test:assertFail("A reserved command must produce a result to post");
    }
    test:assertEquals(result.commandId, "wfc-relay-1");
    test:assertEquals(result.status, "COMPLETED");
    test:assertEquals(result.httpStatus, 200);
    test:assertEquals(result.body, {"items": [], "hasMore": false},
            "The executor's body must be relayed unchanged");
    test:assertEquals(executorCallCount(), 1);

    // The executor sees the operation request the management API expects — and nothing
    // about the tunnel: no commandId, no deadline.
    map<json> seen = seenCommand();
    test:assertEquals(seen["operation"], "humanTasks.list");
    test:assertEquals(seen["params"], {'limit: 20});
    test:assertEquals(seen["identity"], {userId: "alice", roles: ["APPROVER", "OPS"]});
    test:assertFalse(seen.hasKey("commandId"), "The tunnel's correlation id is not the operation's business");
}

@test:Config {}
function testExecutedCommandRelaysNonSuccessStatus() {
    resetExecutorState();
    TunneledCommandResult? result = executeTunneledCommand(
            commandPayload("wfc-relay-404", "instances.get", {workflowId: "missing"}),
            notFoundExecutor, true);

    if result is () {
        test:assertFail("A reserved command must produce a result to post");
    }
    // The operation ran and answered 404. That is the answer, not a tunnel failure —
    // reporting FAILED here would make the ICP retry a question already answered.
    test:assertEquals(result.status, "COMPLETED");
    test:assertEquals(result.httpStatus, 404);
    test:assertEquals(result.body, {"error": {"message": "No such instance"}});
}

// ── Failures the tunnel reports itself ───────────────────────────────────────

@test:Config {}
function testExecutorErrorIsReportedAsFailed() {
    resetExecutorState();
    TunneledCommandResult? result = executeTunneledCommand(
            commandPayload("wfc-error-1"), failingExecutor, true);

    if result is () {
        test:assertFail("A failed execution must still produce a result to post");
    }
    test:assertEquals(result.status, "FAILED");
    test:assertEquals(result.httpStatus, 500);
    json body = result.body;
    test:assertTrue(body.toJsonString().includes("workflow runtime is not available"),
            "The failure must name its cause, got: " + body.toJsonString());
}

@test:Config {}
function testUnexpectedResultShapeIsReportedAsFailed() {
    // An executor result without an int `httpStatus` once reported COMPLETED with a
    // substituted 500 — a server error dressed as a completed command, with nothing to
    // diagnose it by.
    resetExecutorState();
    TunneledCommandResult? result = executeTunneledCommand(
            commandPayload("wfc-malformed-1"), malformedExecutor, true);

    if result is () {
        test:assertFail("A malformed result must still produce a result to post");
    }
    test:assertEquals(result.status, "FAILED",
            "An uninterpretable executor result is a failure, not a completion");
    test:assertEquals(result.httpStatus, 500);
    test:assertTrue(result.body.toJsonString().includes("Unexpected command result shape"),
            "The failure must say the shape was wrong, got: " + result.body.toJsonString());
}

@test:Config {}
function testCommandKindNotAcceptedIsRejected() {
    // The capability is only advertised while management is enabled, so a command arriving
    // with it disabled means the server gated wrongly or the config changed since the last
    // heartbeat. Either way the operation must not run.
    resetExecutorState();
    TunneledCommandResult? result = executeTunneledCommand(
            commandPayload("wfc-refused-1"), okExecutor, false);

    if result is () {
        test:assertFail("A refused command must still produce a result to post");
    }
    test:assertEquals(result.status, "FAILED");
    test:assertEquals(result.httpStatus, 403);
    test:assertEquals(executorCallCount(), 0, "A refused command must not reach the executor");
}

@test:Config {}
function testMissingExecutorIsRejected() {
    resetExecutorState();
    TunneledCommandResult? result = executeTunneledCommand(
            commandPayload("wfc-noexec-1"), (), true);

    if result is () {
        test:assertFail("A command with no executor must still produce a result to post");
    }
    test:assertEquals(result.status, "FAILED");
    test:assertEquals(result.httpStatus, 403);
}

// ── At-most-once execution ───────────────────────────────────────────────────

@test:Config {}
function testRedeliveredCommandReplaysTheStoredResult() {
    // Redelivery is normal: a result can be lost after the operation ran. Executing again
    // would repeat a mutation, so the stored result is replayed instead.
    resetExecutorState();
    TunneledCommandPayload payload = commandPayload("wfc-replay-1", "humanTasks.complete",
            {taskId: "humantask-1"});

    TunneledCommandResult? first = executeTunneledCommand(payload, okExecutor, true);
    TunneledCommandResult? second = executeTunneledCommand(payload, okExecutor, true);

    test:assertEquals(executorCallCount(), 1, "A redelivered command must not execute twice");
    if first is () || second is () {
        test:assertFail("Both deliveries must produce a result to post");
    }
    test:assertEquals(second, first, "The replayed result must be the stored one");
}

@test:Config {}
function testCommandExecutingInAnotherRoundIsSkipped() {
    // Two heartbeat rounds can carry the same command concurrently. The second must post
    // nothing at all — the round that owns the execution answers.
    resetExecutorState();
    string commandId = "wfc-inflight-1";

    // Claim it, as the owning round does before running the executor.
    TunneledCommandResult|boolean reservation = reserveOrReplay(commandId);
    test:assertTrue(reservation is boolean && reservation, "The first caller must own the execution");

    TunneledCommandResult? result = executeTunneledCommand(commandPayload(commandId), okExecutor, true);

    test:assertTrue(result is (), "A command already in flight must produce nothing to post");
    test:assertEquals(executorCallCount(), 0, "The second round must not execute it");

    // Once the owner stores its result, a later redelivery replays that result.
    storeCommandResult({
        runtimeId: "runtime-test",
        commandId: commandId,
        status: "COMPLETED",
        httpStatus: 200,
        body: {"done": true}
    });
    TunneledCommandResult? replayed = executeTunneledCommand(commandPayload(commandId), okExecutor, true);
    if replayed is () {
        test:assertFail("A stored result must be replayed once the owner finished");
    }
    test:assertEquals(replayed.body, {"done": true});
    test:assertEquals(executorCallCount(), 0);
}

@test:Config {}
function testResultCacheEvictsOldestFirst() {
    // The cache is bounded, so a redelivery long after the fact re-executes rather than
    // letting the bridge grow without limit. Only the oldest entries lose their replay.
    resetExecutorState();
    string oldest = "wfc-evict-oldest";
    TunneledCommandResult? first = executeTunneledCommand(commandPayload(oldest), okExecutor, true);
    test:assertTrue(first !is (), "The first command must produce a result");

    foreach int i in 0 ..< PROCESSED_COMMAND_CACHE_CAPACITY {
        TunneledCommandResult? filler = executeTunneledCommand(
                commandPayload(string `wfc-evict-filler-${i}`), okExecutor, true);
        test:assertTrue(filler !is (), "Each filler command must produce a result");
    }

    int callsBefore = executorCallCount();
    TunneledCommandResult? afterEviction = executeTunneledCommand(
            commandPayload(oldest), okExecutor, true);
    test:assertTrue(afterEviction !is (), "The evicted command must be executed again");
    test:assertEquals(executorCallCount(), callsBefore + 1,
            "An evicted commandId is no longer replayable, so it executes again");

    // The most recent command is still replayable.
    string newest = string `wfc-evict-filler-${PROCESSED_COMMAND_CACHE_CAPACITY - 1}`;
    int callsBeforeReplay = executorCallCount();
    TunneledCommandResult? replayed = executeTunneledCommand(commandPayload(newest), okExecutor, true);
    test:assertTrue(replayed !is (), "A cached command must still answer");
    test:assertEquals(executorCallCount(), callsBeforeReplay,
            "A cached commandId must replay rather than execute");
}
