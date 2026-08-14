// Copyright (c) 2026, WSO2 LLC. (http://wso2.com).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import ballerina/lang.runtime;
import ballerina/log;
import ballerina/task;
import ballerina/time;

function init() returns error? {
    log:printInfo("Starting ICP agent...");

    // Load configuration
    IcpConfig config = check loadConfig();
    log:printInfo("Loaded ICP configuration: " + config.toJsonString());

    // Initialize ICP client (JWT is generated internally from config)
    IcpClient icpClient = check new (config);
    log:printInfo("ICP agent initialized with server URL: " + config.serverUrl);

    // Send initial heartbeat to register with ICP server. Fields the ICP server confirmed
    // it understands come back on this same call — no extra round-trip needed to discover
    // them.
    string[]|error supportedFieldsResult = sendInitialHeartbeat(icpClient);
    if supportedFieldsResult is error {
        log:printError("Failed initial heartbeat registration with ICP server", supportedFieldsResult);
        return;
    }
    string[] supportedFields = supportedFieldsResult;

    worker w1 returns error? {
        check startICPAgent(icpClient, config, supportedFields);
    }

}

function sendInitialHeartbeat(IcpClient icpClient) returns string[]|error {
    Heartbeat|error heartbeat = getHeartbeat();
    if heartbeat is error {
        log:printError("Failed to create initial heartbeat", heartbeat);
        return heartbeat;
    }
    HeartbeatResponse|error heartbeatResponse = icpClient->sendHeartbeat(heartbeat);
    if heartbeatResponse is error {
        log:printError("Failed to send initial heartbeat", heartbeatResponse);
        return heartbeatResponse;
    }
    if !heartbeatResponse.acknowledged {
        log:printError("Initial heartbeat not acknowledged by ICP server");
        return error("Initial heartbeat not acknowledged by ICP server");
    }
    return heartbeatResponse.supportedHeartbeatFields ?: [];
}

function startICPAgent(IcpClient icpClient, IcpConfig config, string[] supportedHeartbeatFields) returns error? {
    // Start periodic heartbeat
    HeartbeatJob heartbeatJob = check new (icpClient, <decimal>config.heartbeatInterval, supportedHeartbeatFields);
    task:JobId|task:Error result = task:scheduleJobRecurByFrequency(heartbeatJob, <decimal>config.heartbeatInterval);
    if result is task:Error {
        log:printError("Failed to start heartbeat job", result);
        return error("Heartbeat scheduling failed");
    }

    log:printInfo("ICP agent started successfully with job ID: " + result.toString());

    // Keep the main function running to allow periodic tasks to execute
    while true {
        // Sleep for a while to prevent busy waiting
        runtime:sleep(1.0);
    }
}

// Heartbeat job
public class HeartbeatJob {
    *task:Job;
    private final IcpClient icpClient;
    private final decimal interval;
    private int attemptCount = 0;
    private Heartbeat heartbeat;
    private boolean fullHeartbeatRequired = true;
    private string[] supportedHeartbeatFields;

    public function init(IcpClient icpClient, decimal interval, string[] supportedHeartbeatFields = []) returns error? {
        self.icpClient = icpClient;
        self.interval = interval;
        self.supportedHeartbeatFields = supportedHeartbeatFields;
        self.heartbeat = check getHeartbeat(self.supportedHeartbeatFields);
    }

    # Executes the heartbeat job: one heartbeat round, plus bounded follow-up rounds
    # while the server is actively tunneling work. A follow-up happens immediately
    # after executing a workflow command (its result may already have unblocked the
    # next queued command) or after the server's `nextHeartbeatInSeconds` boost hint
    # (sent while a user is actively working with workflow views). Follow-ups stop
    # once their accumulated delay would exceed one regular interval, so a tick never
    # runs much past the next scheduled one — which then continues the boost.
    public function execute() {
        decimal boostBudget = self.interval;
        while true {
            decimal? followUpDelay = self.heartbeatRound();
            if followUpDelay is () {
                return;
            }
            // An immediate follow-up (delay 0, after executing a command) still consumes
            // budget so a long command queue cannot keep this tick spinning forever.
            decimal consumed = decimal:max(followUpDelay, 1);
            if consumed > boostBudget {
                return;
            }
            boostBudget -= consumed;
            if followUpDelay > 0d {
                runtime:sleep(followUpDelay);
            }
        }
    }

    # Sends one heartbeat (full or delta), processes the response, and decides whether
    # a follow-up round is wanted.
    #
    # + return - Seconds to wait before the follow-up round (0 = immediately), or `()`
    #            when no follow-up is needed this tick
    function heartbeatRound() returns decimal? {
        HeartbeatResponse|error heartbeatResponse;
        if (self.fullHeartbeatRequired) {
            Heartbeat|error newHeartbeat = getHeartbeat(self.supportedHeartbeatFields);
            if newHeartbeat is error {
                log:printError("Failed to create full heartbeat", newHeartbeat);
                return ();
            }
            self.heartbeat = newHeartbeat;
            log:printInfo("Sending full heartbeat to ICP server");
            heartbeatResponse = self.icpClient->sendHeartbeat(self.heartbeat);
        } else {
            // Create delta heartbeat with hash
            DeltaHeartbeat|error deltaHeartbeat = getDeltaHeartbeat(self.heartbeat);
            if deltaHeartbeat is error {
                log:printError("Failed to create delta heartbeat", deltaHeartbeat);
                return ();
            }
            log:printDebug("Sending delta heartbeat to ICP server");
            heartbeatResponse = self.icpClient->sendDeltaHeartbeat(deltaHeartbeat);
        }
        if heartbeatResponse is error {
            log:printError("Heartbeat response error", heartbeatResponse);
            return ();
        }
        if !heartbeatResponse.acknowledged {
            return ();
        }
        self.fullHeartbeatRequired = heartbeatResponse.fullHeartbeatRequired ?: false;
        string[] newSupportedHeartbeatFields = heartbeatResponse.supportedHeartbeatFields ?: [];
        if newSupportedHeartbeatFields != self.supportedHeartbeatFields {
            // Server's understood field set changed since the last ack (e.g. it was
            // upgraded mid-connection) — send a full heartbeat next so newly available (or
            // newly unsupported) optional fields take effect promptly instead of waiting on
            // an unrelated trigger for the next full heartbeat.
            self.fullHeartbeatRequired = true;
        }
        self.supportedHeartbeatFields = newSupportedHeartbeatFields;
        log:printDebug("Heartbeat acknowledged by ICP server");
        boolean executedWorkflowCommand = self.handleControlCommands(heartbeatResponse.commands);
        if executedWorkflowCommand {
            // Fetch the next queued command right away — the posted result has likely
            // unblocked the ICP-side caller already.
            return 0;
        }
        int? boostHint = heartbeatResponse.nextHeartbeatInSeconds;
        if boostHint is int && boostHint > 0 && <decimal>boostHint < self.interval {
            return <decimal>boostHint;
        }
        return ();
    }

    # Handles the control commands delivered in a heartbeat response.
    #
    # + commands - The commands from the response
    # + return - `true` when at least one tunneled workflow command was processed,
    #            so the caller can immediately fetch the next queued command
    function handleControlCommands(ControlCommand[] commands) returns boolean {
        if commands.length() == 0 {
            return false;
        }

        boolean artifactsChanged = false;
        boolean workflowCommandProcessed = false;
        foreach ControlCommand command in commands {
            log:printInfo(string `Handling control command: ${command.toJsonString()}`);
            command.status = PENDING;

            // Handle different command actions
            error? result = ();
            match command.action {
                WORKFLOW_MGMT => {
                    workflowCommandProcessed = true;
                    result = self.handleWorkflowCommand(command.payload ?: "");
                }
                START|STOP => {
                    string artifactName = command.targetArtifact.name;
                    boolean isStart = command.action == START;
                    string action = isStart ? "start" : "stop";

                    log:printInfo(string `${isStart ? "Starting" : "Stopping"} listener: ${artifactName}`);

                    // Execute the control action
                    boolean|error actionResult = isStart
                        ? startListenerArtifact(artifactName)
                        : stopListenerArtifact(artifactName);

                    if actionResult is error || actionResult == false {
                        log:printError(string `Failed to ${action} listener: ${artifactName}`, actionResult is error ? actionResult : ());
                        result = actionResult is error ? actionResult : error(string `Failed to ${action} listener: ${artifactName}`);
                    } else {
                        log:printInfo(string `Successfully ${action}ed listener: ${artifactName}`);
                        artifactsChanged = true;
                    }
                }
                SET_LOGGER_LEVEL => {
                    // Parse the payload
                    string payload = command.payload ?: "";
                    if payload == "" {
                        result = error("Missing payload for SET_LOGGER_LEVEL command");
                    } else {
                        LoggerLevelPayload|error loggerPayload = payload.fromJsonStringWithType();
                        if loggerPayload is error {
                            result = error(string `Failed to parse logger level payload: ${loggerPayload.message()}`);
                        } else {
                            log:printInfo(string `Setting log level to ${loggerPayload.logLevel} for logger: ${loggerPayload.componentName}`);
                            string? packageName = loggerPayload.componentPackage;
                            string trimmedPackage = (packageName is string) ? packageName.trim() : "";
                            string loggerId = (trimmedPackage.length() > 0)
                                ? trimmedPackage + ":" + loggerPayload.componentName
                                : loggerPayload.componentName;
                            result = setLoggerLevel(loggerId, loggerPayload.logLevel);
                            if result is () {
                                log:printInfo(string `Successfully set log level for logger : ${loggerPayload.componentName}`);
                                artifactsChanged = true;
                            }
                        }
                    }
                }
            }

            // Update command status based on result
            if result is error {
                log:printError(string `Command failed: ${command.commandId}`, result);
                command.status = FAILED;
            } else {
                command.status = COMPLETED;
            }
        }

        if artifactsChanged {
            Heartbeat|error newHeartbeat = getHeartbeat(self.supportedHeartbeatFields);
            if newHeartbeat is error {
                log:printError("Failed to create full heartbeat after control command", newHeartbeat);
                return workflowCommandProcessed;
            }
            self.heartbeat = newHeartbeat;
        }
        return workflowCommandProcessed;
    }

    # Executes one tunneled workflow management command and posts its result to the
    # ICP. A command past its deadline is dropped unexecuted — the ICP-side caller
    # has already timed out, and executing (or replying) then would be wasted work
    # or, for mutations, an unwanted late effect.
    #
    # + rawPayload - The command's JSON payload string
    # + return - An error when the payload is unusable or the result could not be
    #            delivered (the command's status is reported FAILED then)
    function handleWorkflowCommand(string rawPayload) returns error? {
        if rawPayload == "" {
            return error("Missing payload for WORKFLOW_MGMT command");
        }
        WorkflowCommandPayload payload = check rawPayload.fromJsonStringWithType();

        string? deadline = payload?.deadline;
        if deadline is string {
            time:Utc|time:Error deadlineTime = time:utcFromString(deadline);
            if deadlineTime is time:Utc && time:utcDiffSeconds(deadlineTime, time:utcNow()) < 0d {
                log:printWarn(string `Dropping expired workflow command ${payload.commandId} ` +
                        string `(deadline ${deadline})`);
                return;
            }
        }

        WorkflowCommandResult result = executeWorkflowCommand(payload);
        check self.icpClient->sendCommandResult(result);
    }
}
