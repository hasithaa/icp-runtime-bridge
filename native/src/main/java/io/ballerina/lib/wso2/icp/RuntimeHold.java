/*
 * Copyright (c) 2026, WSO2 LLC. (http://wso2.com).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.ballerina.lib.wso2.icp;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BObject;

/**
 * Keeps a program running for as long as this bridge offers an entry point into it.
 *
 * <p>A Ballerina program runs while the runtime holds a registered listener. An integration
 * whose only inbound surface is workflow management has none: the workflow worker polls a task
 * queue but cannot be triggered by itself, and this bridge's heartbeat scheduler does not hold
 * the program either — so such an integration registers its workflows and immediately exits,
 * with nothing left to serve the commands the ICP would tunnel to it.
 *
 * <p>Registering a listener at run time rather than declaring one keeps the decision where the
 * facts are: the hold is taken only once a workflow integration has registered and only while
 * workflow management is enabled, neither of which a compiler plugin can know. Note that the
 * hold cannot be given back — deregistering a listener does not let the program exit — so it
 * must not be taken speculatively.
 *
 * @since 0.3.0
 */
public final class RuntimeHold {

    private RuntimeHold() {
    }

    /**
     * Registers {@code listener} with the runtime, so the program keeps running until it is
     * stopped.
     *
     * @param env      the Ballerina runtime environment
     * @param listener the listener object to hold the program open
     */
    public static void takeHold(Environment env, BObject listener) {
        env.getRuntime().registerListener(listener);
    }
}
