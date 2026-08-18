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

// The Try-It host is derived from a configured URL that operators write by hand, so the
// parsing has to survive the ways they write it: with or without a scheme, with a port, with
// a trailing slash, with a path someone pasted along.

import ballerina/test;

@test:Config {}
function testAuthorityOfConfiguredUrl() {
    [string, string][] cases = [
        ["http://localhost", "localhost"],
        ["https://localhost:9090", "localhost:9090"],
        ["localhost:9090", "localhost:9090"],
        ["http://localhost:9090/", "localhost:9090"],
        ["http://localhost:9090///", "localhost:9090"],
        ["http://localhost:9090/some/path", "localhost:9090"],
        ["  http://icp.example.com:8443/console  ", "icp.example.com:8443"],
        ["https://10.0.0.7", "10.0.0.7"]
    ];
    foreach [string, string] [configured, expected] in cases {
        test:assertEquals(authorityOf(configured), expected,
                string `Authority of '${configured}'`);
    }
}
