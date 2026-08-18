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
// a trailing slash, with a path someone pasted along, and as an IPv6 literal — whose own
// colons must not be mistaken for a port separator.

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
        ["https://10.0.0.7", "10.0.0.7"],
        ["http://[::1]:9090", "[::1]:9090"],
        ["http://[2001:db8::1]:8443/console", "[2001:db8::1]:8443"],
        ["https://[fe80::1]", "[fe80::1]"]
    ];
    foreach [string, string] [configured, expected] in cases {
        test:assertEquals(authorityOf(configured), expected,
                string `Authority of '${configured}'`);
    }
}

// getTryItHost drops the port — the Try-It proxy knows the target port separately — which for
// IPv6 means splitting at the colon *after* the closing bracket, not the first one in the
// address. Splitting on the first colon returned "[" for a URL like http://[::1]:9090.
@test:Config {}
function testHostOfAuthority() {
    [string, string][] cases = [
        ["localhost", "localhost"],
        ["localhost:9090", "localhost"],
        ["icp.example.com:8443", "icp.example.com"],
        ["10.0.0.7:9090", "10.0.0.7"],
        ["[::1]:9090", "[::1]"],
        ["[2001:db8::1]:8443", "[2001:db8::1]"],
        ["[fe80::1]", "[fe80::1]"],
        // Malformed but not this function's problem to reject: it must not mangle it either.
        ["[::1", "[::1"]
    ];
    foreach [string, string] [authority, expected] in cases {
        test:assertEquals(hostOf(authority), expected, string `Host of '${authority}'`);
    }
}
