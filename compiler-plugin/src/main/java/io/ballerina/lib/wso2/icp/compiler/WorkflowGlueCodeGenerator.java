/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com) All Rights Reserved.
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.lib.wso2.icp.compiler;

import io.ballerina.compiler.syntax.tree.IdentifierToken;
import io.ballerina.compiler.syntax.tree.ImportDeclarationNode;
import io.ballerina.compiler.syntax.tree.ModulePartNode;
import io.ballerina.compiler.syntax.tree.SeparatedNodeList;
import io.ballerina.projects.Document;
import io.ballerina.projects.DocumentId;
import io.ballerina.projects.Module;
import io.ballerina.projects.Package;
import io.ballerina.projects.plugins.CodeGenerator;
import io.ballerina.projects.plugins.CodeGeneratorContext;
import io.ballerina.projects.plugins.GeneratorTask;
import io.ballerina.projects.plugins.SourceGeneratorContext;
import io.ballerina.tools.text.TextDocument;
import io.ballerina.tools.text.TextDocuments;

/**
 * Generates the glue that wires an integration's workflow runtime into this bridge.
 *
 * <p>When the user's package imports {@code ballerina/workflow}, a source file is added to the
 * default module that registers {@code workflow.management.rest}'s metadata provider and command
 * executor with the bridge ({@code registerWorkflowIntegration}). With that, importing
 * {@code wso2/icp.runtime.bridge} is all an integration needs for the ICP to receive workflow
 * metadata in heartbeats and (when {@code enableWorkflowManagement = true}) to tunnel workflow
 * management commands — no {@code workflow.management} import, HTTP management API, or extra
 * configuration in the user's code.
 *
 * <p>The generated file imports {@code ballerina/workflow.management.rest}, which resolves from
 * the same {@code ballerina/workflow} package the user already depends on (its REST listener
 * only starts when {@code enableManagementApi = true}, which defaults to false — so no port is
 * opened by this glue). The bridge package itself keeps zero compile-time dependency on
 * {@code ballerina/workflow}.
 */
public class WorkflowGlueCodeGenerator extends CodeGenerator {

    @Override
    public void init(CodeGeneratorContext generatorContext) {
        generatorContext.addSourceGeneratorTask(new WorkflowGlueGeneratorTask());
    }

    private static final class WorkflowGlueGeneratorTask implements GeneratorTask<SourceGeneratorContext> {

        private static final String WORKFLOW_ORG = "ballerina";
        private static final String WORKFLOW_PACKAGE = "workflow";
        private static final String GLUE_FILE_PREFIX = "icp_workflow_glue";

        @Override
        public void generate(SourceGeneratorContext context) {
            // Never add sources to a package that already fails to compile.
            if (context.compilation().diagnosticResult().hasErrors()) {
                return;
            }
            if (!usesWorkflowPackage(context.currentPackage())) {
                return;
            }
            TextDocument glue = TextDocuments.from(glueSource());
            context.addSourceFile(glue, GLUE_FILE_PREFIX);
        }

        /**
         * Reports whether any source document in the package imports {@code ballerina/workflow}
         * (or one of its modules, e.g. {@code ballerina/workflow.management}).
         */
        private boolean usesWorkflowPackage(Package currentPackage) {
            for (var moduleId : currentPackage.moduleIds()) {
                Module module = currentPackage.module(moduleId);
                for (DocumentId documentId : module.documentIds()) {
                    Document document = module.document(documentId);
                    ModulePartNode rootNode = document.syntaxTree().rootNode();
                    for (ImportDeclarationNode importDecl : rootNode.imports()) {
                        if (isWorkflowImport(importDecl)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        private boolean isWorkflowImport(ImportDeclarationNode importDecl) {
            if (importDecl.orgName().isEmpty()
                    || !WORKFLOW_ORG.equals(importDecl.orgName().get().orgName().text())) {
                return false;
            }
            SeparatedNodeList<IdentifierToken> moduleName = importDecl.moduleName();
            return !moduleName.isEmpty() && WORKFLOW_PACKAGE.equals(moduleName.get(0).text());
        }

        /**
         * The generated glue. Identifiers carry an {@code _icp} prefix to stay clear of user
         * symbols; imports are file-scoped, so they never clash with the user's own imports of
         * the same modules under different prefixes.
         */
        private String glueSource() {
            return """
                    // AUTO-GENERATED by the wso2/icp.runtime.bridge compiler plugin. Do not edit.
                    // Wires this integration's workflow runtime into the ICP bridge: workflow
                    // metadata is published in heartbeats, and (when enableWorkflowManagement is
                    // true) management commands tunneled by the ICP are executed in-process.
                    import ballerina/workflow.management.rest as _icpWorkflowMgmt;
                    import wso2/icp.runtime.bridge as _icpBridge;

                    final boolean _icpWorkflowIntegrationRegistered = _icpBridge:registerWorkflowIntegration(
                            _icpWorkflowMetadataProvider, _icpWorkflowCommandExecutor);

                    isolated function _icpWorkflowMetadataProvider() returns map<json>|error {
                        json raw = (check _icpWorkflowMgmt:getWorkflowMetadata()).toJson();
                        if raw is map<json> {
                            return raw;
                        }
                        return error("Unexpected workflow metadata shape");
                    }

                    isolated function _icpWorkflowCommandExecutor(map<json> command) returns map<json>|error {
                        _icpWorkflowMgmt:ManagementCommand managementCommand = check command.cloneWithType();
                        _icpWorkflowMgmt:ManagementCommandResult result =
                                _icpWorkflowMgmt:executeManagementCommand(managementCommand);
                        return {httpStatus: result.httpStatus, body: result.body};
                    }
                    """;
        }
    }
}
