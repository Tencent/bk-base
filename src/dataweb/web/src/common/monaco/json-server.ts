/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
 */
import * as fs from 'fs';
import { xhr, getErrorStatusDescription } from 'request-light';
import URI from 'vscode-uri';
import { MessageReader, MessageWriter } from 'vscode-jsonrpc';
import { IConnection, TextDocuments, createConnection } from 'vscode-languageserver';
import {
  TextDocument, Diagnostic, Command, CompletionList, CompletionItem, Hover,
  SymbolInformation, DocumentSymbolParams, TextEdit, FoldingRange, ColorInformation, ColorPresentation
} from 'vscode-languageserver-types';
import {
  TextDocumentPositionParams, DocumentRangeFormattingParams, ExecuteCommandParams, CodeActionParams,
  FoldingRangeRequestParam, DocumentColorParams, ColorPresentationParams
} from 'vscode-languageserver-protocol';
import { getLanguageService, LanguageService, JSONDocument } from 'vscode-json-languageservice';

export function start(reader: MessageReader, writer: MessageWriter): JsonServer {
  const connection = createConnection(reader, writer);
  const server = new JsonServer(connection);
  server.start();
  return server;
}

export class JsonServer {
  protected workspaceRoot: URI | undefined;
  protected readonly documents = new TextDocuments();
  protected readonly jsonService: LanguageService = getLanguageService({
    schemaRequestService: this.resolveSchema.bind(this)
  });

  protected readonly pendingValidationRequests = new Map<string, number>();
  constructor(protected readonly connection: IConnection) {
    this.documents.listen(this.connection);
    this.documents.onDidChangeContent(change => this.validate(change.document));
    this.documents.onDidClose(event => {
      this.cleanPendingValidation(event.document);
      this.cleanDiagnostics(event.document);
    });

    this.connection.onInitialize(params => {
      if (params.rootPath) {
        this.workspaceRoot = URI.file(params.rootPath);
      } else if (params.rootUri) {
        this.workspaceRoot = URI.parse(params.rootUri);
      }
      this.connection.console.log('The server is initialized.');
      return {
        capabilities: {
          textDocumentSync: this.documents.syncKind,
          codeActionProvider: true,
          completionProvider: {
            resolveProvider: true,
            triggerCharacters: ['"', ':']
          },
          hoverProvider: true,
          documentSymbolProvider: true,
          documentRangeFormattingProvider: true,
          executeCommandProvider: {
            commands: ['json.documentUpper']
          },
          colorProvider: true,
          foldingRangeProvider: true
        }
      };
    });
    this.connection.onCodeAction(params => this.codeAction(params));
    this.connection.onCompletion(params => this.completion(params));
    this.connection.onCompletionResolve(item => this.resolveCompletion(item));
    this.connection.onExecuteCommand(params => this.executeCommand(params));
    this.connection.onHover(params => this.hover(params));
    this.connection.onDocumentSymbol(params => this.findDocumentSymbols(params));
    this.connection.onDocumentRangeFormatting(params => this.format(params));
    this.connection.onDocumentColor(params => this.findDocumentColors(params));
    this.connection.onColorPresentation(params => this.getColorPresentations(params));
    this.connection.onFoldingRanges(params => this.getFoldingRanges(params));
  }

  /** start */
  start() {
    this.connection.listen();
  }

  /** getFoldingRanges */
  protected getFoldingRanges(params: FoldingRangeRequestParam): FoldingRange[] {
    const document = this.documents.get(params.textDocument.uri);
    if (!document) {
      return [];
    }
    return this.jsonService.getFoldingRanges(document);
  }

  /** findDocumentColors */
  protected findDocumentColors(params: DocumentColorParams): Thenable<ColorInformation[]> {
    const document = this.documents.get(params.textDocument.uri);
    if (!document) {
      return Promise.resolve([]);
    }
    const jsonDocument = this.getJSONDocument(document);
    return this.jsonService.findDocumentColors(document, jsonDocument);
  }

  /** getColorPresentations */
  protected getColorPresentations(params: ColorPresentationParams): ColorPresentation[] {
    const document = this.documents.get(params.textDocument.uri);
    if (!document) {
      return [];
    }
    const jsonDocument = this.getJSONDocument(document);
    return this.jsonService.getColorPresentations(document, jsonDocument, params.color, params.range);
  }

  /** codeAction */
  protected codeAction(params: CodeActionParams): Command[] {
    const document = this.documents.get(params.textDocument.uri);
    if (!document) {
      return [];
    }
    return [{
      title: 'Upper Case Document',
      command: 'json.documentUpper',
      // Send a VersionedTextDocumentIdentifier
      arguments: [{
        ...params.textDocument,
        version: document.version
      }]
    }];
  }

  /** format */
  protected format(params: DocumentRangeFormattingParams): TextEdit[] {
    const document = this.documents.get(params.textDocument.uri);
    return document ? this.jsonService.format(document, params.range, params.options) : [];
  }

  /** findDocumentSymbols */
  protected findDocumentSymbols(params: DocumentSymbolParams): SymbolInformation[] {
    const document = this.documents.get(params.textDocument.uri);
    if (!document) {
      return [];
    }
    const jsonDocument = this.getJSONDocument(document);
    return this.jsonService.findDocumentSymbols(document, jsonDocument);
  }

  /** executeCommand */
  protected executeCommand(params: ExecuteCommandParams): any {
    if (params.command === 'json.documentUpper' && params.arguments) {
      const versionedTextDocumentIdentifier = params.arguments[0];
      const document = this.documents.get(versionedTextDocumentIdentifier.uri);
      if (document) {
        this.connection.workspace.applyEdit({
          documentChanges: [{
            textDocument: versionedTextDocumentIdentifier,
            edits: [{
              range: {
                start: { line: 0, character: 0 },
                end: { line: Number.MAX_SAFE_INTEGER, character: Number.MAX_SAFE_INTEGER }
              },
              newText: document.getText().toUpperCase()
            }]
          }]
        });
      }
    }
  }

  protected hover(params: TextDocumentPositionParams): Thenable<Hover | null> {
    const document = this.documents.get(params.textDocument.uri);
    if (!document) {
      return Promise.resolve(null);
    }
    const jsonDocument = this.getJSONDocument(document);
    return this.jsonService.doHover(document, params.position, jsonDocument);
  }

  protected async resolveSchema(url: string): Promise<string> {
    const uri = URI.parse(url);
    if (uri.scheme === 'file') {
      return new Promise<string>((resolve, reject) => {
        fs.readFile(uri.fsPath, 'UTF-8', (err, result) => {
          err ? reject('') : resolve(result.toString());
        });
      });
    }
    try {
      const response = await xhr({ url, followRedirects: 5 });
      return response.responseText;
    }
    catch (error) {
      return Promise.reject(error.responseText || getErrorStatusDescription(error.status) || error.toString());
    }
  }

  protected resolveCompletion(item: CompletionItem): Thenable<CompletionItem> {
    return this.jsonService.doResolve(item);
  }

  protected completion(params: TextDocumentPositionParams): Thenable<CompletionList | null> {
    const document = this.documents.get(params.textDocument.uri);
    if (!document) {
      return Promise.resolve(null);
    }
    const jsonDocument = this.getJSONDocument(document);
    return this.jsonService.doComplete(document, params.position, jsonDocument);
  }

  protected validate(document: TextDocument): void {
    this.cleanPendingValidation(document);
    this.pendingValidationRequests.set(document.uri, setTimeout(() => {
      this.pendingValidationRequests.delete(document.uri);
      this.doValidate(document);
    }));
  }

  protected cleanPendingValidation(document: TextDocument): void {
    const request = this.pendingValidationRequests.get(document.uri);
    if (request !== undefined) {
      clearTimeout(request);
      this.pendingValidationRequests.delete(document.uri);
    }
  }

  protected doValidate(document: TextDocument): void {
    if (document.getText().length === 0) {
      this.cleanDiagnostics(document);
      return;
    }
    const jsonDocument = this.getJSONDocument(document);
    this.jsonService.doValidation(document, jsonDocument)
      .then(diagnostics => this.sendDiagnostics(document, diagnostics));
  }

  protected cleanDiagnostics(document: TextDocument): void {
    this.sendDiagnostics(document, []);
  }

  protected sendDiagnostics(document: TextDocument, diagnostics: Diagnostic[]): void {
    this.connection.sendDiagnostics({
      uri: document.uri, diagnostics
    });
  }

  protected getJSONDocument(document: TextDocument): JSONDocument {
    return this.jsonService.parseJSONDocument(document);
  }
}
