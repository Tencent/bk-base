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

import { Component, Prop, Watch, Vue } from 'vue-property-decorator';
import SparkMD5 from 'spark-md5';
import { Component, Prop, Vue, Watch } from 'vue-property-decorator';
import { generateId } from '../../common/js/util.js';

const parseResponse = (response: any) => {
  if (!response) {
    return response || {};
  }
  try {
    return JSON.parse(response);
  } catch (error) {
    return response || {};
  }
};

class ChunkRequest {
  constructor(index: number, fileMd5Value: string) {
    this.xhr = new XMLHttpRequest();
    this.abort = this.xhr.abort;
  }
}

@Component({})
export default class bigFileUploader extends Vue {
  @Prop() public url: string;
  @Prop() public name: string;
  @Prop() public mergeUrl: string;
  @Prop() public validateName: string;
  @Prop() public accept: string;
  @Prop({ default: () => [] }) public formDataAttribute: object[];

  public chunkSize: number = 2 * 1024 * 1024; // 每个分片大小为5MB

  public chunks = 0;

  public hasUploaded = 0;

  public file: any = null;

  public options: object = {};

  public abortList: array<function> = [];

  public handleResCode(res) {
    if (res && res.result === true) {
      return true;
    }
    return false;
  }

  public successHandler(file: object, fileList: object[]) {
    console.log(fileList);
    this.$emit('uploadSuccess', fileList);
  }

  public deleteHandler(file: object, fileList: object[]) {
    this.$emit('fileDelete', file);
  }

  private async upload(options: object) {
    this.$set(this, 'options', options);
    this.file = options.fileObj.origin;
    this.chunks = Math.ceil(this.file.size / this.chunkSize);

    const fileMd5Value = await this.md5File(this.file);
    console.log(fileMd5Value);

    /**  秒传功能接口 */
    // let result = await this.checkFileMd5(file.name, fileMd5Value)
    // if (result.file) {
    //     console.log('秒传')
    //     return
    // }
    const uuid = generateId('offline_', 6);
    const largeUploadResult = await this.checkAndUploadChunks(options, fileMd5Value, uuid);

    if (largeUploadResult) {
      const mergeResult = await this.mergeRequest(fileMd5Value, uuid);
      if (mergeResult.result) {
        mergeResult && this.options.onSuccess({ result: true }, options.fileObj);
        this.options.fileObj.progress = (this.hasUploaded / this.chunks) * 100 + '%';
        this.$emit('uploadFileName', mergeResult.data.task_name);
      } else {
        this.options.fileObj.errorMsg = mergeResult.message;
        this.options.onError(this.options.fileObj, this.options.fileList, mergeResult.message);
      }
      // this.options.onDone(this.options.fileObj)
    } else {
      console.log('merge failed');
    }
    this.hasUploaded = 0;
  }

  private md5File(file: object) {
    const self = this;
    return new Promise((resolve, reject) => {
      const blobSlice = File.prototype.slice || File.prototype.mozSlice || File.prototype.webkitSlice;
      let currentChunk = 0;
      const spark = new SparkMD5.ArrayBuffer();
      const fileReader = new FileReader();
      fileReader.onload = function (e) {
        console.log('read chunk nr', currentChunk + 1, 'of', self.chunks);
        spark.append(e.target.result); // Append array buffer
        currentChunk++;
        if (currentChunk < self.chunks) {
          loadNext();
        } else {
          console.log('finished loading');
          const result = spark.end();
          resolve(result);
        }
      };
      fileReader.onerror = function(err) {
        console.warn('oops, something went wrong.');
        reject(err);
      };
      function loadNext() {
        const start = currentChunk * self.chunkSize;
        const end = start + self.chunkSize >= file.size ? file.size : start + self.chunkSize;
        fileReader.readAsArrayBuffer(blobSlice.call(file, start, end));
      }
      loadNext();
    });
  }

  private async checkAndUploadChunks(options: object, fileMd5Value: string, uuid: string) {
    const requestList = [];
    for (let i = 0; i < this.chunks; i++) {
      requestList.push(this.chunkUpload(i, fileMd5Value, uuid));
    }
    const result = requestList.length
      ? await Promise.all(requestList)
        .then(result => {
          let isSuccess = true;
          result.find(item => {
            if (item.result === false) {
              this.options.fileObj.errorMsg = item.message;
              this.options.onError(this.options.fileObj, this.options.fileList, item.message);
              isSuccess = false;
            }
          });
          return isSuccess;
          // return result.every(res => res.result)
        })
        .catch(err => {
          this.options.fileObj.errorMsg = err.message;
          this.options.onError(this.options.fileObj, this.options.fileList, err);
          return err;
        })
      : true;

    return result === true;
  }

  private async chunkUpload(index: number, fileMd5Value: string, uuid: string) {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      this.options.fileObj.xhr = xhr;

      const formData = new FormData();
      this.formDataAttribute.forEach(item => {
        formData.append(item.name, item.value);
      });
      const end = (index + 1) * this.chunkSize >= this.file.size ? this.file.size : (index + 1) * this.chunkSize;

      formData.append(this.options.fileName, this.file.slice(index * this.chunkSize, end));
      formData.append('chunk_id', index);
      formData.append('task_name', this.file.name);
      formData.append('md5', fileMd5Value);
      uuid && formData.append('upload_uuid', uuid);

      console.log(formData.get(this.options.fileName), index);

      xhr.onreadystatechange = () => {
        if (xhr.readyState === 4) {
          const responseText = parseResponse(xhr.responseText || xhr.response);
          if (xhr.status < 200 || xhr.status >= 300) {
            this.options.fileObj.progress = 100 + '%';
            reject(new Error(responseText));
          } else {
            this.hasUploaded++;
            this.options.fileObj.progress = (this.hasUploaded / this.chunks) * 100 * 0.95 + '%';
            resolve(responseText);
          }
        }
      };

      xhr.withCredentials = this.options.withCredentials;
      xhr.open(this.options.method, this.options.url, true);

      if (this.options.header) {
        if (Array.isArray(this.options.header)) {
          this.options.header.forEach(head => {
            const headerKey = head.name;
            const headerVal = head.value;
            xhr.setRequestHeader(headerKey, headerVal);
          });
        } else {
          const headerKey = this.options.header.name;
          const headerVal = this.options.header.value;
          xhr.setRequestHeader(headerKey, headerVal);
        }
      }
      xhr.send(formData);
    });
  }

  private mergeRequest(md5Value: string, uuid: string) {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      console.log(this.mergeUrl);
      xhr.open('POST', this.mergeUrl, true);
      xhr.withCredentials = this.options.withCredentials;

      xhr.setRequestHeader('Content-type', 'application/x-www-form-urlencoded');

      xhr.onreadystatechange = () => {
        if (xhr.readyState === 4) {
          const responseText = parseResponse(xhr.responseText || xhr.response);
          if (xhr.status < 200 || xhr.status >= 300) {
            this.options.fileObj.errorMsg = responseText;
            this.options.onError(this.options.fileObj, this.options.fileList, xhr.response);
            reject(new Error(responseText));
          } else {
            resolve(responseText);
          }
        }
      };

      this.abortList.push(xhr.abort);

      const formDataObj = {};
      this.formDataAttribute.forEach(item => {
        formDataObj[item.name] = item.value;
      });

      xhr.send(
        JSON.stringify(
          Object.assign(
            {
              task_name: this.file.name,
              md5: md5Value,
              chunk_size: this.chunks,
              upload_uuid: uuid,
            },
            formDataObj
          )
        )
      );
    });
  }
}
