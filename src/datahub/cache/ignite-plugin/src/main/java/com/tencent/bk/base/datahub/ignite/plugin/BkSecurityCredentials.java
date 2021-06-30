/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.tencent.bk.base.datahub.ignite.plugin;

import org.apache.ignite.plugin.security.SecurityCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BkSecurityCredentials {

  private static final Logger log = LoggerFactory.getLogger(BkSecurityCredentials.class);

  private String login;
  private String password;
  private Object userObj;
  private String key;
  private String rootKey;
  private String keyIV;
  private BkAuthConf userAuth;

  /**
   * 构造函数
   *
   * @param sc          SecurityCredentials
   * @param securityCfg 鉴权配置
   */
  public BkSecurityCredentials(SecurityCredentials sc, BkSecurityPluginConf securityCfg) {
    login = sc.getLogin() != null ? sc.getLogin().toString() : "";
    password = sc.getPassword() != null ? sc.getPassword().toString() : "";
    userObj = sc.getUserObject();
    key = securityCfg.getKey();
    rootKey = securityCfg.getRootKey();
    keyIV = securityCfg.getKeyIV();
    userAuth = securityCfg.getUserPermission(login);
  }

  /**
   * 获取账号名
   *
   * @return 账号名
   */
  public String getLogin() {
    return login;
  }

  /**
   * 获取密码
   *
   * @return 密码
   */
  public String getPassword() {
    return password;
  }

  /**
   * 获取用户对象
   *
   * @return 用户对象
   */
  public Object getUserObj() {
    return userObj;
  }

  /**
   * 获取用户权限
   *
   * @return 用户权限
   */
  public BkAuthConf getUserAuth() {
    return userAuth;
  }

  /**
   * 获取解密的key
   *
   * @return key
   */
  public String getKey() {
    return key;
  }

  /**
   * 获取解密的rootKey
   *
   * @return rootKey
   */
  public String getRootKey() {
    return rootKey;
  }

  /**
   * 获取解密的keyIV
   *
   * @return keyIV
   */
  public String getKeyIV() {
    return keyIV;
  }

  /**
   * SecurityCredentials中提供的账户信息是否有效
   *
   * @return True/False
   */
  public boolean isValidCredentials() {
    return !("".equals(login) || userAuth == null || !password.equals(userAuth.getPassword()));
  }
}