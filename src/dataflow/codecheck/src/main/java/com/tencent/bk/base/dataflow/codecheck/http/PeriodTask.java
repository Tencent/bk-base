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

package com.tencent.bk.base.dataflow.codecheck.http;

import static java.util.stream.Collectors.toSet;

import com.gliwka.hyperscan.wrapper.CompileErrorException;
import com.tencent.bk.base.dataflow.codecheck.MethodParserGroup;
import com.tencent.bk.base.dataflow.codecheck.util.BlackListGroup;
import com.tencent.bk.base.dataflow.codecheck.util.db.CodeCheckDBUtil;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeriodTask implements Runnable {

    public static Logger logger = LoggerFactory.getLogger(PeriodTask.class);

    /**
     * run
     */
    @Override
    public void run() {
        // init parserGroup and blacklistGroup info from db
        loadParserGroup();
        loadBlacklistGroup();
    }

    private void loadParserGroup() {
        // load parser_group config from db
        List parserGroupFromDB = CodeCheckDBUtil.listParserGroup();
        if (parserGroupFromDB == null) {
            logger.info("can not get parser_group from db");
            return;
        }

        Map<String, Map> parserGroupMapFromDB = new HashMap<>();
        for (Object oneItem : parserGroupFromDB) {
            String parserGroupName = (String) ((Map) oneItem).get("parser_group_name");
            parserGroupMapFromDB.put(parserGroupName, (Map) oneItem);
        }
        Set<String> parserGroupSetFromDB = parserGroupMapFromDB.keySet();
        logger.info("get current parser_group in db, cnt {}", parserGroupSetFromDB.size());

        // get current parser_groups
        Map parserGroupMap = MethodParserGroup.getSingleton().listParserGroup(null);
        Set<String> currentParserGroupSet = parserGroupMap.keySet();
        Set<String> addedParserGroup = parserGroupSetFromDB.stream().filter(
                item -> !currentParserGroupSet.contains(item)).collect(toSet());

        Set<String> deletedParserGroup = currentParserGroupSet.stream().filter(
                item -> !parserGroupSetFromDB.contains(item)).collect(toSet());

        for (String parserGroupName : addedParserGroup) {
            logger.info("add parser_group {}", parserGroupName);
            String libDirName = (String) (parserGroupMapFromDB.get(parserGroupName)).get("lib_dir");
            String sourceDirName = (String) (parserGroupMapFromDB.get(parserGroupName)).get("source_dir");
            try {
                MethodParserGroup.getSingleton().addParserGroup(parserGroupName, libDirName, sourceDirName);
            } catch (IOException e) {
                logger.warn("add parser_group {} exception {}", parserGroupName, e.getMessage());
            }
        }
        logger.info("add parser_group cnt {}", addedParserGroup.size());

        for (String parserGroupName : deletedParserGroup) {
            logger.info("delete parser_group {}", parserGroupName);
            MethodParserGroup.getSingleton().deleteParserGroup(parserGroupName);
        }
        logger.info("delete parser_group cnt {}", deletedParserGroup.size());
    }

    private void loadBlacklistGroup() {
        // load blacklist_group config from db
        List blacklistGroupsFromDB = CodeCheckDBUtil.listBlacklistGroup();
        if (blacklistGroupsFromDB == null) {
            logger.info("can not get blacklist_group from db");
            return;
        }
        Map<String, Map> blacklistGroupMapFromDB = new HashMap<>();
        for (Object oneItem : blacklistGroupsFromDB) {
            String blacklistGroupName = (String) ((Map) oneItem).get("blacklist_group_name");
            blacklistGroupMapFromDB.put(blacklistGroupName, (Map) oneItem);
        }
        Set<String> blacklistGroupSetFromDB = blacklistGroupMapFromDB.keySet();
        logger.info("get current blacklist_group in db, cnt {}", blacklistGroupSetFromDB.size());

        // get current blacklist_groups
        Map blacklistGroupMap = BlackListGroup.getSingleton().listBlackListGroup(null);
        Set<String> currentBlacklistGroupSet = blacklistGroupMap.keySet();
        Set<String> addedBlacklistGroup = blacklistGroupSetFromDB.stream().filter(
                item -> !currentBlacklistGroupSet.contains(item)).collect(toSet());

        Set<String> deletedBlacklistGroup = currentBlacklistGroupSet.stream().filter(
                item -> !blacklistGroupSetFromDB.contains(item)).collect(toSet());

        for (String blacklistGroupName : addedBlacklistGroup) {
            logger.info("add blacklist_group {}", blacklistGroupName);
            String blacklist = (String) blacklistGroupMapFromDB.get(blacklistGroupName).get("blacklist");
            try {
                BlackListGroup.getSingleton().addBlackListGroup(blacklistGroupName, blacklist);
            } catch (CompileErrorException e) {
                logger.warn("add blacklist_group {} exception {}", blacklistGroupName, e.getMessage());
            }
        }
        logger.info("add blacklist_group cnt {}", addedBlacklistGroup.size());

        for (String blacklistGroupName : deletedBlacklistGroup) {
            logger.info("delete blacklist_group {}", blacklistGroupName);
            BlackListGroup.getSingleton().deleteBlackListGroup(blacklistGroupName);
        }
        logger.info("delete blacklist_group cnt {}", deletedBlacklistGroup.size());
    }
}
