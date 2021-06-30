# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.

Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.

BK-BASE 蓝鲸基础平台 is licensed under the MIT License.

License for BK-BASE 蓝鲸基础平台:
--------------------------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

ACCEPTED = "ACCEPTED"
FAILED = "FAILED"
FINISHED = "FINISHED"
KILLED = "KILLED"
NEW = "NEW"
NEW_SAVING = "NEW_SAVING"
RUNNING = "RUNNING"
SUBMITTED = "SUBMITTED"
SUCCEEDED = "SUCCEEDED"
UNDEFINED = "UNDEFINED"
INITING = "INITING"
INITED = "INITED"
FINISHING_CONTAINERS_WAIT = "FINISHING_CONTAINERS_WAIT"
APPLICATION_RESOURCES_CLEANINGUP = "APPLICATION_RESOURCES_CLEANINGUP"
SETUP = "SETUP"
COMMITTING = "COMMITTING"
FAIL_WAIT = "FAIL_WAIT"
FAIL_ABORT = "FAIL_ABORT"
KILL_WAIT = "KILL_WAIT"
KILL_ABORT = "KILL_ABORT"
ERROR = "ERROR"
REBOOT = "REBOOT"

YarnApplicationState = (
    (ACCEPTED, "Application has been accepted by the scheduler."),
    (FAILED, "Application which failed."),
    (FINISHED, "Application which finished successfully."),
    (KILLED, "Application which was terminated by a user or admin."),
    (NEW, "Application which was just created."),
    (NEW_SAVING, "Application which is being saved."),
    (RUNNING, "Application which is currently running."),
    (SUBMITTED, "Application which has been submitted."),
)

ApplicationState = (
    (NEW, NEW),
    (INITING, INITING),
    (RUNNING, RUNNING),
    (FINISHING_CONTAINERS_WAIT, FINISHING_CONTAINERS_WAIT),
    (APPLICATION_RESOURCES_CLEANINGUP, APPLICATION_RESOURCES_CLEANINGUP),
    (FINISHED, FINISHED),
)

FinalApplicationStatus = (
    (FAILED, "Application which failed."),
    (KILLED, "Application which was terminated by a user or admin."),
    (SUCCEEDED, "Application which finished successfully."),
    (UNDEFINED, "Undefined state when either the application has not yet finished."),
)

JobStateInternal = (
    (NEW, NEW),
    (SETUP, SETUP),
    (INITED, INITED),
    (RUNNING, RUNNING),
    (COMMITTING, COMMITTING),
    (SUCCEEDED, SUCCEEDED),
    (FAIL_WAIT, FAIL_WAIT),
    (FAIL_ABORT, FAIL_ABORT),
    (FAILED, FAILED),
    (KILL_WAIT, KILL_WAIT),
    (KILL_ABORT, KILL_ABORT),
    (KILLED, KILLED),
    (ERROR, ERROR),
    (REBOOT, REBOOT),
)
