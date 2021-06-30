# coding=utf-8
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
import json
import os
import sys
import traceback

from django.conf import settings

from auth.management.handlers.do_migrate import do_migrate, load_data
from auth.models.iam_models import IamFileConfig
from auth.utils.base import compute_file_md5
from auth.bkiam import IAM_API_HOST, IAM_REGISTERED_APP_CODE, IAM_REGISTERED_APP_SECRET


class ActionSyncHandler:
    def exec_migrate(self, base_path, file_list):
        """
        :param base_path: /xx/xx/xxx/auth/bkiam/iam_migrations/
        :param file_list: ["1.json", "2.json", "3.json"]
        :return:
        """
        self.check_file(file_list)
        for file_name in file_list:
            sys.stdout.write(f"[exec_migrate]: {file_name} migrate file found \n")
            file_path = os.path.join(base_path, file_name)
            data = self.get_file_data(file_path)
            iam_file_config = IamFileConfig.objects.filter(file_name=file_name).first()
            if iam_file_config is None:
                try:
                    migrate_result = do_migrate(
                        data=data,
                        bk_iam_host=IAM_API_HOST,
                        app_code=IAM_REGISTERED_APP_CODE,
                        app_secret=IAM_REGISTERED_APP_SECRET,
                    )
                    if migrate_result:
                        sys.stdout.write(
                            "[exec_migrate] Synchronize the action for the first time, {} do_migrate is "
                            "success!\n".format(file_name)
                        )
                    else:
                        sys.stderr.write(
                            "[exec_migrate] Synchronize the action for the first time, {} do_migrate is "
                            "error! Reason: Synchronization Failure\n".format(file_name)
                        )
                        continue
                except Exception:
                    traceback.print_exc()
                    sys.stderr.write(
                        "[exec_migrate] Synchronize the action for the first time, {} do_migrate is "
                        "error! Reason: System Exception\n".format(file_name)
                    )
                    continue

                IamFileConfig.objects.create(
                    file_name=file_name, file_md5=compute_file_md5(file_path), file_content=json.dumps(data)
                )
            else:
                # calculate file MD5
                file_md5 = compute_file_md5(file_path)
                # If the file has changed, save the new MD5
                if iam_file_config.file_md5 != file_md5:
                    try:
                        migrate_result = do_migrate(
                            data=data,
                            bk_iam_host=IAM_API_HOST,
                            app_code=IAM_REGISTERED_APP_CODE,
                            app_secret=IAM_REGISTERED_APP_SECRET,
                        )
                        if migrate_result:
                            sys.stdout.write(f"{file_name} migrate is success !\n")
                        else:
                            sys.stderr.write(
                                "File modification Change Actions: {} do_migrate is error!"
                                "Reason：Synchronization Failure\n".format(file_name)
                            )
                            continue
                    except Exception:
                        traceback.print_exc()
                        sys.stderr.write(
                            "File modification Change Actions: {} do_migrate is error, reason：system "
                            "exception!\n".format(file_name)
                        )
                        continue
                    iam_file_config.file_md5 = file_md5
                    iam_file_config.file_content = json.dumps(data)
                    iam_file_config.save()

    def check_file(self, file_list):
        # check the json file,deletes files that do not exist in the folder in the database
        sys.stdout.write("checking file !\n")
        file_config_list = IamFileConfig.objects.all()
        for file_config in file_config_list:
            if file_config.file_name not in file_list:
                file_config.delete()

    def get_file_data(self, file_path):
        """
        获取配置文件内容
        """
        file_data = load_data(file_path)
        auth_service = settings.AUTH_API_URL

        # 设置钩子函数，需要重载 provider_config.host 的地址
        if "operations" in file_data:
            for operation in file_data["operations"]:
                if operation["operation"] == "upsert_system":
                    operation["data"]["provider_config"]["host"] = auth_service

        return file_data

    def sync_actions(self):
        file_path, file_list = self.get_file_path()
        return self.exec_migrate(file_path, file_list)

    def get_file_path(self):
        file_path = os.path.join(settings.APP_DIR, "config/iam_migrations/")
        if not os.path.exists(file_path):
            sys.stderr.write(f"{file_path} is not exist.\n")
            exit(1)
        file_list = os.listdir(file_path)
        return file_path, sorted(file_list)
