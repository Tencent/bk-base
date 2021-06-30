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

import collections
import json
import uuid

from common.local import get_request_username

from dataflow.shared.handlers import processing_job_info
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import uc_logger as logger
from dataflow.shared.meta.tag.tag_helper import TagHelper
from dataflow.uc.adapter.operation_client import get_job_operation_adapter
from dataflow.uc.exceptions.comp_exceptions import IllegalArgumentException
from dataflow.uc.graph.unified_computing_graph_generator import UnifiedComputingGraphGenerator
from dataflow.uc.handlers import unified_computing_graph_info, unified_computing_graph_job
from dataflow.uc.settings import GET_OPERATE_RESULT_HTTP_URL, OPERATE_JOB_HTTP_URL, GraphStatus, UnifiedComputingJobType


class GraphController(object):
    def __init__(self, graph_id):
        self.graph_id = graph_id
        self.__graph_info = None

    @property
    def graph_info(self):
        if not self.__graph_info:
            self.__graph_info = unified_computing_graph_info.get(self.graph_id)
        return self.__graph_info

    @classmethod
    def create_graph(cls, graph_conf):
        chaining = True
        if "chaining" in graph_conf:
            chaining = graph_conf["chaining"]

        job_graph = UnifiedComputingGraphGenerator(chaining).generate(graph_conf).get_job_graph()
        geog_area_code = TagHelper.get_geog_area_code_by_project(job_graph.project_id)
        graph_id = "g{}_{}".format(job_graph.project_id, str(uuid.uuid4()).replace("-", ""))

        # 记录处理过的任务
        processed_job_vertex_ids = set()
        # 记录 graph 生成的job 信息
        job_infos = collections.OrderedDict()
        for head_job_vertex_id in job_graph.head_vertex_ids:
            cls.__process_job_operation(job_graph, head_job_vertex_id, processed_job_vertex_ids, job_infos)
        job_info_list = []
        for job_info in list(job_infos.values()):
            job_info_list.append(job_info)
        # 在 graph info表中记录当前graph信息
        jobserver_config = {
            "geog_area_code": geog_area_code,
        }
        unified_computing_graph_info.save(
            graph_id=graph_id,
            jobserver_config=json.dumps(jobserver_config),
            graph_config=json.dumps(graph_conf),
            job_info_list=json.dumps(job_info_list),
            created_by=get_request_username(),
        )
        return graph_id

    def update_graph(self, graph_conf):
        """
        更新 graph，不允许增加新的 node 或者减少 node，只能更新node的逻辑

        :param graph_conf: graph conf
        :return: graph id
        """
        # 检测 graph 是否可以被更新
        existing_job_infos = self.__check_updated(graph_conf)
        chaining = True
        if "chaining" in graph_conf:
            chaining = graph_conf["chaining"]
        job_graph = UnifiedComputingGraphGenerator(chaining).generate(graph_conf).get_job_graph()

        # 记录处理过的任务
        processed_job_vertex_ids = set()
        # 记录 graph 生成的job 信息
        job_infos = collections.OrderedDict()
        for head_job_vertex_id in job_graph.head_vertex_ids:
            self.__update_job_operation(
                job_graph, head_job_vertex_id, processed_job_vertex_ids, job_infos, existing_job_infos
            )
        unified_computing_graph_info.update(
            self.graph_id, graph_config=json.dumps(graph_conf), updated_by=get_request_username()
        )
        return self.graph_id

    def delete_graph(self):
        """
        删除 graph，先调用基础API删除对应的 job，再删除 graph 表中的信息
        """
        logger.info("To delete the graph %s." % self.graph_id)
        job_info_list = json.loads(self.graph_info.job_info_list)
        for job_info in job_info_list:
            get_job_operation_adapter(job_info["job_type"]).delete_job(job_info["job_id"])
        # delete graph 信息
        unified_computing_graph_job.delete(graph_id=self.graph_id)
        unified_computing_graph_info.delete(graph_id=self.graph_id)

    def __update_job_operation(
        self,
        job_graph,
        current_job_vertex_id,
        processed_job_vertex_ids,
        job_infos,
        existing_job_infos,
        parent_job_vertex_id=None,
    ):
        if current_job_vertex_id not in processed_job_vertex_ids and self.__is_source_job_vertex_processed(
            job_graph, current_job_vertex_id, processed_job_vertex_ids
        ):
            job_vertex = job_graph.vertices[current_job_vertex_id]
            job_id = job_vertex.get_existing_job_id(existing_job_infos)
            # 调用基础 api 更新对应的job
            get_job_operation_adapter(job_vertex.job_type).update_job(job_id, job_vertex)
            job_infos[current_job_vertex_id] = {"job_id": job_id, "job_type": job_vertex.job_type, "children": []}
            if parent_job_vertex_id:
                job_infos[parent_job_vertex_id]["children"].append(job_id)
            processed_job_vertex_ids.add(current_job_vertex_id)
            for output_edge in job_vertex.outputs:
                target_vertex_id = output_edge.target_vertex_id
                self.__update_job_operation(
                    job_graph,
                    target_vertex_id,
                    processed_job_vertex_ids,
                    job_infos,
                    existing_job_infos,
                    parent_job_vertex_id=current_job_vertex_id,
                )

    def __check_updated(self, graph_conf):
        job_info_list = json.loads(self.graph_info.job_info_list)
        existing_node_ids = set()
        existing_job_infos = {}
        for job_info in job_info_list:
            if job_info["job_type"] in [
                UnifiedComputingJobType.SPARK_STRUCTURED_STREAMING_CODE.value,
                UnifiedComputingJobType.TENSORFLOW_CODE.value,
            ]:
                job_config = json.loads(processing_job_info.get(job_info["job_id"]).job_config)
                if "processings" in job_config and job_config["processings"]:
                    existing_node_ids.update(job_config["processings"])
                    existing_job_infos[job_info["job_id"]] = set(job_config["processings"])
            elif job_info["job_type"] == UnifiedComputingJobType.SPARK_CODE.value:
                # 如果是 spark code，processing id默认和job id一致
                existing_node_ids.update(job_info["job_id"])
                existing_job_infos[job_info["job_id"]] = {job_info["job_id"]}
            else:
                raise IllegalArgumentException("Not support component type %s" % job_info["job_type"])
        update_node_ids = set()
        for node_conf in graph_conf["nodes"]:
            update_node_ids.add(node_conf["processing_id"])
        if existing_node_ids != update_node_ids:
            raise IllegalArgumentException(
                "Update is not allowed, "
                "because the updated node is inconsistent with the existing node, "
                "the existing node has %s, and the node to be updated has %s."
                % (str(existing_node_ids), str(update_node_ids))
            )
        return existing_job_infos

    def start_graph(self):
        logger.info("To start the graph %s" % self.graph_id)
        job_info_list = json.loads(self.graph_info.job_info_list)
        geog_area_code = json.loads(self.graph_info.jobserver_config)["geog_area_code"]
        return self.__operate_graph(job_info_list, geog_area_code, "start")

    def stop_graph(self):
        logger.info("To stop the graph %s" % self.graph_id)
        # graph 自创建后包含拓扑信息不会改变，可以通过 graph info表来获取任务信息
        job_info_list = json.loads(self.graph_info.job_info_list)
        geog_area_code = json.loads(self.graph_info.jobserver_config)["geog_area_code"]
        return self.__operate_graph(job_info_list, geog_area_code, "stop")

    def __operate_graph(self, job_info_list, geog_area_code, operate):
        graph_workflow_config = {"params": {}, "config": {}, "node": {}}

        for job_info in job_info_list:
            sub_task_info = {
                "operate_job_url": "{}{}/".format(OPERATE_JOB_HTTP_URL, operate),
                "get_operate_result_url": GET_OPERATE_RESULT_HTTP_URL,
                "graph_id": self.graph_id,
                "job_id": job_info["job_id"],
                "operate": operate,
                "bk_username": get_request_username(),
            }
            graph_workflow_config["node"][job_info["job_id"]] = {
                "node_type": "subtask",
                "type_id": "ucapi-service",
                "children": job_info["children"],
                "subtask_info": json.dumps(sub_task_info),
            }
        jobnavi_args = {
            "schedule_id": self.graph_id,
            "description": "uc api start graph %s" % self.graph_id,
            "type_id": "workflow",
            "active": True,
            "exec_oncreate": True,
            "extra_info": json.dumps(graph_workflow_config),
        }
        logger.info("The graph {} start config is {}".format(self.graph_id, str(jobnavi_args)))
        cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
        jobnavi = JobNaviHelper(geog_area_code, cluster_id)
        jobnavi.delete_schedule(self.graph_id)
        execute_id = jobnavi.create_schedule_info(jobnavi_args)
        return {"operate": operate, "task_id": execute_id}

    def get_latest_deploy_data(self, operate, task_id):
        geog_area_code = json.loads(self.graph_info.jobserver_config)["geog_area_code"]
        if operate == "start":
            status = GraphStatus.RUNNING.value
        elif operate == "stop":
            status = GraphStatus.NO_START.value
        else:
            raise IllegalArgumentException("Not support the operate %s" % operate)
        cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
        jobnavi = JobNaviHelper(geog_area_code, cluster_id)
        data = jobnavi.get_execute_status(task_id)
        if data and data["status"] == "failed":
            self.__save_graph_job(self.graph_info, self.graph_id, GraphStatus.FAILURE.value)
            return {"status": "failed", "info": data["info"]}
        elif data and data["status"] == "finished":
            self.__save_graph_job(self.graph_info, self.graph_id, status)
            return {"status": "finished"}
        else:
            return {"status": "running"}

    @classmethod
    def __save_graph_job(cls, graph_info, graph_id, status):
        unified_computing_graph_job.save(
            graph_id=graph_id,
            jobserver_config=graph_info.jobserver_config,
            status=status,
            graph_config=graph_info.graph_config,
            job_info_list=graph_info.job_info_list,
            updated_by=get_request_username(),
        )

    @classmethod
    def __process_job_operation(
        cls, job_graph, current_job_vertex_id, processed_job_vertex_ids, job_infos, parent_job_vertex_id=None
    ):
        if current_job_vertex_id not in processed_job_vertex_ids and cls.__is_source_job_vertex_processed(
            job_graph, current_job_vertex_id, processed_job_vertex_ids
        ):
            job_vertex = job_graph.vertices[current_job_vertex_id]
            # 调用基础 api 生成对应的job
            job_id = get_job_operation_adapter(job_vertex.job_type).create_job(job_vertex)
            job_infos[current_job_vertex_id] = {"job_id": job_id, "job_type": job_vertex.job_type, "children": []}
            if parent_job_vertex_id:
                job_infos[parent_job_vertex_id]["children"].append(job_id)
            processed_job_vertex_ids.add(current_job_vertex_id)
            for output_edge in job_vertex.outputs:
                target_vertex_id = output_edge.target_vertex_id
                cls.__process_job_operation(
                    job_graph,
                    target_vertex_id,
                    processed_job_vertex_ids,
                    job_infos,
                    parent_job_vertex_id=current_job_vertex_id,
                )

    @classmethod
    def __is_source_job_vertex_processed(cls, job_graph, current_job_vertex_id, processed_job_vertex_ids):
        """
        判断当前 job vertex 是否可以进行处理，判断条件为当前节点的上游节点是否都被处理，如果都处理了，则可以处理当前job vertex

        :param job_graph:  job graph
        :param current_job_vertex_id: 当前 job vertex id
        :param processed_job_vertex_ids: 记录已经处理的 job vertex id
        :return: True: 当前 job vertex 可以进行处理; False: 当前 job vertex 不能进行处理
        """
        for input_edge in job_graph.vertices[current_job_vertex_id].inputs:
            if input_edge.source_vertex_id and input_edge.source_vertex_id not in processed_job_vertex_ids:
                return False
        return True
