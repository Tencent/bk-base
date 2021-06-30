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
"""
model模块model
"""

from sqlalchemy import TIMESTAMP, Column, String, Text, text
from sqlalchemy.dialects.mysql import BIGINT, INTEGER, MEDIUMTEXT, TINYINT
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata
Base._db_name_ = 'bkdata_modeling'


class ActionInfo(Base):
    __tablename__ = 'action_info'

    action_name = Column(String(64), primary_key=True)
    action_alias = Column(String(64), nullable=False)
    description = Column(Text)
    status = Column(String(32), nullable=False, server_default=text("'developing'"))
    input_config = Column(Text)
    output_config = Column(Text)
    logic_config = Column(Text, nullable=False)
    target_id = Column(String(64))
    target_type = Column(String(64), nullable=False)
    generate_type = Column(String(32), nullable=False, server_default=text("'system'"))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class ActionInputArg(Base):
    __tablename__ = 'action_input_arg'

    id = Column(INTEGER(11), primary_key=True)
    arg_name = Column(String(64), nullable=False)
    action_name = Column(String(64), nullable=False)
    arg_alias = Column(String(64), nullable=False)
    arg_index = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    data_type = Column(String(32), nullable=False)
    properties = Column(Text)
    description = Column(Text)
    default_value = Column(Text)
    advance_config = Column(Text)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class ActionOutputField(Base):
    __tablename__ = 'action_output_field'

    id = Column(INTEGER(11), primary_key=True)
    action_name = Column(String(64), nullable=False)
    field_name = Column(String(64), nullable=False)
    field_alias = Column(String(64), nullable=False)
    field_index = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    data_type = Column(String(32), nullable=False)
    properties = Column(Text)
    description = Column(Text)
    default_value = Column(Text)
    is_dimension = Column(TINYINT(4), nullable=False, server_default=text("'0'"))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class ActionType(Base):
    __tablename__ = 'action_type'

    action_type = Column(String(64), primary_key=True)
    action_type_alias = Column(String(64), nullable=False)
    action_type_index = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    parent_action_type = Column(String(64), nullable=False)
    icon = Column(String(255))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class ActionVisual(Base):
    __tablename__ = 'action_visual'

    action_name = Column(String(64), nullable=False)
    id = Column(INTEGER(11), primary_key=True)
    visual_name = Column(String(64), nullable=False)
    visual_index = Column(INTEGER(11))
    brief_info = Column(String(255))
    scene_name = Column(String(64), nullable=False)
    description = Column(Text)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class ActionVisualComponent(Base):
    __tablename__ = 'action_visual_component'

    component_name = Column(String(64), primary_key=True)
    component_alias = Column(String(64), nullable=False)
    component_type = Column(String(45))
    description = Column(MEDIUMTEXT)
    remark = Column(Text)
    properties = Column(Text)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class ActionVisualComponentRelation(Base):
    __tablename__ = 'action_visual_component_relation'

    id = Column(INTEGER(11), primary_key=True)
    action_visual_id = Column(INTEGER(11), nullable=False)
    component_name = Column(String(64), nullable=False)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class Algorithm(Base):
    __tablename__ = 'algorithm'

    algorithm_name = Column(String(64), primary_key=True)
    algorithm_alias = Column(String(64), nullable=False)
    description = Column(Text)
    algorithm_original_name = Column(String(128), nullable=False)
    algorithm_type = Column(String(32))
    category_type = Column(String(32))
    generate_type = Column(String(32), nullable=False, server_default=text("'\"system\"'"), comment='生成类型')
    sensitivity = Column(String(32), nullable=False, server_default=text("'\"private\"'"), comment='敏感度')
    project_id = Column(INTEGER(11), comment='关联的项目id')
    run_env = Column(String(64), comment='执行环境')
    framework = Column(String(64), comment='支持框架')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
    protocol_version = Column(String(16), nullable=False, server_default=text("'1.0'"), comment='应用协议版本')
    scene_name = Column(String(255), nullable=False, server_default=text("'custom'"), comment='数据类型')


class AlgorithmType(Base):
    __tablename__ = 'algorithm_type'

    algorithm_type = Column(String(64), primary_key=True)
    algorithm_type_alias = Column(String(45), nullable=False)
    algorithm_type_index = Column(INTEGER(11), nullable=False)
    parent_algorithm_type = Column(String(45))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class Feature(Base):
    __tablename__ = 'feature'

    feature_name = Column(String(255), primary_key=True)
    feature_alias = Column(String(255), nullable=False)
    feature_type = Column(String(64), nullable=False)
    description = Column(String(255))
    class_name = Column(String(255))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class FeatureTemplate(Base):
    __tablename__ = 'feature_template'

    id = Column(INTEGER(11), primary_key=True)
    template_name = Column(String(64), nullable=False)
    template_alias = Column(String(255), nullable=False)
    status = Column(String(32), nullable=False)
    scene_name = Column(String(64), nullable=False, server_default=text("'developing'"))
    properties = Column(Text)
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class FeatureTemplateNode(Base):
    __tablename__ = 'feature_template_node'

    id = Column(INTEGER(11), primary_key=True)
    node_name = Column(String(255), nullable=False)
    node_alias = Column(String(255), nullable=False)
    node_index = Column(INTEGER(11), nullable=False)
    default_node_config = Column(MEDIUMTEXT)
    properties = Column(Text)
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"))
    step_name = Column(String(64), nullable=False)
    action_name = Column(String(64), nullable=False)
    feature_template_id = Column(INTEGER(11), nullable=False)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class FeatureTransformInfo(Base):
    __tablename__ = 'feature_transform_info'

    id = Column(INTEGER(11), primary_key=True)
    sample_set_id = Column(INTEGER(11), nullable=False)
    description = Column(String(255))
    status = Column(String(32))
    scene_name = Column(String(64), nullable=False)
    active = Column(TINYINT(4))
    properties = Column(Text)
    run_status = Column(
        String(32),
        nullable=False,
        server_default=text("'not_run'"),
        comment='not_run / pending / running /  success / failed',
    )
    feature_template_id = Column(INTEGER(11), nullable=False)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class FeatureTransformInfoStage(Base):
    __tablename__ = 'feature_transform_info_stage'

    id = Column(INTEGER(11), primary_key=True)
    sample_set_id = Column(INTEGER(11), nullable=False)
    description = Column(String(255))
    status = Column(String(32))
    scene_name = Column(String(64), nullable=False)
    active = Column(TINYINT(4))
    properties = Column(Text)
    run_status = Column(
        String(32),
        nullable=False,
        server_default=text("'not_run'"),
        comment='not_run / pending / running /  success / failed',
    )
    feature_template_id = Column(INTEGER(11), nullable=False)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class FeatureTransformNode(Base):
    __tablename__ = 'feature_transform_node'

    id = Column(String(128), primary_key=True)
    feature_info_id = Column(INTEGER(11), nullable=False)
    action_name = Column(String(64), nullable=False)
    input_config = Column(Text)
    output_config = Column(Text)
    execute_config = Column(Text)
    node_index = Column(INTEGER(11), nullable=False)
    run_status = Column(String(32), nullable=False, comment='not_run / pending / running /  success / failed')
    operate_status = Column(String(32), nullable=False, comment=' init / updated / finished')
    step_name = Column(String(64), nullable=False)
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"))
    properties = Column(Text)
    node_config = Column(Text)
    select = Column(TINYINT(4), nullable=False, server_default=text("'1'"), comment='是否被选中')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
    status = Column(String(32), nullable=False, server_default=text("'init'"))


class FeatureType(Base):
    __tablename__ = 'feature_type'

    feature_type_name = Column(String(64), primary_key=True)
    feature_type_alias = Column(String(255))
    type_index = Column(INTEGER(11))
    parent_feature_type_name = Column(String(64))
    description = Column(String(255))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class ModelExperiment(Base):
    __tablename__ = 'model_experiment'

    experiment_id = Column(INTEGER(11), primary_key=True)
    experiment_name = Column(String(64), nullable=False)
    experiment_alias = Column(String(64), nullable=False)
    description = Column(Text)
    project_id = Column(INTEGER(11), nullable=False)
    status = Column(
        String(32), nullable=False, server_default=text("'developing'"), comment='developing/finished/release'
    )
    pass_type = Column(
        String(32), nullable=False, server_default=text("'not_confirmed'"), comment='not_confirmed/failed/pass'
    )
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"))
    experiment_config = Column(MEDIUMTEXT)
    model_file_config = Column(Text)
    execute_config = Column(Text)
    properties = Column(Text)
    model_id = Column(String(64), nullable=False)
    template_id = Column(INTEGER(11), nullable=False)
    experiment_index = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
    sample_latest_time = Column(TIMESTAMP)
    protocol_version = Column(String(16), nullable=False, server_default=text("'1.0'"), comment='应用协议版本')
    experiment_training = Column(TINYINT(4), nullable=False, server_default=text("'0'"), comment='是否在实验中训练/评估')
    continuous_training = Column(TINYINT(4), nullable=False, server_default=text("'0'"), comment='是否在发布后持续训练')


class ModelExperimentNode(Base):
    __tablename__ = 'model_experiment_node'

    node_id = Column(String(128), primary_key=True)
    model_id = Column(String(128), nullable=False)
    model_experiment_id = Column(INTEGER(11), nullable=False)
    node_name = Column(String(64), nullable=False)
    node_alias = Column(String(64), nullable=False)
    node_config = Column(MEDIUMTEXT)
    properties = Column(Text)
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"))
    step_name = Column(String(64), nullable=False)
    node_index = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    action_name = Column(String(64), nullable=False)
    node_role = Column(String(32), nullable=False)
    input_config = Column(Text)
    output_config = Column(Text)
    execute_config = Column(Text)
    algorithm_config = Column(MEDIUMTEXT)
    run_status = Column(
        String(32),
        nullable=False,
        server_default=text("'not_run'"),
        comment='not_run / pending / running /  success / failed',
    )
    operate_status = Column(
        String(32), nullable=False, server_default=text("'init'"), comment='init / updated / finished'
    )
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class ModelExperimentNodeUpstream(Base):
    __tablename__ = 'model_experiment_node_upstream'

    id = Column(INTEGER(11), primary_key=True)
    properties = Column(Text)
    complete_type = Column(String(32), nullable=False, server_default=text("'success'"))
    concurrent_type = Column(String(32), nullable=False, server_default=text("'sequence'"))
    execute_index = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    node_id = Column(String(128), nullable=False)
    model_id = Column(String(128), nullable=False)
    parent_node_id = Column(String(128), nullable=False)
    model_experiment_id = Column(INTEGER(11), nullable=False)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class ModelInfo(Base):
    __tablename__ = 'model_info'

    model_id = Column(String(128), primary_key=True)
    model_name = Column(String(64), nullable=False)
    model_alias = Column(String(64), nullable=False)
    description = Column(Text)
    project_id = Column(INTEGER(11), nullable=False)
    model_type = Column(
        String(64), nullable=False, server_default=text("'process'"), comment='process / modelflow / 还有复合模型等等'
    )
    sensitivity = Column(String(32), nullable=False, server_default=text("'private'"))
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"))
    properties = Column(Text)
    scene_name = Column(String(64), nullable=False)
    status = Column(String(32), nullable=False, server_default=text("'developing'"))
    train_mode = Column(String(32), nullable=False, server_default=text("'supervised'"))
    run_env = Column(String(32))
    sample_set_id = Column(INTEGER(11))
    input_standard_config = Column(MEDIUMTEXT, comment='输入的标准')
    output_standard_config = Column(MEDIUMTEXT, comment='输出的标准')
    execute_standard_config = Column(MEDIUMTEXT, comment='执行配置的标准')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
    protocol_version = Column(String(16), nullable=False, server_default=text("'1.0'"), comment='应用协议版本')
    sample_type = Column(String(64), nullable=False)
    modeling_type = Column(String(64), nullable=False)


class ModelInstance(Base):
    __tablename__ = 'model_instance'

    id = Column(INTEGER(11), primary_key=True)
    model_release_id = Column(INTEGER(11), nullable=False)
    model_id = Column(String(128), nullable=False)
    model_experiment_id = Column(INTEGER(11), nullable=True)
    project_id = Column(INTEGER(11), nullable=False)
    flow_id = Column(INTEGER(11))
    flow_node_id = Column(INTEGER(11))
    instance_config = Column(MEDIUMTEXT, nullable=False)
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"))
    upgrade_config = Column(Text)
    sample_feedback_config = Column(Text)
    serving_mode = Column(String(32), nullable=False, server_default=text("'realtime'"))
    instance_status = Column(String(32))
    execute_config = Column(Text)
    data_processing_id = Column(String(255), nullable=False)
    input_result_table_ids = Column(Text)
    output_result_table_ids = Column(Text)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
    protocol_version = Column(String(16), nullable=False, server_default=text("'1.0'"), comment='应用协议版本')
    basic_model_id = Column(String(128), comment='模型实例id')


class ModelRelease(Base):
    __tablename__ = 'model_release'

    id = Column(INTEGER(11), primary_key=True)
    model_experiment_id = Column(INTEGER(11), nullable=True)
    model_id = Column(String(128), nullable=False)
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"))
    description = Column(Text)
    publish_status = Column(String(32), nullable=False, server_default=text("'latest'"))
    model_config_template = Column(MEDIUMTEXT)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
    version_index = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    protocol_version = Column(String(16), nullable=False, server_default=text("'1.0'"), comment='应用协议版本')
    basic_model_id = Column(String(128), comment='模型实例id')


class ModelTemplate(Base):
    __tablename__ = 'model_template'

    id = Column(INTEGER(11), primary_key=True)
    template_name = Column(String(64), nullable=False)
    description = Column(Text)
    properties = Column(Text)
    status = Column(String(32), nullable=False, server_default=text("'developing'"))
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"))
    scene_name = Column(String(64), nullable=False)
    template_alias = Column(String(64), nullable=False)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class ModelTemplateNode(Base):
    __tablename__ = 'model_template_node'

    id = Column(INTEGER(11), primary_key=True)
    node_name = Column(String(64), nullable=False, server_default=text("''"))
    node_alias = Column(String(64), nullable=False, server_default=text("''"))
    node_index = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    default_node_config = Column(MEDIUMTEXT)
    properties = Column(Text)
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"))
    template_id = Column(INTEGER(11), nullable=False)
    step_name = Column(String(64), nullable=False)
    action_name = Column(String(64), nullable=False)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class ModelTemplateNodeUpstream(Base):
    __tablename__ = 'model_template_node_upstream'

    id = Column(INTEGER(11), primary_key=True)
    node_id = Column(INTEGER(11), nullable=False)
    parent_node_id = Column(INTEGER(11), nullable=False)
    properties = Column(Text)
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"))
    complete_type = Column(String(32), nullable=False, server_default=text("'success'"))
    concurrent_type = Column(String(32), nullable=False, server_default=text("'sequence'"))
    execute_index = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class ModelingStep(Base):
    __tablename__ = 'modeling_step'

    step_name = Column(String(64), primary_key=True)
    step_alias = Column(String(64), nullable=False)
    description = Column(Text)
    brief_info = Column(String(255))
    step_type = Column(String(32))
    step_level = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    step_index = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    step_icon = Column(String(255))
    properties = Column(Text)
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"))
    parent_step_name = Column(String(64), nullable=False)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class ModelingTask(Base):
    __tablename__ = 'modeling_task'

    id = Column(INTEGER(11), primary_key=True)
    task_type = Column(String(32), server_default=text("'serving'"))
    task_mode = Column(String(32), server_default=text("'realtime'"))
    executor_type = Column(String(32), server_default=text("'local / jobnavi'"))
    executor_config = Column(Text, comment='local / jobnavi config')
    owner_id = Column(INTEGER(11), nullable=False, comment='外键bkdata_modeling.model_instance.id')
    status = Column(String(32), nullable=False, server_default=text("'no-start'"))
    task_config = Column(Text, nullable=False)
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"))
    schedule_type = Column(String(32))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class ModelingTaskExecuteLog(Base):
    __tablename__ = 'modeling_task_execute_log'

    id = Column(INTEGER(11), primary_key=True)
    modeling_task_id = Column(INTEGER(11), nullable=False)
    execute_id = Column(INTEGER(11))
    status = Column(String(32))
    task_result = Column(String(32), nullable=False)
    start_time = Column(TIMESTAMP)
    end_time = Column(TIMESTAMP)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class SampleFeatures(Base):
    __tablename__ = 'sample_features'

    id = Column(String(255), primary_key=True)
    sample_set_id = Column(INTEGER(11), nullable=False)
    field_name = Column(String(255), nullable=False)
    field_alias = Column(String(255), nullable=False)
    description = Column(String(255), nullable=False)
    field_index = Column(INTEGER(11), nullable=False)
    is_dimension = Column(TINYINT(4), nullable=False, server_default=text("'0'"))
    field_type = Column(String(32), nullable=False)
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"))
    origin = Column(Text, comment='从哪些原始的样本集字段计算出来的。列表 []')
    attr_type = Column(String(32), nullable=False, comment='字段的属性类型 feature / metric / label / ts_field')
    properties = Column(Text)
    generate_type = Column(String(32), nullable=False, comment='原生 还是 生成的, origin / generate')
    field_evaluation = Column(Text, comment='字段评估结果,如特征重要性 importance 和 质量评分 quality')
    parents = Column(Text, comment='父字段')
    feature_transform_node_id = Column(String(128), comment='特征来自于哪个特征准备的节点')
    require_type = Column(String(64), nullable=False, comment='required / not_required / inner_used')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
    status = Column(String(50), nullable=False, comment='状态')


class SampleResultTable(Base):
    __tablename__ = 'sample_result_table'

    id = Column(String(255), primary_key=True)
    result_table_id = Column(String(255), nullable=False)
    sample_set_id = Column(INTEGER(11), nullable=False)
    fields_mapping = Column(Text, comment='字段映射信息,json结构')
    bk_biz_id = Column(INTEGER(11), nullable=False, comment='样本集业务')
    properties = Column(Text)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
    status = Column(String(50), nullable=False, comment='状态')
    curve_type = Column(String(32), nullable=False, server_default=text("'single'"))
    group_config = Column(Text, nullable=True)
    ts_freq = Column(INTEGER(11), nullable=False, server_default=text("'0'"))
    protocol_version = Column(String(32), nullable=False, comment='版本', server_default=text("'1.3.2'"))


class SampleSet(Base):
    __tablename__ = 'sample_set'

    id = Column(INTEGER(11), primary_key=True)
    sample_set_name = Column(String(255), nullable=False)
    sample_set_alias = Column(String(255), nullable=False)
    sensitivity = Column(String(32), nullable=False, server_default=text("'private'"))
    project_id = Column(INTEGER(11), nullable=False)
    scene_name = Column(String(64), nullable=False)
    sample_latest_time = Column(TIMESTAMP)
    sample_feedback = Column(TINYINT(4), nullable=False)
    sample_feedback_config = Column(Text, comment='样本反馈配置')
    properties = Column(Text)
    sample_size = Column(BIGINT(20))
    description = Column(String(255), nullable=True)
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"))
    sample_type = Column(String(45), nullable=False, server_default=text("'timeseries'"), comment='样本集的类型,暂时是 时序类型的')
    modeling_type = Column(String(45), nullable=False, server_default=text("'aiops'"), comment='模型类型')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
    commit_status = Column(String(32), nullable=False, server_default=text("'init'"))
    processing_cluster_id = Column(INTEGER(11), nullable=True)
    storage_cluster_id = Column(INTEGER(11), nullable=True)


class SampleSetStoreDataset(Base):
    __tablename__ = 'sample_set_store_dataset'

    id = Column(INTEGER(11), primary_key=True)
    sample_set_id = Column(INTEGER(11), nullable=False)
    data_set_id = Column(String(256), nullable=False)
    data_set_type = Column(String(256), nullable=False)
    role = Column(String(256), nullable=False)
    created_by = Column(String(128), nullable=False)
    updated_by = Column(String(128), nullable=False)
    created_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='创建时间'
    )
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class SceneInfo(Base):
    __tablename__ = 'scene_info'

    scene_name = Column(String(64), primary_key=True)
    scene_alias = Column(String(64), nullable=False, unique=True)
    description = Column(Text)
    scene_index = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    scene_type = Column(String(32), nullable=False)
    scene_level = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    status = Column(String(32), nullable=False, server_default=text("'developing'"), comment='developing/finished')
    brief_info = Column(String(255), server_default=text("''"))
    properties = Column(Text)
    parent_scene_name = Column(String(64), nullable=False)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class SceneSampleOriginFeature(Base):
    __tablename__ = 'scene_sample_origin_features'

    id = Column(INTEGER(11), primary_key=True)
    scene_name = Column(String(64), nullable=False)
    field_name = Column(String(255), nullable=False)
    field_alias = Column(String(255), nullable=False)
    description = Column(String(255))
    field_index = Column(INTEGER(11), nullable=False)
    is_dimension = Column(TINYINT(4), nullable=False)
    field_type = Column(String(64), nullable=False)
    active = Column(String(45), nullable=False)
    attr_type = Column(String(45), nullable=False)
    properties = Column(Text)
    require_type = Column(String(64), nullable=False, comment='required / not_required / inner_used')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class SceneStepAction(Base):
    __tablename__ = 'scene_step_action'

    id = Column(INTEGER(11), primary_key=True)
    action_name = Column(String(64), nullable=False)
    step_name = Column(String(64), nullable=False)
    scene_name = Column(String(64), nullable=False)
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class StorageModel(Base):
    __tablename__ = 'storage_model'

    id = Column(INTEGER(11), primary_key=True)
    physical_table_name = Column(String(64), nullable=False)
    expires = Column(String(32))
    storage_config = Column(Text, nullable=False)
    storage_cluster_config_id = Column(INTEGER(11), nullable=False)
    storage_cluster_name = Column(String(32), nullable=False)
    storage_cluster_type = Column(String(32), nullable=False)
    active = Column(TINYINT(4))
    priority = Column(INTEGER(11), nullable=False, server_default=text("'0'"), comment='优先级, 优先级越高, 先使用这个存储')
    generate_type = Column(
        String(32), nullable=False, server_default=text("'system'"), comment='用户还是系统产生的user / system'
    )
    content_type = Column(String(32), server_default=text("'model'"))
    model_id = Column(String(128), nullable=False)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class TimeseriesSampleConfig(Base):
    __tablename__ = 'timeseries_sample_config'

    id = Column(INTEGER(11), primary_key=True)
    sample_set_id = Column(INTEGER(11), nullable=False)
    ts_depend = Column(String(32), nullable=False, server_default=text("'0d'"), comment='历史依赖')
    ts_freq = Column(String(32), nullable=False, server_default=text("'0'"), comment='统计频率')
    ts_depend_config = Column(Text)
    ts_freq_config = Column(Text)
    description = Column(String(255), comment='描述')
    properties = Column(Text)
    ts_config_status = Column(
        String(32), nullable=False, server_default=text("'not_confirmed'"), comment='not_confirmed / confirmed'
    )
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
    start_time = Column(TIMESTAMP)
    end_time = Column(TIMESTAMP)


class TimeseriesSampleFragments(Base):
    __tablename__ = 'timeseries_sample_fragments'

    id = Column(String(255), primary_key=True)
    start_time = Column(TIMESTAMP)
    end_time = Column(TIMESTAMP)
    generate_type = Column(
        String(32), nullable=False, server_default=text("'manual'"), comment='来源manual还是sample_feedback'
    )
    labels = Column(
        Text,
        comment='这个片段的labels信息, {“start_time”:xx, “end_time”: xxx, '
        '"anomaly_label": 1, ”remark": "备注”,  "anomaly_label_type": “manual”}',
    )
    status = Column(String(32), nullable=False, server_default=text("'developing'"))
    labels_type = Column(
        String(32), nullable=False, server_default=text("'timeseries'"), comment='Label的类别,默认是teimseries'
    )
    result_table_id = Column(String(255), nullable=False)
    sample_set_id = Column(INTEGER(11), nullable=False)
    line_id = Column(String(255), nullable=False)
    group_fields = Column(Text)
    group_dimension = Column(Text)
    properties = Column(Text)
    sample_size = Column(BIGINT(20))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class TimeseriesSampleLabel(Base):
    __tablename__ = 'timeseries_sample_label'

    id = Column(String(255), primary_key=True)
    start = Column(TIMESTAMP)
    end = Column(TIMESTAMP)
    remark = Column(Text)
    result_table_id = Column(String(255), nullable=False)
    sample_set_id = Column(INTEGER(11), nullable=False)
    line_id = Column(String(255), nullable=False)
    status = Column(String(32), nullable=False, server_default=text("'finished'"))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
    anomaly_label = Column(String(64), nullable=False)
    anomaly_label_type = Column(String(64), nullable=False)
    label_size = Column(BIGINT(20), nullable=False)


class UserOperationLog(Base):
    __tablename__ = 'user_operation_log'

    id = Column(INTEGER(11), primary_key=True)
    operation = Column(String(64), nullable=False)
    module = Column(String(64), nullable=False)
    operator = Column(String(64), nullable=False)
    description = Column(String(255), nullable=False)
    operation_result = Column(String(64), nullable=False)
    errors = Column(Text)
    extra = Column(Text)
    object = Column(String(255), nullable=False, comment='操作的对象')
    object_id = Column(String(255), nullable=False, comment='对象的id')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class FeatureTemplateNodeUpstream(Base):
    __tablename__ = 'feature_template_node_upstream'

    id = Column(INTEGER(11), primary_key=True)
    node_id = Column(INTEGER(11), nullable=False)
    parent_node_id = Column(INTEGER(11))
    properties = Column(Text)
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"))
    complete_type = Column(String(32), nullable=False, server_default=text("'success'"))
    concurrent_type = Column(String(32), nullable=False, server_default=text("'sequence'"))
    execute_index = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


# 2020.09.04 新增
class AlgorithmDemo(Base):
    __tablename__ = 'algorithm_demo'

    algorithm_demo_name = Column(String(64), primary_key=True, comment='算法demo名称')
    algorithm_demo_alias = Column(String(64), nullable=False, comment='算法demo别名')
    description = Column(Text, comment='描述')
    algorithm_type = Column(String(32), comment='算法类别')
    logic = Column(MEDIUMTEXT, comment='代码逻辑')
    config = Column(MEDIUMTEXT, nullable=False, comment='配置')
    execute_config = Column(MEDIUMTEXT, nullable=False, comment='执行时独特的配置')
    properties = Column(MEDIUMTEXT, nullable=False, comment='属性')
    demo_type = Column(String(64), nullable=False, comment='算法示例类型')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
        comment='算法demo',
    )


class AlgorithmVersion(Base):
    __tablename__ = 'algorithm_version'

    id = Column(INTEGER(11), primary_key=True)
    algorithm_name = Column(String(64), nullable=False, comment='算法名字')
    version = Column(INTEGER(11), nullable=False, server_default=text("'1'"), comment='版本')
    logic = Column(MEDIUMTEXT, comment='代码逻辑')
    config = Column(MEDIUMTEXT, nullable=False, comment='配置')
    execute_config = Column(MEDIUMTEXT, nullable=False, comment='执行时独特的配置')
    properties = Column(MEDIUMTEXT, nullable=False, comment='属性')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
    description = Column(Text, comment='描述')
    status = Column(String(64), nullable=False, server_default=text("'developing'"), comment='算法状态')


class BasicModel(Base):
    __tablename__ = 'basic_model'

    basic_model_id = Column(String(128), primary_key=True)
    basic_model_name = Column(String(64), nullable=False, comment='名字')
    basic_model_alias = Column(String(64), nullable=False, comment='别名')
    algorithm_name = Column(String(64), nullable=False, comment='算法名称')
    algorithm_version = Column(INTEGER(11), nullable=False, comment='算法版本')
    description = Column(Text, comment='描述')
    status = Column(
        String(32), nullable=False, server_default=text("'developing'"), comment='developing/finished/release'
    )
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"), comment='是否启用')
    config = Column(MEDIUMTEXT, comment='配置')
    execute_config = Column(MEDIUMTEXT, comment='执行时独特的配置')
    properties = Column(MEDIUMTEXT, comment='属性')
    experiment_id = Column(INTEGER(11), nullable=True, comment='实验id')
    experiment_instance_id = Column(INTEGER(11), nullable=True, comment='实验实例id')
    node_id = Column(String(128), nullable=False, comment='节点id')
    model_id = Column(String(128), nullable=False, comment='模型id')
    source = Column(String(64), nullable=False, comment='来源。上传/实验生成')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
    sample_latest_time = Column(TIMESTAMP, comment='备选模型')


class BasicModelGenerator(Base):
    __tablename__ = 'basic_model_generator'

    generator_name = Column(String(64), primary_key=True, comment='生成器名字')
    generator_alias = Column(String(64), comment='生成器别名')
    generator_type = Column(String(32), comment='生成器类型')
    config = Column(MEDIUMTEXT, nullable=False, comment='配置')
    execute_config = Column(MEDIUMTEXT, nullable=False, comment='执行时独特的配置')
    properties = Column(MEDIUMTEXT, nullable=False, comment='属性')
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"), comment='是否启用')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
    description = Column(Text, comment='备选模型生成器')


class CommonEnum(Base):
    __tablename__ = 'common_enum'

    id = Column(INTEGER(11), primary_key=True)
    enum_name = Column(String(64), primary_key=True, comment='枚举名称')
    enum_alias = Column(String(64), nullable=False, comment='枚举别名')
    enum_category = Column(String(64), comment='枚举分类')
    properties = Column(MEDIUMTEXT, nullable=False, comment='属性')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
    description = Column(Text, comment='算法demo')


class ExperimentInstance(Base):
    __tablename__ = 'experiment_instance'

    id = Column(INTEGER(11), primary_key=True)
    experiment_id = Column(INTEGER(11), nullable=False, comment='实验id')
    model_id = Column(String(128), nullable=False, comment='模型id')
    generate_type = Column(String(32), nullable=False, server_default=text("'\"system\"'"), comment='生成类型')
    training_method = Column(String(32), nullable=False, server_default=text("'\"manual\"'"), comment='训练方式: 手动，自动，本地')
    config = Column(MEDIUMTEXT, nullable=False, comment='配置')
    execute_config = Column(MEDIUMTEXT, nullable=False, comment='执行时独特的配置')
    properties = Column(MEDIUMTEXT, nullable=False, comment='属性')
    status = Column(String(64), nullable=False, server_default=text("'developing'"), comment='状态')
    run_status = Column(String(64), nullable=False, server_default=text("'init'"), comment='执行状态')
    runtime_info = Column(Text, comment='运行信息')
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"), comment='是否启用')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
    description = Column(Text, comment='实验实例')
    protocol_version = Column(String(16), nullable=False, server_default=text("'1.3.2'"), comment='应用协议版本')


class ProcessingModel(Base):
    __tablename__ = 'processing_model'

    id = Column(INTEGER(11), primary_key=True)
    model_id = Column(String(128), nullable=False, comment='模型id')
    processing_config = Column(Text, nullable=False, comment='计算集群配置')
    processing_cluster_config_id = Column(INTEGER(11), nullable=False, comment='计算集群')
    active = Column(TINYINT(4), comment='是否启用')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
    description = Column(Text, comment='模型关联的计算资源')


class TrainingStrategy(Base):
    __tablename__ = 'training_strategy'

    id = Column(INTEGER(11), primary_key=True)
    experiment_id = Column(INTEGER(11), nullable=False, comment='实验id')
    node_id = Column(String(64))
    basic_model_generator_name = Column(String(64), nullable=False, comment='备选模型生成器id')
    config = Column(MEDIUMTEXT, nullable=False, comment='配置')
    execute_config = Column(MEDIUMTEXT, nullable=False, comment='执行时独特的配置')
    properties = Column(MEDIUMTEXT, nullable=False, comment='属性')
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"), comment='是否启用')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
    description = Column(Text, comment='训练策略')


class TrainingStrategyTemplate(Base):
    __tablename__ = 'training_strategy_template'

    training_strategy_template_name = Column(String(128), primary_key=True, comment='训练策略模板id')
    config = Column(MEDIUMTEXT, nullable=False, comment='配置')
    execute_config = Column(MEDIUMTEXT, nullable=False, comment='执行时独特的配置')
    properties = Column(MEDIUMTEXT, nullable=False, comment='属性')
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"), comment='是否启用')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
    description = Column(Text, comment='训练策略模板')


# ai_ops v1.2 added at 2020.12.30
class SampleCurve(Base):
    """样本曲线表"""

    __tablename__ = 'sample_curve'

    id = Column(String(255), primary_key=True)
    line_id = Column(String(128), nullable=False)
    line_alias = Column(String(255), nullable=False)
    sample_set_id = Column(INTEGER(11), nullable=False)
    sample_result_table_id = Column(String(255), nullable=False)
    result_table_id = Column(String(255), nullable=False)
    # 存放group_fields 对应的值，如 {'zone_id': 'QQ', 'area': 'china'}
    group_dimension = Column(Text, nullable=True)
    properties = Column(Text, nullable=True)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class SampleCollectConfig(Base):
    """样本修改配置表"""

    __tablename__ = 'sample_collect_config'

    id = Column(String(255), primary_key=True)
    # 是否自动添加样本
    auto_append = Column(INTEGER(11), nullable=False, default=0)
    # 是否自动删除样本
    auto_remove = Column(INTEGER(11), nullable=False, default=0)
    # 自动修改样本的相关配置
    config = Column(Text, nullable=True)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class SampleCollectRelation(Base):
    """样本修改配置关联表"""

    __tablename__ = 'sample_collect_relation'

    id = Column(String(255), primary_key=True)
    sample_set_id = Column(INTEGER(11), nullable=False)
    # 可以关联到RT层级或者曲线层级
    result_table_id = Column(String(255), nullable=True)
    line_id = Column(String(128), nullable=True)
    config_id = Column(String(255), nullable=False)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class LabelMappingConfig(Base):
    """标注映射策略表"""

    __tablename__ = 'label_mapping_config'

    id = Column(String(255), primary_key=True)
    # 映射类型 value/script
    mapping_type = Column(String(32), nullable=False)
    # 值映射规则
    config = Column(Text, nullable=True)
    # 映射脚本内容
    logic = Column(Text, nullable=True)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class LabelMappingRelation(Base):
    """标注映射关联表"""

    __tablename__ = 'label_mapping_relation'

    id = Column(String(255), primary_key=True)
    sample_set_id = Column(INTEGER(11), nullable=False)
    # 可以关联到RT层级或者曲线层级
    result_table_id = Column(INTEGER(11), nullable=True)
    line_id = Column(INTEGER(11), nullable=True)
    config_id = Column(String(255), nullable=False)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class SampleSetInstance(Base):
    """样本集执行配置实例"""

    __tablename__ = 'sample_set_instance'

    id = Column(INTEGER(11), primary_key=True)
    sample_set_id = Column(INTEGER(11), nullable=False)
    # pipeline 执行配置
    config = Column(Text, nullable=True)
    execute_config = Column(Text, nullable=True)
    properties = Column(Text, nullable=True)
    runtime_info = Column(Text, nullable=True)
    description = Column(String(255), nullable=True)
    status = Column(String(32), nullable=False, server_default=text("'standby'"))
    run_status = Column(String(32), nullable=False, server_default=text("'init'"))
    active = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class SampleSetTask(Base):
    """样本集执行任务"""

    __tablename__ = 'sample_set_task'

    id = Column(INTEGER(11), primary_key=True)
    sample_set_instance_id = Column(INTEGER(11), nullable=False)
    sample_set_id = Column(INTEGER(11), nullable=False)
    # 后台运行执行结果/状态，任务调度配置
    execute_config = Column(Text, nullable=True)
    task_config = Column(Text, nullable=True)
    task_result = Column(Text, nullable=True)
    status = Column(String(32), nullable=False, server_default=text("'standby'"))
    active = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class SampleSetNode(Base):
    """样本集任务节点"""

    __tablename__ = 'sample_set_node'

    # action_name + uuid[:8]
    id = Column(String(64), primary_key=True)
    sample_set_id = Column(INTEGER(11), nullable=False)
    action_name = Column(String(32), nullable=False)
    input_config = Column(Text, nullable=True)
    output_config = Column(Text, nullable=True)
    execute_config = Column(Text, nullable=True)
    node_index = Column(INTEGER(11), nullable=False)
    step_name = Column(String(32), nullable=False)
    properties = Column(Text, nullable=True)
    node_config = Column(Text, nullable=True)
    status = Column(String(32), nullable=False, server_default=text("'standby'"))
    active = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    select = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class SampleSetNodeUpstream(Base):
    """样本集任务节点上下游关系"""

    __tablename__ = 'sample_set_node_upstream'

    id = Column(String(255), primary_key=True)
    sample_set_id = Column(INTEGER(11), nullable=False)
    sample_set_node_id = Column(String(64), nullable=False)
    parent_sample_set_node_id = Column(String(64), nullable=True)
    properties = Column(Text, nullable=True)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
    status = Column(String(32), nullable=False, server_default=text("'standby'"))
    active = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    execute_index = Column(INTEGER(11), nullable=False, server_default=text("'0'"))


class SampleSetTemplate(Base):
    """样本集任务模板"""

    __tablename__ = 'sample_set_template'

    id = Column(INTEGER(11), primary_key=True)
    template_name = Column(String(64), nullable=False)
    template_alias = Column(String(64), nullable=False)
    status = Column(String(64), nullable=False)
    properties = Column(Text, nullable=True)
    active = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    scene_name = Column(String(64), nullable=False, server_default=text("'custom'"))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class SampleSetTemplateNode(Base):
    """样本集任务节点模版"""

    __tablename__ = 'sample_set_template_node'

    id = Column(String(64), primary_key=True)
    action_name = Column(String(32), nullable=False)
    input_config = Column(Text, nullable=True)
    output_config = Column(Text, nullable=True)
    execute_config = Column(Text, nullable=True)
    node_index = Column(INTEGER(11), nullable=False, server_default=text("'0'"))
    step_name = Column(String(32), nullable=False)
    properties = Column(Text, nullable=True)
    node_config = Column(Text, nullable=True)
    active = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    status = Column(String(32), nullable=False, server_default=text("'standby'"))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class SampleSetTemplateNodeUpstream(Base):
    """样本集任务节点上下游关系模版"""

    __tablename__ = 'sample_set_template_node_upstream'

    id = Column(INTEGER(11), primary_key=True)
    node_id = Column(String(64), nullable=False)
    parent_node_id = Column(String(64), nullable=True)
    properties = Column(Text, nullable=True)
    active = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    status = Column(String(32), nullable=False, server_default=text("'standby'"))
    execute_index = Column(INTEGER(11), nullable=False, server_default=text("'0'"))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


# AIOps v1.3
class AggregationMethod(Base):
    __tablename__ = 'aggregation_method'

    agg_name = Column(String(32), primary_key=True, comment='聚合名,主键')
    agg_name_alias = Column(String(64), comment='聚合中文名')
    properties = Column(Text)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class Visualization(Base):
    __tablename__ = 'visualization'

    visualization_name = Column(String(255), primary_key=True)
    visualization_alias = Column(String(255))
    description = Column(Text)
    properties = Column(Text)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class VisualizationComponent(Base):
    __tablename__ = 'visualization_component'

    component_name = Column(String(255), primary_key=True)
    component_alias = Column(String(255))
    component_type = Column(String(255))
    description = Column(Text)
    logic = Column(Text)
    logic_type = Column(String(64))
    config = Column(Text)
    properties = Column(Text)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class VisualizationComponentRelation(Base):
    __tablename__ = 'visualization_component_relation'

    id = Column(INTEGER(11), primary_key=True)
    component_name = Column(String(255))
    visualization_name = Column(String(255))
    description = Column(Text)
    properties = Column(Text)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class TargetVisualizationRelation(Base):
    __tablename__ = 'target_visualization_relation'

    id = Column(INTEGER(11), primary_key=True)
    target_name = Column(String(255))
    target_type = Column(String(255))
    visualization_name = Column(String(255))
    description = Column(Text)
    properties = Column(Text)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
