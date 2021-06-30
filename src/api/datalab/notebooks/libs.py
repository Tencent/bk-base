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

from collections import OrderedDict

from django.utils.translation import ugettext_lazy as _


def get_lib_detail(description, version, doc):
    return {"description": description, "version": version, "doc": doc}


# python3支持的机器学习库
PY_LIBS = OrderedDict(
    [
        ("numpy", get_lib_detail(_("用于处理大型多维数组和矩阵"), "1.17.2", "https://numpy.org/")),
        ("pandas", get_lib_detail(_("提供高级数据结构和各种分析工具"), "0.25.3", "https://pandas.pydata.org/")),
        ("matplotlib", get_lib_detail(_("用于创建二维图表和图形的绘图库"), "3.1.1", "https://matplotlib.org/")),
        ("tensorflow", get_lib_detail(_("深度学习和机器学习框架"), "2.0.0", "https://www.tensorflow.org/")),
        (
            "tensorboard",
            get_lib_detail(_("tensorflow的可视化工具，可视化神经网络"), "2.0.1", "https://www.tensorflow.org/tensorboard"),
        ),
        ("torch", get_lib_detail(_("开源深度学习库"), "1.3.1", "https://pytorch.org/")),
        ("keras", get_lib_detail(_("用于处理神经网络的高级库"), "2.3.1", "https://keras.io/")),
        (
            "plotly",
            get_lib_detail(_("用于构建复杂的图形，例如：轮廓图形、三元图和3D图表"), "4.2.1", "https://plot.ly/python/getting-started/"),
        ),
        ("scikit-learn", get_lib_detail(_("为标准机器学习和数据挖掘任务提供常用算法"), "0.21.3", "https://scikit-learn.org/stable/")),
        ("xgboost", get_lib_detail(_("梯度提升算法库"), "0.90", "https://xgboost.readthedocs.io/en/latest/")),
        ("catboost", get_lib_detail(_("梯度提升算法库"), "0.18.1", "https://catboost.ai/docs/")),
        ("patsy", get_lib_detail(_("用于描述统计模型"), "0.5.1", "https://pypi.org/project/patsy/")),
        ("tsfresh", get_lib_detail(_("对时间序列进行特征提取的工具"), "0.12.0", "https://tsfresh.readthedocs.io/en/latest/")),
        ("statsmodels", get_lib_detail(_("用于统计数据分析的方法"), "0.10.1", "http://statsmodels.sourceforge.net/")),
        ("scipy", get_lib_detail(_("解决线性代数、概率论、积分计算等的工具"), "1.1.0", "https://www.scipy.org/")),
        ("seaborn", get_lib_detail(_("提供丰富的可视化图库"), "0.9.0", "http://seaborn.pydata.org/")),
        ("graphviz", get_lib_detail(_("用于绘制关系图、流程图"), "0.13.2", "https://www.graphviz.org/")),
        ("tblib", get_lib_detail(_("金融指数处理库"), "1.4.0", "https://pypi.org/project/tblib/")),
        ("kmodes", get_lib_detail(_("数据挖掘聚类算法"), "0.10.2", "https://github.com/nicodv/kmodes")),
        ("yellowbrick", get_lib_detail(_("机器学习可视化库"), "1.1", "https://www.scikit-yb.org/en/latest/")),
    ]
)
