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


def dfs(node, graph, visited, stack, circles):
    """
    DFS遍历图，并找到出环
    @param node: 节点
    @param graph: 图
    @param visited: 已访问的节点
    @param stack: 节点栈
    @param circles: 成环的列表
    @return:
    """
    visited[node] = True
    stack.append(node)
    if node in graph:
        for n in graph[node]:
            if n not in stack:
                if not visited[n]:
                    dfs(n, graph, visited, stack, circles)
            else:
                index = stack.index(n)
                circle = " ---> ".join(str(_node) for _node in stack[index:])
                circle = f"{circle} ---> {n}"
                circles.append(circle)
    stack.pop(-1)


def check_circle(check_list):
    """
    检测是否有环，列表中的每个元素是一个元组 (from, to)
    @param check_list: [(1,2), (2,3), (3,1), (3,4), (5,4), (5,6), (6,7), (7,5)]
    @return: True
    """
    graph = dict()
    visited = dict()
    stack = list()
    circles = list()
    for link in check_list:
        _from = link[0]
        _to = link[1]
        if _from not in graph:
            graph[_from] = [_to]
        elif _to not in graph[_from]:
            graph[_from].append(_to)
        if _from not in visited:
            visited[_from] = False
        if _to not in visited:
            visited[_to] = False

    for node in list(visited.keys()):
        if not visited[node]:
            dfs(node, graph, visited, stack, circles)

    if circles:
        # for circle in circles:
        #     print(circle)
        # print('')
        return True
    else:
        return False
