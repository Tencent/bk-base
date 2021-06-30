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

import abc

from common.exceptions import ValidationError


class Rule(abc.ABC):
    """
    The abstract class of DataQuality Rule.

    Attributes:
        _config: The detail configuration of the Rule
    """

    def __init__(self, config):
        self._config = config


class RuleItem(abc.ABC):
    """
    The abstract class of Rule Item in the DataQuality Rule configs.

    Attributes:
        _input: The input list of the rule.
        _rule: The rule detail logic of this rule item.
        _output: The output event config of the rule.
    """

    def __init__(self, input, rule, output):
        self._input = input
        self._rule = rule
        self._output = output

    @property
    def input(self):
        return self._input

    @property
    def rule(self):
        return self._rule

    @property
    def output(self):
        return self._output


class Template(abc.ABC):
    """
    The abstract class of DataQuality Rule Template.
    """

    def __init__(self):
        pass


class Function(abc.ABC):
    """
    The abstract class of DataQuality function for Rule functions.
    """

    NAME = ""
    ALIAS = ""
    TYPE = ""
    LEAST_PARAMETERS_COUNT = None
    PARAMETERS_COUNT = None
    ONLY_NUMBER = False
    ONLY_STRING = False

    def __init__(self):
        pass

    def call(self, *args):
        if self.LEAST_PARAMETERS_COUNT:
            self.check_least_parameters_count(args, self.LEAST_PARAMETERS_COUNT)

        if self.PARAMETERS_COUNT:
            self.check_parameters_count(args, self.PARAMETERS_COUNT)

        if self.ONLY_NUMBER:
            self.check_parameters_type(args, (int, float))

        if self.ONLY_STRING:
            self.check_parameters_type(args, (str,))

        self.validate(*args)
        return self.execute(*args)

    def check_least_parameters_count(self, args, count):
        if not len(args) >= count:
            raise ValidationError(
                "More than 1 parameters are required for `function({})`".format(
                    self.__class__.__name__
                )
            )

    def check_parameters_count(self, args, count):
        if len(args) != count:
            raise ValidationError(
                "{} parameter(s) are required for `function({})`".format(
                    len(args), self.__class__.__name__
                )
            )

    def check_parameters_type(self, args, types):
        for arg in args:
            if not isinstance(arg, types):
                raise ValidationError(
                    "The number field is required for `function({})`".format(
                        self.__class__.__name__
                    )
                )

    @abc.abstractmethod
    def execute(self, *args):
        raise NotImplementedError()

    def validate(self, *args):
        pass


class Timer(abc.ABC):
    def __init__(self, timer_config):
        self._timer_config = timer_config

    @abc.abstractmethod
    def next(self):
        pass

    @abc.abstractmethod
    def tick(self):
        pass

    @abc.abstractmethod
    def finished(self):
        pass
