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

from __future__ import absolute_import, print_function, unicode_literals

import re

import six
from dm_engine.base.exceptions import CrontabParseError
from dm_engine.config import settings
from dm_engine.utils.time import timestamp_to_arrow, weekday

string_t = six.text_type

ParseException = CrontabParseError

CRON_PATTERN_INVALID = """\
Invalid crontab pattern.  Valid range is {min}-{max}. \
'{value}' was found.\
"""


class Crontab(object):
    """
    Crontab analysis crontab-expression, such as '0 16 1,10,22 * *'

        - Example '* * * * *'
            It means: every minute of every hour of every day of the month for
            every month for every day of the week.

        - Example '0 16 1,10,22 * *'
            It tells cron to run a task at 4 PM (which is the 16th hour) on
            the 1st, 10th and 22nd day of every month.

    Inner handler switch expression '0 1,13,30 * * *' to that CrontabObject
        CrontabObject.minute = [0]
        CrontabObject.hours = [1, 13, 30]
        CrontabObject.day_of_month = [1, 2, ..., 31]
        CrontabObject.month_of_year = [1, 2, ..., 12]
        CrontabObject.day_of_week = [0, 1, 2, 3, 4, 5, 6]
    """

    def __init__(self, cronspec):
        self.cronspec = cronspec

        parts = self.cronspec.split(" ")
        minute = parts[0]
        hour = parts[1]
        day_of_month = parts[2]
        month_of_year = parts[3]
        day_of_week = parts[4]

        self.hour = self._expand_cronspec(hour, 24)
        self.minute = self._expand_cronspec(minute, 60)
        self.day_of_week = self._expand_cronspec(day_of_week, 7)
        self.day_of_month = self._expand_cronspec(day_of_month, 31, 1)
        self.month_of_year = self._expand_cronspec(month_of_year, 12, 1)
        self.tz = settings.TIME_ZONE

    def match(self, timestamp):
        """
        Check whether timestamp match the crontab settings
        """
        target_time = timestamp_to_arrow(timestamp, self.tz)

        if target_time.minute not in self.minute:
            return False
        if target_time.hour not in self.hour:
            return False
        if target_time.day not in self.day_of_month:
            return False
        if target_time.month not in self.month_of_year:
            return False
        # arrow time object use Python’s weekday numbers (Monday = 0, Tuesday = 1 .. Sunday = 6)
        if (target_time.weekday() + 1) % 7 not in self.day_of_week:
            return False

        return True

    @staticmethod
    def _expand_cronspec(cronspec, max_, min_=0):
        """Expand cron specification.

        Takes the given cronspec argument in one of the forms:

        .. code-block:: text

            int         (like 7)
            str         (like '3-5,*/15', '*', or 'monday')
            set         (like {0,15,30,45}
            list        (like [8-17])

        And convert it to an (expanded) set representing all time unit
        values on which the Crontab triggers.  Only in case of the base
        type being :class:`str`, parsing occurs.  (It's fast and
        happens only once for each Crontab instance, so there's no
        significant performance overhead involved.)

        For the other base types, merely Python type conversions happen.

        The argument ``max_`` is needed to determine the expansion of
        ``*`` and ranges.  The argument ``min_`` is needed to determine
        the expansion of ``*`` and ranges for 1-based cronspecs, such as
        day of month or month of year.  The default is sufficient for minute,
        hour, and day of week.
        """
        result = crontab_parser(max_, min_).parse(cronspec)

        # assure the result does not preceed the min or exceed the max
        for number in result:
            if number >= max_ + min_ or number < min_:
                raise ValueError(CRON_PATTERN_INVALID.format(min=min_, max=max_ - 1 + min_, value=number))
        return result

    def __repr__(self):
        return self.cronspec

    def __str__(self):
        return self.cronspec


class crontab_parser(object):
    """Parser for Crontab expressions.

    Any expression of the form 'groups'
    (see BNF grammar below) is accepted and expanded to a set of numbers.
    These numbers represent the units of time that the Crontab needs to
    run on:

    .. code-block:: bnf

        digit   :: '0'..'9'
        dow     :: 'a'..'z'
        number  :: digit+ | dow+
        steps   :: number
        range   :: number ( '-' number ) ?
        numspec :: '*' | range
        expr    :: numspec ( '/' steps ) ?
        groups  :: expr ( ',' expr ) *

    The parser is a general purpose one, useful for parsing hours, minutes and
    day of week expressions.  Example usage:

    .. code-block:: pycon

        >>> minutes = crontab_parser(60).parse('*/15')
        [0, 15, 30, 45]
        >>> hours = crontab_parser(24).parse('*/4')
        [0, 4, 8, 12, 16, 20]
        >>> day_of_week = crontab_parser(7).parse('*')
        [0, 1, 2, 3, 4, 5, 6]

    It can also parse day of month and month of year expressions if initialized
    with a minimum of 1.  Example usage:

    .. code-block:: pycon

        >>> days_of_month = crontab_parser(31, 1).parse('*/3')
        [1, 4, 7, 10, 13, 16, 19, 22, 25, 28, 31]
        >>> months_of_year = crontab_parser(12, 1).parse('*/2')
        [1, 3, 5, 7, 9, 11]
        >>> months_of_year = crontab_parser(12, 1).parse('2-12/2')
        [2, 4, 6, 8, 10, 12]

    The maximum possible expanded value returned is found by the formula:

        :math:`max_ + min_ - 1`
    """

    ParseException = ParseException

    _range = r"(\w+?)-(\w+)"
    _steps = r"/(\w+)?"
    _star = r"\*"

    def __init__(self, max_=60, min_=0):
        self.max_ = max_
        self.min_ = min_
        self.pats = (
            (re.compile(self._range + self._steps), self._range_steps),
            (re.compile(self._range), self._expand_range),
            (re.compile(self._star + self._steps), self._star_steps),
            (re.compile("^" + self._star + "$"), self._expand_star),
        )

    def parse(self, spec):
        acc = set()
        for part in spec.split(","):
            if not part:
                raise self.ParseException("empty part")
            acc |= set(self._parse_part(part))
        return acc

    def _parse_part(self, part):
        for regex, handler in self.pats:
            m = regex.match(part)
            if m:
                return handler(m.groups())
        return self._expand_range((part,))

    def _expand_range(self, toks):
        fr = self._expand_number(toks[0])
        if len(toks) > 1:
            to = self._expand_number(toks[1])
            if to < fr:  # Wrap around max_ if necessary
                return list(range(fr, self.min_ + self.max_)) + list(range(self.min_, to + 1))
            return list(range(fr, to + 1))
        return [fr]

    def _range_steps(self, toks):
        if len(toks) != 3 or not toks[2]:
            raise self.ParseException("empty filter")
        return self._expand_range(toks[:2])[:: int(toks[2])]

    def _star_steps(self, toks):
        if not toks or not toks[0]:
            raise self.ParseException("empty filter")
        return self._expand_star()[:: int(toks[0])]

    def _expand_star(self, *args):
        return list(range(self.min_, self.max_ + self.min_))

    def _expand_number(self, s):
        if isinstance(s, string_t) and s[0] == "-":
            raise self.ParseException("negative numbers not supported")
        try:
            i = int(s)
        except ValueError:
            try:
                i = weekday(s)
            except KeyError:
                raise ValueError("Invalid weekday literal {!r}.".format(s))

        max_val = self.min_ + self.max_ - 1
        if i > max_val:
            raise ValueError("Invalid end range: {} > {}.".format(i, max_val))
        if i < self.min_:
            raise ValueError("Invalid beginning range: {} < {}.".format(i, self.min_))

        return i
