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

import gc
import logging
from collections import OrderedDict
from enum import Enum
from itertools import groupby
from typing import Any, List, Set, Union

import attr
import cattr
import jieba
import jsonext
import numpy as np
import requests
from cached_property import threaded_cached_property
from jinja2 import Template
from networkx import DiGraph
from networkx import faster_could_be_isomorphic as is_isomorphic
from scipy.sparse import vstack
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.preprocessing import normalize

from metadata.exc import CompareError
from metadata.runtime import rt_context, rt_g
from metadata.util.i18n import lazy_selfish as _
from metadata_pro.analyse import AnalyseComponent, AnalyseFlow, AnalyseProcedure

logger = logging.getLogger(__name__)


class VectorizerType(Enum):
    COUNT = 'count'
    TF_IDF = 'tf_idf'


class SegmentMode(Enum):
    CHINESE = 'chinese'
    UNDER_SCORE = 'under_score'


class AnalyseMode(Enum):
    CACHED = 'cached'
    UP_TO_DATE = 'up_to_date'


@attr.s(frozen=True)
class RTField(object):
    field_name = attr.ib(type=str)
    field_type = attr.ib(type=str)


@attr.s
class RT(object):
    result_table_id = attr.ib(type=str)
    fields = attr.ib(type=Set[RTField])
    result_table_name = attr.ib(type=str)
    description = attr.ib(type=str)


def compare_content_len(instance, attribute, value):
    if not value:
        raise CompareError(_('The compare content is blank.'))


@attr.s
class CompareContext(object):
    target = attr.ib(type=Union[Set[Any], List[Any]], validator=compare_content_len)
    reference = attr.ib(type=Union[Set[Any], List[Any]], validator=compare_content_len)


def word_segmentation(sentence, segment_mode):
    if not sentence:
        return ['']
    if segment_mode is SegmentMode.CHINESE:
        return jieba.cut(sentence)
    elif segment_mode is SegmentMode.UNDER_SCORE:
        return sentence.split('_')


class BagOfWord(AnalyseComponent):
    """
    词袋。支持count，tf_idf两种词集合向量生成方式。
    """

    def __init__(self, mode=VectorizerType.COUNT):
        super(BagOfWord, self).__init__()
        self.mode = mode

    @threaded_cached_property
    def vectorizer(self):
        return CountVectorizer() if self.mode is VectorizerType.COUNT else TfidfVectorizer()

    def build(self, documents):
        return self.vectorizer.fit_transform(documents)

    def transform(self, documents):
        return self.vectorizer.transform(documents)

    @staticmethod
    def load_corpus():
        documents = []
        ret_rt = rt_context.layers.asset.complex_search(
            statement="select result_table_id,result_table_name,description from result_table", backend_type='mysql'
        )
        ret_rt_field = rt_context.layers.asset.complex_search(
            statement="select result_table_id,field_name,field_alias,field_type from result_table_field",
            backend_type='mysql',
        )
        for item in ret_rt:
            documents.append(
                ' '.join(word_segmentation(item['result_table_name'], segment_mode=SegmentMode.UNDER_SCORE))
            )
            if item['description']:
                documents.append(' '.join(word_segmentation(item['description'], segment_mode=SegmentMode.CHINESE)))
        for item in ret_rt_field:
            documents.append(item['field_name'])
        return documents, ret_rt, ret_rt_field


def renew_context():
    # 生成后台词袋
    bow = BagOfWord(VectorizerType.TF_IDF)
    documents, ret_rt, ret_rt_field = bow.load_corpus()
    bow.build(documents)
    rt_g.bow_now = bow
    logger.info('Bow is generated.')

    # 生成既有RT向量
    rt_info = OrderedDict()
    for item in ret_rt:
        rt_info[item.pop('result_table_id')] = item
    for k, g in groupby(ret_rt_field, key=lambda item: item['result_table_id']):
        if k in rt_info:
            rt_info[k]['fields'] = {item['field_name']: {'field_alias': item['field_alias']} for item in g}
    ss = SchemaSimilarity(bow)
    schema_vectors = ss.transform_input(item.get('fields', []) for item in rt_info.values())
    result_table_name_vectors = DescriptionSimilarity(bow).transform_input(
        item.pop('result_table_name') for item in rt_info.values()
    )
    description_vectors = DescriptionSimilarity(bow, SegmentMode.CHINESE).transform_input(
        item.pop('description') for item in rt_info.values()
    )
    rt_vectors = OrderedDict()
    for n, k in enumerate(rt_info):
        rt_vectors[k] = {
            'schema': schema_vectors[n],
            'result_table_name': result_table_name_vectors[n],
            'description': description_vectors[n],
        }
    rt_g.rt_vectors_now = rt_vectors
    rt_g.rt_ids_now = np.char.asarray(list(rt_vectors.keys()))
    rt_g.rt_info_now = rt_info
    logger.info('Existed RT vectors are generated.')

    gc.collect()


class WordsSetSimilarity(AnalyseProcedure):
    def __init__(self, bow):
        super(WordsSetSimilarity, self).__init__()
        self.bow = bow

    def calc_cos_distance(self, matrix_target, matrix_reference):
        matrix_x = normalize(matrix_target) if self.bow.mode is VectorizerType.COUNT else matrix_target
        matrix_y = matrix_reference.transpose()
        return np.dot(matrix_x, matrix_y)

    def transform_input(self, _input):
        return self.bow.transform(' '.join(item) for item in _input)

    def execute(self, _input=None, *args, **kwargs):
        super(WordsSetSimilarity, self).execute(_input)
        matrix_target = self.transform_input(self.input.target)
        matrix_reference = self.transform_input(self.input.reference)
        self.output = self.calc_cos_distance(matrix_target, matrix_reference)
        return self.output


class SchemaSimilarity(WordsSetSimilarity):
    pass


class DescriptionSimilarity(WordsSetSimilarity):
    def __init__(self, bow, segment_mode=SegmentMode.UNDER_SCORE):
        super(DescriptionSimilarity, self).__init__(bow)
        self.segment_mode = segment_mode

    def transform_input(self, _input):
        words_sets = [word_segmentation(sentence, self.segment_mode) for sentence in _input]
        return super(DescriptionSimilarity, self).transform_input(words_sets)


class ResultTableSimilarity(AnalyseProcedure):
    def __init__(self, bow=None):
        super(ResultTableSimilarity, self).__init__()
        if not bow:
            self.bow = rt_context.bow_now
        self.schema_matrix, self.result_table_name_matrix, self.description_matrix = None, None, None

    def calc_content_similarity(self, **kwargs):
        ws = WordsSetSimilarity(self.bow)
        ws.input = CompareContext(
            [[item.field_name for item in rt.fields] for rt in self.input.target],
            [[item.field_name for item in rt.fields] for rt in self.input.reference],
        )
        self.schema_matrix = ws.execute()
        ds = DescriptionSimilarity(
            self.bow,
        )
        ds.input = CompareContext(
            [rt.result_table_name for rt in self.input.target], [rt.result_table_name for rt in self.input.reference]
        )
        self.result_table_name_matrix = ds.execute()
        ds_2 = DescriptionSimilarity(
            self.bow,
            segment_mode=SegmentMode.CHINESE,
        )
        ds_2.input = CompareContext(
            {rt.description for rt in self.input.target}, {rt.description for rt in self.input.reference}
        )
        self.description_matrix = ds_2.execute()
        return self.schema_matrix * 0.8 + self.result_table_name_matrix * 0.1 + self.description_matrix * 0.1

    def execute(self, *args, **kwargs):
        args, kwargs = super(ResultTableSimilarity, self).execute(*args, **kwargs)
        self.output = self.calc_content_similarity(**kwargs)
        return self.output


class CachedResultTableSimilarity(ResultTableSimilarity):
    def __init__(self, *args, **kwargs):
        super(CachedResultTableSimilarity, self).__init__(*args, **kwargs)
        self.ws = WordsSetSimilarity(self.bow)

    def calc_content_similarity(self, two_way_exchange=False):
        target, reference = self.input.target, self.input.reference
        if two_way_exchange:
            target, reference = reference, target
        self.schema_matrix = self.ws.calc_cos_distance(
            vstack(item['schema'] for item in target), vstack(item['schema'] for item in reference)
        )
        self.result_table_name_matrix = self.ws.calc_cos_distance(
            vstack(item['result_table_name'] for item in target),
            vstack(item['result_table_name'] for item in reference),
        )
        self.description_matrix = self.ws.calc_cos_distance(
            vstack(item['description'] for item in target), vstack(item['description'] for item in reference)
        )
        if two_way_exchange:
            self.schema_matrix = self.schema_matrix.transpose()
            self.result_table_name_matrix = self.result_table_name_matrix.transpose()
            self.description_matrix = self.description_matrix.transpose()
        return self.schema_matrix * 0.8 + self.result_table_name_matrix * 0.1 + self.description_matrix * 0.1


class ResultTableSimilarityAnalyseFlow(AnalyseFlow):
    content_access_graphql = Template(
        "{rt_metadata(func:eq(ResultTable.result_table_id,{{ rt_ids }}))"
        "{result_table_id:ResultTable.result_table_id result_table_name:ResultTable.result_table_name  "
        "description: ResultTable.description "
        "fields:~ResultTableField.result_table "
        "{field_name:ResultTableField.field_name field_type:ResultTableField.field_type}}}"
    )

    def __init__(self):
        super(ResultTableSimilarityAnalyseFlow, self).__init__()

    def load_rt_metadata(self):
        all_rt_ids = set(self.input.target) | set(self.input.reference)
        query_statement = self.content_access_graphql.render(rt_ids=jsonext.dumps(all_rt_ids))
        ret_rt = rt_context.layers.asset.complex_search(statement=query_statement, backend_type='dgraph')
        ret_rt_dct = {item['result_table_id']: item for item in ret_rt['data']['rt_metadata']}
        not_existed_rt_id = set(all_rt_ids) - set(ret_rt_dct)
        if not_existed_rt_id:
            raise CompareError(_('Some rt id {} is not existed.').format(not_existed_rt_id))
        target_rt_metadata = [cattr.structure(ret_rt_dct[rt_name], RT) for rt_name in self.input.target]
        reference_rt_metadata = [cattr.structure(ret_rt_dct[rt_name], RT) for rt_name in self.input.reference]
        return {'target': target_rt_metadata, 'reference': reference_rt_metadata}

    def execute(self, *args, **kwargs):
        super(ResultTableSimilarityAnalyseFlow, self).execute(*args, **kwargs)
        rt_metadata = self.load_rt_metadata()
        a = ResultTableSimilarity()
        a.execute(CompareContext(**rt_metadata))
        self.output = {
            'final_goal': a.output.toarray(),
            'detail': {
                'schema': a.schema_matrix.toarray(),
                'name': a.result_table_name_matrix.toarray(),
                'description': a.description_matrix.toarray(),
            },
        }
        return self.output


class CachedResultTableSimilarityAnalyseFlow(AnalyseFlow):
    def __init__(self):
        super(CachedResultTableSimilarityAnalyseFlow, self).__init__()

    def load_rt_metadata(self):
        if set(self.input.target) == set(self.input.reference) == {'*'}:
            raise CompareError(_("Can't compare all items together."))
        for rt_id in set(self.input.target) | set(self.input.reference):
            if rt_id not in rt_context.rt_vectors_now and rt_id != "*":
                raise CompareError('RT id {} is not in cache.'.format(rt_id))
        target = (
            [rt_context.rt_vectors_now[rt_id] for rt_id in self.input.target]
            if set(self.input.target) != {'*'}
            else list(rt_context.rt_vectors_now.values())
        )
        reference = (
            [rt_context.rt_vectors_now[rt_id] for rt_id in self.input.reference]
            if set(self.input.reference) != {'*'}
            else list(rt_context.rt_vectors_now.values())
        )
        two_way_exchange = True if set(self.input.reference) == {'*'} else False
        return target, reference, two_way_exchange

    def execute(self, _input=None, reference_max_n=None, *args, **kwargs):
        super(CachedResultTableSimilarityAnalyseFlow, self).execute(_input)
        target, reference, two_way_exchange = self.load_rt_metadata()
        a = CachedResultTableSimilarity()
        a.execute(CompareContext(target, reference), two_way_exchange=two_way_exchange)
        ret = {
            'final_goal': a.output.toarray(),
            'detail': {
                'schema': a.schema_matrix.toarray(),
                'name': a.result_table_name_matrix.toarray(),
                'description': a.description_matrix.toarray(),
            },
        }
        if reference_max_n:
            self.gather_reference_top(reference_max_n, ret)
        self.output = ret
        return self.output

    @staticmethod
    def gather_reference_top(reference_max_n, ret):
        indexes = np.unravel_index(np.argsort(ret['final_goal'], axis=1)[:, -reference_max_n:], ret['final_goal'].shape)
        ret['final_goal'] = ret['final_goal'][indexes]
        for k, v in ret['detail'].items():
            ret['detail'][k] = v[indexes]

        ret['reference_result_table_ids'] = [rt_context.rt_ids_now[i] for i in indexes[1]]


class FieldsSuggestionAnalyseFlow(AnalyseFlow):
    def __init__(self, bow=None):
        super(FieldsSuggestionAnalyseFlow, self).__init__()
        if not bow:
            self.bow = rt_context.bow_now
        self.ss = SchemaSimilarity(self.bow)

    def execute(self, _input=None, match_percent=0.5, *args, **kwargs):
        super(FieldsSuggestionAnalyseFlow, self).execute(_input)
        matrix_target = self.ss.transform_input(self.input)
        schema_matrix = self.ss.calc_cos_distance(
            vstack(item['schema'] for item in rt_context.rt_vectors_now.values()),
            matrix_target,
        )
        schema_matrix = schema_matrix.transpose()
        schema_similarity, reference_result_table_ids = self.gather_reference_top(
            match_percent, schema_matrix.toarray()
        )
        rts_fields_info = self.query_rt_info(reference_result_table_ids)
        self.output = {'schema_similarity': schema_similarity, 'rts_fields_info': rts_fields_info}
        return self.output

    def query_rt_info(self, reference_result_table_ids):
        info_lst = []
        for lst in reference_result_table_ids:
            info_lst.append(OrderedDict((rt_id, rt_g.rt_info_now[rt_id]) for rt_id in lst))
        return info_lst

    @staticmethod
    def gather_reference_top(match_percent, ret):
        r, c = np.where(ret >= match_percent)
        indexes = np.split(c, np.searchsorted(r, list(range(1, ret.shape[0]))))
        indexes = np.unravel_index(indexes, ret.shape)
        ret = ret[indexes]
        reference_result_table_ids = [rt_context.rt_ids_now[i] for i in indexes[1]]
        return ret, reference_result_table_ids


def is_chinese(word):
    for ch in word:
        if '\u4e00' <= ch <= '\u9fff':
            return True
    return False


class FieldAliasSuggestionAnalyseFlow(FieldsSuggestionAnalyseFlow):
    def execute(self, _input=None, *args, **kwargs):
        ret = super(FieldAliasSuggestionAnalyseFlow, self).execute(_input, *args, **kwargs)
        suggest_aliases = {}
        for dct in ret['rts_fields_info']:
            for rt_info in dct.values():
                for field_name, info in rt_info['fields'].items():
                    if is_chinese(info['field_alias']) and field_name not in suggest_aliases:
                        suggest_aliases[field_name] = info['field_alias']
        suggested_aliases_lst = [
            {field_name: suggest_aliases.get(field_name, None) for field_name in field_name_lst}
            for field_name_lst in self.input
        ]
        return suggested_aliases_lst


class ResultTableLineageSimilarityAnalyseFlow(AnalyseFlow):
    def __init__(self):
        super(ResultTableLineageSimilarityAnalyseFlow, self).__init__()
        self.normal_conf = rt_context.config_collection.normal_config

    def query_lineage(self, qualified_name):
        url = self.normal_conf.meta_api_url + 'lineage/'
        r = requests.get(url, params={'type': 'result_table', 'qualified_name': qualified_name})
        r.raise_for_status()
        info = r.json()
        if info['result'] and info['data'] and info['data']['relations']:
            return info['data']
        else:
            raise CompareError(_('Fail to get lineage.'))

    def build_lineage_graph(self, qualified_name):
        edges = [(relation['from'], relation['to']) for relation in self.query_lineage(qualified_name)['relations']]
        g = DiGraph()
        g.add_edges_from(edges)
        return g

    def execute(self, _input=None, *args, **kwargs):
        super(ResultTableLineageSimilarityAnalyseFlow, self).execute(_input)
        target, reference = self.input.target[0], self.input.reference[0]
        target_g = self.build_lineage_graph(target)
        reference_g = self.build_lineage_graph(reference)
        self.output = is_isomorphic(target_g, reference_g)
        return self.output
