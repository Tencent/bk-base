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

package com.tencent.bk.base.dataflow.ml.spark;

import com.tencent.bk.base.dataflow.server.UCMain;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;

public class TestMain {

    /**
     * 测试主入口
     */
    public static void main(String[] args) throws Exception {
        /*InputStream inputStream = TestMain.class.getResourceAsStream("/feature_transformers/test_tokenizer.json");
        InputStream inputStream = TestMain.class
                .getResourceAsStream("/feature_transformers/test_stop_words_remover.json");
        InputStream inputStream = TestMain.class.getResourceAsStream("/feature_transformers/test_n_gram.json");
        InputStream inputStream = TestMain.class.getResourceAsStream("/feature_transformers/test_binarizer.json");
        InputStream inputStream = TestMain.class.getResourceAsStream("/feature_transformers/test_pca.json");
        InputStream inputStream = TestMain.class
                .getResourceAsStream("/feature_transformers/test_vector_assmembler.json");
        InputStream inputStream = TestMain.class.getResourceAsStream("/feature_transformers/test_poly_expansion.json");
        InputStream inputStream = TestMain.class.getResourceAsStream("/feature_transformers/test_dct.json");
        InputStream inputStream = TestMain.class.getResourceAsStream("/feature_transformers/test_string_indexer.json");
        InputStream inputStream = TestMain.class
                .getResourceAsStream("/feature_transformers/test_one_encoder_estimator.json");
        InputStream inputStream = TestMain.class.getResourceAsStream("/feature_transformers/test_bucketizer.json");
        InputStream inputStream = TestMain.class
                .getResourceAsStream("/feature_transformers/test_elementwise_product.json");
        InputStream inputStream = TestMain.class
                .getResourceAsStream("/classification_regression/test_decision_tree_classifier.json");*/
        InputStream inputStream = TestMain.class.getResourceAsStream("/feature_transformers/test_periodic_modl.json");

        String s = IOUtils.toString(inputStream);
        UCMain.init(s);
        inputStream.close();

    }
}
