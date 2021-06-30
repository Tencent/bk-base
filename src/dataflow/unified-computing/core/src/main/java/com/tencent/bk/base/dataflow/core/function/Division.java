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

package com.tencent.bk.base.dataflow.core.function;

import com.tencent.bk.base.dataflow.core.function.base.IFunction;
import java.io.Serializable;

public class Division implements IFunction, Serializable {

    // Return null if the divisor is zero.

    // ------------------divisor is integer---------------------------------

    /**
     * bkdata division
     *
     * @param dividend Long dividend
     * @param divisor Integer divisor
     * @return
     */
    public Double call(final Long dividend, final Integer divisor) {
        double value = dividend.doubleValue() / divisor;
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        } else {
            return value;
        }
    }

    /**
     * bkdata division
     *
     * @param dividend Double dividend
     * @param divisor Integer divisor
     * @return
     */
    public Double call(final Double dividend, final Integer divisor) {
        double value = dividend / divisor;
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        } else {
            return value;
        }
    }

    /**
     * bkdata division
     *
     * @param dividend Float dividend
     * @param divisor Integer divisor
     * @return
     */
    public Double call(final Float dividend, final Integer divisor) {
        double value = dividend.doubleValue() / divisor;
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        } else {
            return value;
        }
    }

    /**
     * bkdata division
     *
     * @param dividend Integer dividend
     * @param divisor Integer divisor
     * @return
     */
    public Double call(final Integer dividend, final Integer divisor) {
        double value = dividend.doubleValue() / divisor;
        if ("infinity".equalsIgnoreCase(String.valueOf(value)) || "-infinity"
                .equalsIgnoreCase(String.valueOf(value))
                || "nan".equalsIgnoreCase(String.valueOf(value))) {
            return null;
        } else {
            return value;
        }
    }

    // ------------------divisor is long---------------------------------

    /**
     * bkdata division
     *
     * @param dividend Long dividend
     * @param divisor Long divisor
     * @return
     */
    public Double call(final Long dividend, final Long divisor) {
        double value = dividend.doubleValue() / divisor;
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        } else {
            return value;
        }
    }

    /**
     * bkdata division
     *
     * @param dividend Double dividend
     * @param divisor Long divisor
     * @return
     */
    public Double call(final Double dividend, final Long divisor) {
        double value = dividend / divisor;
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        } else {
            return value;
        }
    }

    /**
     * bkdata division
     *
     * @param dividend Float dividend
     * @param divisor Long divisor
     * @return
     */
    public Double call(final Float dividend, final Long divisor) {
        double value = dividend.doubleValue() / divisor;
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        } else {
            return value;
        }
    }

    /**
     * bkdata division
     *
     * @param dividend Integer dividend
     * @param divisor Long divisor
     * @return
     */
    public Double call(final Integer dividend, final Long divisor) {
        double value = dividend.doubleValue() / divisor;
        if ("infinity".equalsIgnoreCase(String.valueOf(value)) || "-infinity"
                .equalsIgnoreCase(String.valueOf(value))
                || "nan".equalsIgnoreCase(String.valueOf(value))) {
            return null;
        } else {
            return value;
        }
    }

    // ------------------divisor is float---------------------------------

    /**
     * bkdata division
     *
     * @param dividend Long dividend
     * @param divisor Float divisor
     * @return
     */
    public Double call(final Long dividend, final Float divisor) {
        double value = dividend.doubleValue() / divisor;
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        } else {
            return value;
        }
    }

    /**
     * bkdata division
     *
     * @param dividend Double dividend
     * @param divisor Float divisor
     * @return
     */
    public Double call(final Double dividend, final Float divisor) {
        double value = dividend / divisor;
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        } else {
            return value;
        }
    }

    /**
     * bkdata division
     *
     * @param dividend Float dividend
     * @param divisor Float divisor
     * @return
     */
    public Double call(final Float dividend, final Float divisor) {
        double value = dividend.doubleValue() / divisor;
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        } else {
            return value;
        }
    }

    /**
     * bkdata division
     *
     * @param dividend Integer dividend
     * @param divisor Float divisor
     * @return
     */
    public Double call(final Integer dividend, final Float divisor) {
        double value = dividend.doubleValue() / divisor;
        if ("infinity".equalsIgnoreCase(String.valueOf(value)) || "-infinity"
                .equalsIgnoreCase(String.valueOf(value))
                || "nan".equalsIgnoreCase(String.valueOf(value))) {
            return null;
        } else {
            return value;
        }
    }

    // ------------------divisor is double---------------------------------

    /**
     * bkdata division
     *
     * @param dividend Long dividend
     * @param divisor Double divisor
     * @return
     */
    public Double call(final Long dividend, final Double divisor) {
        double value = dividend.doubleValue() / divisor;
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        } else {
            return value;
        }
    }

    /**
     * bkdata division
     *
     * @param dividend Double dividend
     * @param divisor Double divisor
     * @return
     */
    public Double call(final Double dividend, final Double divisor) {
        double value = dividend / divisor;
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        } else {
            return value;
        }
    }

    /**
     * bkdata division
     *
     * @param dividend Float dividend
     * @param divisor Double divisor
     * @return
     */
    public Double call(final Float dividend, final Double divisor) {
        double value = dividend.doubleValue() / divisor;
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        } else {
            return value;
        }
    }

    /**
     * bkdata division
     *
     * @param dividend Integer dividend
     * @param divisor Double divisor
     * @return
     */
    public Double call(final Integer dividend, final Double divisor) {
        double value = dividend.doubleValue() / divisor;
        if ("infinity".equalsIgnoreCase(String.valueOf(value)) || "-infinity"
                .equalsIgnoreCase(String.valueOf(value))
                || "nan".equalsIgnoreCase(String.valueOf(value))) {
            return null;
        } else {
            return value;
        }
    }
}
