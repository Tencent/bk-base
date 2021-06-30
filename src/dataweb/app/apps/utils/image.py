# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

import math
import os

from django.conf import settings
from PIL import Image, ImageDraw, ImageFont


def produce_watermark(text):
    """
    将文字内容生成图片

    @param {String} text 文字内容
    @return {Image} 图片对象
    """
    image_w = 720
    image_h = 537
    k = 24

    text_image_w = int(image_w * 1.5)  # 确定写文字图片的尺寸，如前所述，要比照片大，这里取1.5倍
    text_image_h = int(image_h * 1.5)
    blank = Image.new("RGBA", (text_image_w, text_image_h), (255, 255, 255, 0))  # 创建用于添加文字的空白图像
    d = ImageDraw.Draw(blank)  # 创建draw对象
    d.ink = 0 + 0 * 256 + 0 * 256 * 256  # 黑色
    # 创建Font对象，k之为字号
    font = ImageFont.truetype(os.path.join(settings.PROJECT_ROOT, "static/font/arial.ttf"), k)
    text_w, text_h = font.getsize(text)  # 获取文字尺寸
    d.text([(text_image_w - text_w) / 2, (text_image_h - text_h) / 2], text, font=font, fill=(180, 180, 190, 110))
    # 旋转文字
    text_rotate = blank.rotate(30)
    # text_rotate.show()
    r_len = math.sqrt((text_w / 2) ** 2 + (text_h / 2) ** 2)
    ori_angle = math.atan(text_h / text_w)
    crop_w = r_len * math.cos(ori_angle + math.pi / 6) * 2  # 被截取区域的宽高
    crop_h = r_len * math.sin(ori_angle + math.pi / 6) * 2
    box = [
        int((text_image_w - crop_w) / 2 - 1) - 40,
        int((text_image_h - crop_h) / 2 - 1) - 40,
        int((text_image_w + crop_w) / 2 + 40),
        int((text_image_h + crop_h) / 2 + 40),
    ]
    text_im = text_rotate.crop(box)  # 截取文字图片
    # text_im.show()
    crop_w, crop_h = text_im.size

    # 旋转后的文字图片粘贴在一个新的blank图像上
    text_blank = Image.new("RGBA", (image_w, image_h), (255, 255, 255, 0))
    for i in range(4):
        for j in range(4):
            paste_box = (int(crop_w * j), int(crop_h * i))
            text_blank.paste(text_im, paste_box)
    text_blank = text_blank.resize((image_w, image_h), Image.ANTIALIAS)
    return text_blank
