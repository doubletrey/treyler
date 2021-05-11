# -*- coding: utf-8 -*-
import scrapy


class BaseItem(scrapy.Item):
    """
    基本item
    platform: 平台名称
    spider: 爬虫名称
    task: 具体任务
    下游存储根据platform、spider获取数据存储表
    """
    platform = scrapy.Field()
    spider = scrapy.Field()
    task = scrapy.Field()
    priority = scrapy.Field()
    extra = scrapy.Field()
