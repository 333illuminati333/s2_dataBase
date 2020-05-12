# -*- coding: utf-8 -*-
from scrapy.http.response import Response
import scrapy


class OdisseySpider(scrapy.Spider):
    name = 'odissey'
    allowed_domains = ['odissey.kiev.ua']
    start_urls = ['https://odissey.kiev.ua/category_3640.html']

    def parse(self, response: Response):
        products = response.xpath("//div[contains(@class, 'row table-row')]")[:20]
        for number in range(20):
            yield {
                'description': products.xpath("//a[contains(@class, 'pnameh')]/text()").extract()[number],
                'price': products.xpath("//div[contains(@class, 'pprice')]/text()").extract()[number],
                'img': 'https://odissey.kiev.ua/' + products.xpath("//img[contains(@class, 'thumbnail')]/@src").extract()[number]
            }
