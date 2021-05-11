# -*- coding: utf-8 -*-

import logging

from scrapy.http import Request

logger = logging.getLogger(__name__)


class CustomDepthMiddleware(object):
    """
    custom depth middleware
    filter request that with `custom_depth` in meta.

        `custom_depth`: int, default 0 for no limit
    """

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def process_spider_output(self, response, result, spider):
        def _filter(request):
            if isinstance(request, Request):
                depth = response.meta['depth'] + 1
                custom_max_depth = request.meta.get('custom_max_depth')
                if custom_max_depth and depth >= custom_max_depth:      # xxx: diff from `DepthMiddleware`
                    # TODO: change to debug in the future
                    logger.info(
                        "Ignoring link (depth > custom_max_depth %(custom_depth)d): %(req_url)s ",
                        {'custom_depth': custom_max_depth, 'req_url': request.url},
                        extra={'spider': spider}
                    )
                    return False
            return True

        # base case (depth=0)
        if 'depth' not in response.meta:
            response.meta['depth'] = 0

        return (r for r in result or () if _filter(r))
