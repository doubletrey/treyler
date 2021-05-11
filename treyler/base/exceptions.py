# -*- coding: utf-8 -*-
from scrapy.exceptions import IgnoreRequest


class ParseException(Exception):
    """
    parse异常
    """
    pass


class PipelineException(Exception):
    """
    pipeline异常
    """
    pass


class RetryException(Exception):
    """
    fake exception, used when need retry
    """
    pass


class GiveUpRequest(IgnoreRequest):
    """
    retry give up
    """
    pass


class UnavailableError(IgnoreRequest):
    """A  404 response was processed"""

    def __init__(self, response, *args, **kwargs):
        self.response = response
        super(UnavailableError, self).__init__(*args, **kwargs)
