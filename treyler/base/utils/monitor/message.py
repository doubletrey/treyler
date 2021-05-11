import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def generate_message(spider=None, task=None, status=0, process=None, **kwargs):
    """
    platform: str 平台名称
    spider: str 爬虫名称
    task: str or dict 源任务信息 or 关联任务信息
    status: integer 0 成功，-1 失败, 1 警告
    process: str 过程名称，download, parse, pipeline, etc...
    """
    assert process is not None
    return {
        'platform': getattr(spider, 'platform', None),
        'spider': getattr(spider, 'name', None),
        'process': process,
        'status': status,
        'task': task,
        'extra': kwargs,
        'date_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
    }
