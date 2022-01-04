import os
import sys

from loguru import logger

SELECTED_ENV = 'AIRFLOW'
confs = {
    'AIRFLOW': {
        # Airflow log util has already add the time
        'format':
        '[{level}]\t{name}:{function}:{line}\t{message}',
        'sinks': [
            {
                'output': sys.stdout,
            },
            #  {
            #      'output': 'my.log',
            #      'rotation': '500 MB'
            #  },
        ]
    }
}

conf = confs[SELECTED_ENV]

# Override the confs by env vars is unnecessary
# The framework has already handled the env vars
#  FORMAT = USER = os.getenv('LOGURU_FORMAT') or None
#  if FORMAT:
#      conf['format'] = FORMAT

logger.remove()
for sink in conf['sinks']:
    kargs = [sink['output']]
    kwargs = {'format': conf['format']}
    if 'rotation' in sink:
        kwargs['rotation'] = sink['rotation']
    logger.add(*kargs, **kwargs)


def get_logger():
    return logger
