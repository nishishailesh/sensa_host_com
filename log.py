import logging

FORMAT = '%(asctime)s %(clientip)-15s %(user)-8s %(message)s'
logging.basicConfig(format=FORMAT)
logging.debug('Protocol problem: %s', 'connection reset', "dddd")
