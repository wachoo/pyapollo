# -*- coding: utf-8 -*-
import json
import logging
import os
import platform
import sys
import threading
import time
from multiprocessing import Process

import psutil as psutil
import requests


class ApolloClient(object):

    def __init__(self, app_id, cluster='default', server_url=None, timeout=30, ip=None):
        self.config_server_url = server_url if server_url is not None else self.get_apollo_server_url()
        self.appId = app_id
        self.cluster = cluster
        self.timeout = timeout
        self.stopped = False
        self.ip = ip if ip is not None else self.init_ip()

        self._stopping = False
        self._cache = {}
        self._notification_map = {'application': -1}

    def get_apollo_server_url(self):
        os_name = platform.system()
        logging.getLogger(__name__).info("os_name: {}".format(os_name))
        if os_name == "Windows":
            config_path = "c:/data/ucm2/settings/server.properties"
        else:
            config_path = "/data/ucm2/settings/server.properties"
        zk_url = ""
        logging.getLogger(__name__).info("open ucm via path: {}".format(config_path))
        with open(config_path) as lines:
            for line in lines:
                logging.getLogger(__name__).info("ucm: {}".format(line))
                if str.startswith(line, "apollo.meta"):
                    zk_url = str.split(line, "=")[1].strip()
        return zk_url.split(",")[0]

    def init_ip(self):
        import socket
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 53))
            ip = s.getsockname()[0]
        finally:
            s.close()
        return ip

    # Main method
    def get_value(self, key, default_val=None, namespace='application', auto_fetch_on_cache_miss=False):
        if namespace not in self._notification_map:
            self._notification_map[namespace] = -1
            logging.getLogger(__name__).info("Add namespace '%s' to local notification map", namespace)

        if namespace not in self._cache:
            self._cache[namespace] = {}
            logging.getLogger(__name__).info("Add namespace '%s' to local cache", namespace)
            # This is a new namespace, need to do a blocking fetch to populate the local cache
            self._long_poll()

        if key in self._cache[namespace]:
            return self._cache[namespace][key]
        else:
            if auto_fetch_on_cache_miss:
                return self._cached_http_get(key, default_val, namespace)
        return default_val

    # Start the long polling loop. Two modes are provided:
    # 1: thread mode (default), create a worker thread to do the loop. Call self.stop() to quit the loop
    # 2: eventlet mode (recommended), no need to call the .stop() since it is async
    def start(self, use_eventlet=False, eventlet_monkey_patch=False, catch_signals=False, use_process=False):
        # First do a blocking long poll to populate the local cache, otherwise we may get racing problems
        if len(self._cache) == 0:
            self._long_poll()
        if use_eventlet:
            import eventlet
            if eventlet_monkey_patch:
                eventlet.monkey_patch()
            eventlet.spawn(self._listener)
            return
        elif use_process:
            p = Process(target=self._listener_p, name='demon_apollo_listener', args=(os.getpid(),), daemon=True)
            p.start()
            return
        elif catch_signals:
            import signal
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
            signal.signal(signal.SIGABRT, self._signal_handler)
        t = threading.Thread(target=self._listener, name='demon_apollo_listener', daemon=True)
        t.start()

    def stop(self):
        self._stopping = True
        logging.getLogger(__name__).info("Stopping apollo-listener...")

    def _cached_http_get(self, key, default_val, namespace='application'):
        url = '{}/configfiles/json/{}/{}/{}?ip={}'.format(self.config_server_url, self.appId, self.cluster, namespace,
                                                          self.ip)
        r = requests.get(url)
        if r.ok:
            data = r.json()
            self._cache[namespace] = data
            logging.getLogger(__name__).info('Updated local cache for namespace %s', namespace)
        else:
            data = self._cache[namespace]

        if key in data:
            return data[key]
        else:
            return default_val

    def _uncached_http_get(self, namespace='application'):
        url = '{}/configs/{}/{}/{}?ip={}'.format(self.config_server_url, self.appId, self.cluster, namespace, self.ip)
        r = requests.get(url)
        if r.status_code == 200:
            data = r.json()
            self._cache[namespace] = data['configurations']
            logging.getLogger(__name__).info('Updated local cache for namespace %s release key %s: %s',
                                             namespace, data['releaseKey'],
                                             repr(self._cache[namespace]))

    def _signal_handler(self):
        logging.getLogger(__name__).info('signal_handler stop!')
        self._stopping = True

    def _long_poll(self):
        url = '{}/notifications/v2'.format(self.config_server_url)
        notifications = []
        for key in self._notification_map:
            notification_id = self._notification_map[key]
            notifications.append({
                'namespaceName': key,
                'notificationId': notification_id
            })
        r = None
        params = {
            'appId': self.appId,
            'cluster': self.cluster,
            'notifications': json.dumps(notifications, ensure_ascii=False)
        }
        try:
            r = requests.get(url=url, params=params, timeout=self.timeout)
            logging.getLogger(__name__).info('Long polling returns %d, url=%s, params=%s', r.status_code, r.request.url
                                             , params)
        except Exception as e:
            logging.getLogger(__name__).exception(e)

        if not r:
            logging.getLogger(__name__).warning('Long polling returns None, url=%s, params=%s, loop...', url, params)
            return

        if r.status_code == 304:
            # no change, loop
            logging.getLogger(__name__).debug('No change, loop...')
            return
        if r.status_code == 200:
            data = r.json()
            for entry in data:
                ns = entry['namespaceName']
                nid = entry['notificationId']
                logging.getLogger(__name__).debug("%s has changed: notificationId, before:%d, after:%d", ns,
                                                  self._notification_map.get(ns), nid)
                self._uncached_http_get(ns)
                # self._notification_map[ns] = nid
        else:
            logging.getLogger(__name__).warning('Sleep...')

    def _listener(self):
        logging.getLogger(__name__).info('Entering listener loop...')
        while not self._stopping:
            self._long_poll()
            time.sleep(15)
        logging.getLogger(__name__).info("Listener stopped!")
        self.stopped = True

    def _listener_p(self, pid):
        pps = psutil.Process(pid=pid)
        logging.getLogger(__name__).info('Entering listener loop...')
        while True:
            try:
                if pps.status() in (psutil.STATUS_DEAD, psutil.STATUS_STOPPED):
                    break
            except psutil.NoSuchProcess:
                break
            self._long_poll()
            time.sleep(15)
        logging.getLogger(__name__).info("Listener stopped!")
        self.stopped = True


def init_logger():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)


if __name__ == '__main__':
    init_logger()

    client = ApolloClient(app_id='pricing-elasticity', cluster='default')
    client.start()
    redis_port = client.get_value("rdfa.redis.port", "db", namespace='redis')
    spring_data_source_hikari_pool = client.get_value("spring.datasource.hikari.pool-name", "db", namespace='db')
    spring_data_source_ucm2_url = client.get_value("spring.datasource.ucm2.url", namespace="application")
    logging.getLogger(__name__).info('redis_port: {}'.format(redis_port))
    logging.getLogger(__name__).info('spring_data_source_hikari_pool: {}'.format(spring_data_source_hikari_pool))
    logging.getLogger(__name__).info('spring_data_source_ucm2_url: {}'.format(spring_data_source_ucm2_url))
    if sys.version_info[0] < 3:
        v = input('Press any key to quit...')
    else:
        v = input('Press any key to quit...')
    client.stop()
    while not client.stopped:
        pass
    exit(0)
