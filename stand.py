import json
import logging
from concurrent.futures import ThreadPoolExecutor

from docker import Client
from tornado import gen, locks
from tornado.httpclient import AsyncHTTPClient, HTTPError

log = logging.getLogger(__name__)


class Stand(object):
    TPE = ThreadPoolExecutor(max_workers=4)

    def __init__(self, name, test_tools_addr, stop_timeout):
        # параметр для управления контейнером
        self.name = name
        # параметры для коммуникации с test-tools внутри контейнера
        self.test_tools_addr = test_tools_addr
        self.test_tools_port = None
        self.uni_port = None

        # динамические параметры этого приложения
        self.queue = None
        self.docker = Client(base_url='unix:///var/run/docker.sock')
        self.stop_future = None
        self.stop_timeout = stop_timeout
        self.lock = locks.Lock()

        # обновляемые параметры
        self.db_addr = None
        self.running = None
        self.tomcat_returncode = None
        self.last_task = None
        self.last_error = None
        self.active_task = None
        self.uni_version = None

    @gen.coroutine
    def refresh(self):
        c_inspect = yield self.TPE.submit(self.docker.inspect_container, self.name)
        self.running = c_inspect['State']['Running']
        if self.running:
            try:
                self.test_tools_port = c_inspect['NetworkSettings']['Ports']['8082/tcp'][0]['HostPort']
                self.uni_port = c_inspect['NetworkSettings']['Ports']['8080/tcp'][0]['HostPort']
            except (KeyError, IndexError, TypeError):
                logging.warning('Found container name=%s with unbind ports. Cannot use it', self.name)
                return
            response = yield self._test_tool_action('engine_status', 10)
            engine_status = json.loads(response.body.decode('utf8'))
            self.db_addr = engine_status['db_addr']
            self.tomcat_returncode = engine_status['tomcat_returncode']
            self.last_task = engine_status['last_task']
            self.last_error = engine_status['last_error']
            self.active_task = engine_status['active_task']
            self.uni_version = engine_status['uni_version']
        else:
            self.tomcat_returncode = '-'
            self.last_task = '-'
            self.last_error = '-'
            self.active_task = '-'
            self.uni_version = '-'

    @gen.coroutine
    def _test_tool_action(self, action_name, timeout):
        http_request = 'http://{}:{}/{}?sync=1'.format(self.test_tools_addr, self.test_tools_port, action_name)
        log.debug('Request %s timeout=%s', http_request, timeout)
        cl = AsyncHTTPClient()
        # сразу после запуска контейнера test tools недоступен, Пытаемся соедениться примерно 15 секунд
        try_count = 0
        while try_count < 15:
            try:
                response = yield cl.fetch(http_request, request_timeout=timeout)
                # если порт уже прослушивается но сервер не готов отвечать на запросы то вернется пустой ответ
                if response:
                    return response
            # вместо ConnectionError торнадо клиент иногда кидает HTTPError с кодом 599
            except ConnectionError:
                pass
            except HTTPError as e:
                if not e.code == 599:
                    raise e
            yield gen.sleep(1)
            try_count += 1
        # Если подключиться не удалось то возоможно кто-то остановил контейнер, но что поделать, значит нельзя
        # закончить текущую операцию
        self.running = yield self.TPE.submit(self.docker.inspect_container, self.name)['State']['Running']
        if not self.running:
            raise RuntimeError('Container %s has stopped unexpectedly' % self.name)
        else:
            raise TimeoutError('Container %s test tools is not available' % self.name)

    @gen.coroutine
    def backup(self):
        log.info('Backup %s started', self.name)
        yield self.start()
        yield self.refresh()
        # ждем запуска uni чтобы не забэкапить сломанный стенд
        yield self._test_tool_action('check_uni', 1000)
        yield self._test_tool_action('backup', 10800)
        yield self.stop()
        log.info('Backup %s completed', self.name)

    @gen.coroutine
    def update(self):
        log.info('Update %s started', self.name)
        yield self.start()
        yield self._test_tool_action('build_and_update', 1800)
        yield self._test_tool_action('check_uni', 1000)
        yield self.stop()
        log.info('Update %s completed', self.name)

    @gen.coroutine
    def backup_and_update(self):
        yield self.backup()
        yield self.update()

    @gen.coroutine
    def log(self, tail=150):
        """
        Получить логи стенда
        :param tail: колличество строк с конца
        :return: str
        """
        log.debug('Read log file for stand %s', self.name)
        if tail != 'all':
            try:
                tail = int(tail)
            except ValueError:
                tail = 150
        log_str = yield self.TPE.submit(self.docker.logs, self.name, tail=tail)
        return log_str

    @gen.coroutine
    def stop_by_timeout(self, future):
        # Tornado ``Futures`` do not support cancellation at current version
        # if name in sm.stands_futures:
        #     sm.stands_futures[name].cancel()
        # Торнадовская футура передаст self в callback
        if future is self.stop_future:
            log.info('Stop by timeout %s', self.name)
            with (yield self.lock.acquire()):
                yield self.stop()

    @gen.coroutine
    def start(self):
        log.info('Start stand %s', self.name)
        # Выключение стенда по таймауту
        if self.stop_timeout:
            future = gen.sleep(self.stop_timeout * 60)
            future.add_done_callback(self.stop_by_timeout)
            self.stop_future = future

        yield self.TPE.submit(self.docker.start, self.name)
        yield self.refresh()
        yield self._test_tool_action('start_tomcat', 30)

    @gen.coroutine
    def stop(self):
        log.info('Stop stand %s', self.name)
        self.stop_future = None
        yield self._test_tool_action('stop_tomcat', 60)
