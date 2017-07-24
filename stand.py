import asyncio
import functools
import json
import logging

from docker import Client
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.platform.asyncio import to_asyncio_future

log = logging.getLogger(__name__)


class Stand(object):
    @staticmethod
    async def run_in_tpe(func, *args, **kwargs):
        return await asyncio.get_event_loop().run_in_executor(None, functools.partial(func, *args, **kwargs))

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
        self.stop_task = None
        self.stop_timeout = stop_timeout
        self.lock = asyncio.Lock()

        # обновляемые параметры
        self.db_addr = None
        self.running = None
        self.tomcat_returncode = None
        self.last_task = None
        self.last_error = None
        self.active_task = None
        self.uni_version = None

    async def refresh(self):
        c_inspect = await self.run_in_tpe(self.docker.inspect_container, self.name)
        self.running = c_inspect['State']['Running']
        if self.running:
            try:
                self.test_tools_port = c_inspect['NetworkSettings']['Ports']['8082/tcp'][0]['HostPort']
                self.uni_port = c_inspect['NetworkSettings']['Ports']['8080/tcp'][0]['HostPort']
            except (KeyError, IndexError, TypeError):
                logging.warning('Found container name=%s with unbind ports. Cannot use it', self.name)
                return
            response = await self._test_tool_action('engine_status', 10)
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

    async def _test_tool_action(self, action_name, timeout):
        http_request = 'http://{}:{}/{}?sync=1'.format(self.test_tools_addr, self.test_tools_port, action_name)
        log.debug('Request %s timeout=%s', http_request, timeout)
        cl = AsyncHTTPClient()
        # сразу после запуска контейнера test tools недоступен, Пытаемся соедениться примерно 15 секунд
        try_count = 0
        while try_count < 15:
            try:
                response = await to_asyncio_future(cl.fetch(http_request, request_timeout=timeout))
                # если порт уже прослушивается но сервер не готов отвечать на запросы то вернется пустой ответ
                if response:
                    return response
            # вместо ConnectionError торнадо клиент иногда кидает HTTPError с кодом 599
            except ConnectionError:
                pass
            except HTTPError as e:
                if not e.code == 599:
                    raise e
            await asyncio.sleep(1)
            try_count += 1
        # Если подключиться не удалось то возоможно кто-то остановил контейнер, но что поделать, значит нельзя
        # закончить текущую операцию
        self.running = (await self.run_in_tpe(self.docker.inspect_container, self.name))['State']['Running']
        if not self.running:
            raise RuntimeError('Container %s has stopped unexpectedly' % self.name)
        else:
            raise TimeoutError('Container %s test tools is not available' % self.name)

    async def backup(self):
        log.info('Backup %s started', self.name)
        await self.start()
        await self.refresh()
        # ждем запуска uni чтобы не забэкапить сломанный стенд
        await self._test_tool_action('check_uni', 1000)
        await self._test_tool_action('backup', 10800)
        await self.stop()
        log.info('Backup %s completed', self.name)

    async def update(self):
        log.info('Update %s started', self.name)
        await self.start()
        await self._test_tool_action('build_and_update', 1800)
        await self._test_tool_action('check_uni', 1000)
        await self.stop()
        log.info('Update %s completed', self.name)

    async def backup_and_update(self):
        await self.backup()
        await self.update()

    async def log(self, tail=150):
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
        log_str = await self.run_in_tpe(self.docker.logs, self.name, tail=tail)
        return log_str

    async def stop_by_timeout(self):
        try:
            await asyncio.sleep(30)
            log.info('Stop by timeout %s', self.name)
            with (await self.lock):
                await self._test_tool_action('stop_tomcat', 60)
        except asyncio.CancelledError:
            return

    async def start(self):
        log.info('Start stand %s', self.name)

        # Выключение стенда по таймауту
        if self.stop_task:
            self.stop_task.cancel()

        with (await self.lock):
            if self.stop_timeout:
                self.stop_task = asyncio.get_event_loop().create_task(self.stop_by_timeout())
            await self.run_in_tpe(self.docker.start, self.name)
            await self.refresh()
            await self._test_tool_action('start_tomcat', 30)

    async def stop(self):
        log.info('Stop stand %s', self.name)
        with (await self.lock):
            if self.stop_task:
                self.stop_task.cancel()
            await self._test_tool_action('stop_tomcat', 60)
