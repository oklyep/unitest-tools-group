import datetime
import logging

from docker import Client
from requests.exceptions import ReadTimeout
from tornado import gen
from tornado.httpclient import HTTPError
from tornado.ioloop import IOLoop
from tornado.queues import Queue

from stand import Stand

log = logging.getLogger(__name__)


class Engine(object):
    def __init__(self, domain_name, image, max_active_stands, stop_timeout):
        log.info('Start engine')
        self.domain_name = domain_name
        self.max_active_stands = max_active_stands
        self.stop_timeout = int(stop_timeout)

        self.docker = Client(base_url='unix:///var/run/docker.sock')
        self.filter = {"ancestor": image}
        self._stands = {}
        self.queues = {}
        self.stands()

    def queues_status(self):
        return {name: [v[2] for v in list(q._queue)] for name, q in self.queues.items()}

    @gen.coroutine
    def _queue_worker(self, queue):
        while True:
            runnable, lock, name = yield queue.get()
            try:
                with (yield lock.acquire()):
                    yield runnable()
            except Exception as e:
                # 400 - ошибки связанные с инфраструктурой и ошибки после обновления тестовых сборок,
                # т. е. штатные ситуации
                if isinstance(e, HTTPError) and e.code == 400:
                    log.error('Error while task %s', name)
                    log.error(e)
                else:
                    log.exception(e)
            finally:
                queue.task_done()

    @gen.coroutine
    def stands(self):
        """
        Создает список контейнеров, открытых из указанных images на этом докер-хосте.
        Узнает у контейнеров имя базы с которой они работают
        Если контейнер найден в списке впервые то опрашивает его на тему какая база
        """
        # Этот блок обходит баг докера https://github.com/moby/moby/issues/29058
        # Если докер не ответил за 5 секунд, значит он вероятно занят копированием.
        # в случае если с последнего вызова изменился список стендов (добавлены, удалены) то эти изменения
        # не будут показаны пользователю. quiet_exceptions - не логировать это исключение.
        # Им закончится выполнение футуры когда докер "очнется"
        try:
            ps_result = yield gen.with_timeout(datetime.timedelta(seconds=5),
                                               Stand.TPE.submit(self.docker.containers, all=True, filters=self.filter),
                                               quiet_exceptions=(ReadTimeout))
        except gen.TimeoutError:
            log.debug('Cannot update stand list. Timeout in docker request')
            return self._stands

        # отсекаю слеш в начале имени
        name_list = [record['Names'][0][1:] for record in ps_result]

        # удаляю информацию о контейнерах которые перестали существовать
        for current_name in list(self._stands):
            if current_name not in name_list:
                del self._stands[current_name]

        for name in name_list:
            if name in self._stands:
                continue

            s = Stand(name=name,
                      test_tools_addr=self.domain_name,
                      stop_timeout=self.stop_timeout)
            self._stands[name] = s
            with (yield s.lock.acquire()):
                # Если стенд не запущен, то нужно его запустить чтобы опросить на тему, какой сервер баз он использует
                yield s.refresh()
                if not s.running:
                    yield s.start()
                    yield s.stop()

                try:
                    # если уже есть очередь для этого сервера баз данных, то "записываем" стенд в эту очередь
                    s.queue = self.queues[s.db_addr]
                except KeyError:
                    # если очереди нет, создаем новую
                    q = Queue(maxsize=100)
                    s.queue = q
                    IOLoop.current().spawn_callback(self._queue_worker, q)
                    self.queues[s.db_addr] = q

        return self._stands

    @gen.coroutine
    def refresh_all(self):
        ss = yield self.stands()
        yield gen.multi([s.refresh() for s in ss.values()])
        return ss

    @gen.coroutine
    def _free_resources(self):
        """
        Можно ли запустить еще один стенд
        """
        c = 0
        ss = yield self.refresh_all()
        for s in ss.values():
            if s.running and s.tomcat_returncode is None:
                c += 1
        if c >= self.max_active_stands:
            raise RuntimeError('No resources')

    @gen.coroutine
    def _stand_with_validate(self, name):
        try:
            stands = yield self.stands()
            s = stands[name]
            assert isinstance(s, Stand)
            return s
        except KeyError:
            raise RuntimeError('Stand is not exists')

    @gen.coroutine
    def log(self, name, tail):
        """
        Получить логи стенда
        :param tail: колличество строк с конца
        """

        s = yield self._stand_with_validate(name)
        log_str = yield s.log(tail)
        return log_str

    @gen.coroutine
    def stop(self, name):
        """
ё       Остановить стенд
        """
        s = yield self._stand_with_validate(name)
        with (yield s.lock.acquire()):
            yield s.stop()
        return 'Done'

    @gen.coroutine
    def start(self, name):
        """
        Запустить стенд
        """
        self._free_resources()
        s = yield self._stand_with_validate(name)
        with (yield s.lock.acquire()):
            yield s.start()
        return 'Done'

    @gen.coroutine
    def backup_all(self):
        """
        Создать бэкап базы данных всех стендов
        """
        log.info('Backup all stands')
        ss = yield self.stands()
        for s in ss.values():
            assert isinstance(s, Stand)
            log.info('New task: backup, stand: %s', s.name)
            with(yield s.lock.acquire()):
                s.queue.put((s.backup, s.lock, 'backup {}'.format(s.name)))

    @gen.coroutine
    def update_all(self):
        """
        Обновить все стенды
        """
        log.info('Backup all stands')
        ss = yield self.stands()
        for s in ss.values():
            assert isinstance(s, Stand)
            log.info('New task: update, stand: %s', s.name)
            # возможно в этот момент для стенда создется очередь
            with(yield s.lock.acquire()):
                s.queue.put((s.update, s.lock, 'update {}'.format(s.name)))

    @gen.coroutine
    def backup_and_update(self):
        """
        Создать бэкапы всех стендов и обновить. Если во время бэкапа произошла ошибка, то обновление не происходит
        """
        log.info('Backup and update all stands')
        ss = yield self.stands()
        for s in ss.values():
            assert isinstance(s, Stand)

            log.info('New task: backup and update, stand: %s', s.name)
            with(yield s.lock.acquire()):
                s.queue.put((s.backup_and_update, s.lock, 'backup_and_update {}'.format(s.name)))
