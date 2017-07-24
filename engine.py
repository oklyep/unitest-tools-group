import asyncio
import logging
from typing import Dict, Tuple, List

from docker import Client
from tornado.httpclient import HTTPError

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
        asyncio.get_event_loop().create_task(self.stands())

    async def _queue_worker(self, queue):
        log.debug('start new queue worker')
        while True:
            func, lock, name = await queue.get()
            log.debug('start task %s', name)
            try:
                with (await lock):
                    await func()
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

    def queues_status(self):
        return {name: [v[2] for v in list(q._queue)] for name, q in self.queues.items()}

    async def stands(self):
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
            ps_result = await asyncio.wait_for(
                Stand.run_in_tpe(self.docker.containers, all=True, filters=self.filter), 5)
        except asyncio.TimeoutError:
            log.debug('Cannot update stand list. Timeout in docker request')
            return self._stands

        # отсекаю слеш в начале имени
        name_list = {record['Names'][0][1:] for record in ps_result}

        # удаляю информацию о контейнерах которые перестали существовать
        for name in self._stands.keys() - name_list:
            del self._stands[name]

        # добавляю новые
        for name in name_list - self._stands.keys():
            s = Stand(name=name,
                      test_tools_addr=self.domain_name,
                      stop_timeout=self.stop_timeout)

            # Перед тем как передать управление отметим что с этим именем уже работаем
            self._stands[name] = s

            with (await s.lock):
                # Если стенд не запущен, то нужно его запустить чтобы опросить на тему, какой сервер баз он использует
                await s.refresh()
                if not s.running:
                    await s.start()

                try:
                    # если уже есть очередь для этого сервера баз данных, то "записываем" стенд в эту очередь
                    s.queue = self.queues[s.db_addr]
                except KeyError:
                    # если очереди нет, создаем новую
                    q = asyncio.Queue(maxsize=100)
                    s.queue = q
                    asyncio.get_event_loop().create_task(self._queue_worker(q))
                    self.queues[s.db_addr] = q

        return self._stands

    async def refresh_all(self):
        ss = await self.stands()
        futures = [s.refresh() for s in ss.values()]
        if futures:  # иначе ValueError: Set of coroutines/Futures is empty если нет стендов.
            await asyncio.wait(futures)
        return ss

    async def _free_resources(self):
        """
        Можно ли запустить еще один стенд
        """
        c = 0
        ss = await self.refresh_all()
        for s in ss.values():
            if s.running and s.tomcat_returncode is None:
                c += 1
        if c >= self.max_active_stands:
            raise RuntimeError('No resources')

    async def _stand_with_validate(self, name):
        try:
            stands = await self.stands()
            s = stands[name]
            assert isinstance(s, Stand)
            return s
        except KeyError:
            raise RuntimeError('Stand is not exists')

    async def log(self, name, tail):
        """
        Получить логи стенда
        :param tail: колличество строк с конца
        """

        s = await self._stand_with_validate(name)
        log_str = await s.log(tail)
        return log_str

    async def stop(self, name):
        """
ё       Остановить стенд
        """
        s = await self._stand_with_validate(name)
        with (await s.lock):
            await s.stop()
        return 'Done'

    async def start(self, name):
        """
        Запустить стенд
        """
        await self._free_resources()
        s = await self._stand_with_validate(name)
        with (await s.lock):
            await s.start()
        return 'Done'

    async def backup_all(self):
        """
        Создать бэкап базы данных всех стендов
        """
        log.info('Backup all stands')
        for s in (await self.stands()).values():
            assert isinstance(s, Stand)
            log.info('New task: backup, stand: %s', s.name)
            with(await s.lock):
                await s.queue.put((s.backup, s.lock, 'backup {}'.format(s.name)))

    async def update_all(self):
        """
        Обновить все стенды
        """
        log.info('Backup all stands')
        for s in (await self.stands()).values():
            assert isinstance(s, Stand)
            log.info('New task: update, stand: %s', s.name)
            # возможно в этот момент для стенда создется очередь
            with(await s.lock):
                await s.queue.put((s.update, s.lock, 'update {}'.format(s.name)))

    async def backup_and_update(self):
        """
        Создать бэкапы всех стендов и обновить. Если во время бэкапа произошла ошибка, то обновление не происходит
        """
        log.info('Backup and update all stands')
        for s in (await self.stands()).values():
            assert isinstance(s, Stand)

            log.info('New task: backup and update, stand: %s', s.name)
            with(await s.lock):
                await s.queue.put((s.backup_and_update, s.lock, 'backup_and_update {}'.format(s.name)))
