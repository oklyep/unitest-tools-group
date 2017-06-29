import logging
import os

from tornado import gen
from tornado.web import RequestHandler

from stand import Stand

log = logging.getLogger('engine')


class MainPageHandler(RequestHandler):
    with open(os.path.join(os.path.dirname(__file__), 'html', 'main_page.html')) as f:
        PAGE_TEMPLATE = f.read()

    with open(os.path.join(os.path.dirname(__file__), 'html', 'stand.html')) as f:
        CONTENT_TEMPLATE = f.read()

    @gen.coroutine
    def get(self):
        content = ''
        engine = self.application.engine

        ss = yield engine.refresh_all()
        names = list(ss.keys())
        names.sort()

        for name in names:
            s = ss[name]
            # ss.values() возвращает не в алфавитном порядке
            assert isinstance(s, Stand)
            # 143 application was terminated due to a SIGTERM command (стандартная остановка процесса)
            # 137 Script terminated by kill signal (например неожиданно убит докер полным выключением питания)
            # -15 A negative value -N indicates that the child was terminated by signal N (POSIX only).
            # 1 error
            if not s.running:
                tomcat_status = '-'
            elif s.tomcat_returncode in (0, 143, -15):
                tomcat_status = 'Остановлен (корректно)'
            elif s.tomcat_returncode == 137:
                tomcat_status = 'Остановлен (принудительно)'
            elif s.tomcat_returncode is None:
                tomcat_status = 'Работает'
            else:
                tomcat_status = 'Ошибка (returncode {})'.format(s.tomcat_returncode)
            content += self.CONTENT_TEMPLATE.format(name=s.name,
                                                    container_status='запущен' if s.running else 'остановлен',
                                                    tomcat_status=tomcat_status,
                                                    active_task=s.active_task or 'нет',
                                                    last_task=s.last_task or 'нет',
                                                    last_error=s.last_error or 'нет',
                                                    domain_name=engine.domain_name,
                                                    test_tools_port=s.test_tools_port,
                                                    uni_version=s.uni_version,
                                                    uni_port=s.uni_port)
        self.finish(self.PAGE_TEMPLATE.format(content=content or 'Стенды не найдены'))


class MassActionHandler(RequestHandler):
    ACTIONS = ('update_all', 'backup_all', 'backup_and_update')
    QUEUES_STATUS = 'queues_status'

    @gen.coroutine
    def get(self, action):
        engine = self.application.engine
        if action in self.ACTIONS:
            for q in engine.queues.values():
                if not q.empty():
                    self.finish('Busy with another mass task')
                    return

            output = yield getattr(engine, action)()
            self.finish(output)
            return

        if action == self.QUEUES_STATUS:
            self.finish(engine.queues_status())
            return

        self.set_status(404, 'invalid action')
        self.finish('invalid action')


class ContainerActions(RequestHandler):
    ACTIONS = ('start', 'stop')
    LOG = 'log'

    with open(os.path.join(os.path.dirname(__file__), 'html', 'log.html')) as f:
        LOG_TEMPLATE = f.read()

    @gen.coroutine
    def get(self, name, action):
        engine = self.application.engine
        try:
            if action == self.LOG:
                tail = self.get_argument('tail', '150')
                output = yield engine.log(name=name, tail=tail)
                self.finish(self.LOG_TEMPLATE.format(content=output.decode()))
                return

            if action in self.ACTIONS:
                output = yield getattr(engine, action)(name=name)
                self.finish(output)
                return

        except Exception as e:
            self.set_status(400)
            self.finish(str(e))
            return

        self.set_status(404, 'invalid action')
        self.finish('invalid action')


class AdminPageHandler(RequestHandler):
    with open(os.path.join(os.path.dirname(__file__), 'html', 'admin_page.html')) as f:
        HTML_TEMPLATE = f.read()

    def get(self):
        self.finish(self.HTML_TEMPLATE)
