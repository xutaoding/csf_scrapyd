import sys
from datetime import datetime
from multiprocessing import cpu_count

from twisted.internet import reactor, defer, protocol, error
from twisted.application.service import Service
from twisted.python import log

from scrapy.utils.python import stringify_dict
from scrapyd.utils import get_crawl_args
from scrapyd import __version__
from scrapyd.interfaces import IPoller, IEnvironment, ISpiderScheduler
from utils import get_scheduler_persist
from scrapyd.utils import get_spider_queues


class Launcher(Service):
    name = 'launcher'

    def __init__(self, config, app):
        self.processes = {}
        self.finished = []
        self.finished_to_keep = config.getint('finished_to_keep', 100)
        self.max_proc = self._get_max_proc(config)
        self.runner = config.get('runner', 'scrapyd.runner')
        self.app = app
        self.scheduler_persist = get_scheduler_persist(config)
        self.spider_queue = get_spider_queues(config)

        self.scheduler = self.app.getComponent(ISpiderScheduler)

    def startService(self):
        for project in self.spider_queue:
            self.spider_queue[project].clear()

        for project in self.scheduler_persist:
            proj = self.scheduler_persist[project]
            spider = proj["spider"]
            args = proj["args"]
            self.scheduler.schedule(project, spider, **args)

        for slot in range(self.max_proc):
            self._wait_for_project(slot)
            log.msg(format='Scrapyd %(version)s started: max_proc=%(max_proc)r, runner=%(runner)r',
                    version=__version__,
                    max_proc=self.max_proc,
                    runner=self.runner,
                    system='Launcher')

    def _wait_for_project(self, slot):
        poller = self.app.getComponent(IPoller)
        poller.next().addCallback(self._spawn_process, slot)

    def _spawn_process(self, message, slot):
        msg = stringify_dict(message, keys_only=False)
        project = msg['_project']
        args = [sys.executable, '-m', self.runner, 'crawl']
        args += get_crawl_args(msg)
        e = self.app.getComponent(IEnvironment)

        env = e.get_environment(msg, slot)
        env = stringify_dict(env, keys_only=False)

        pp = ScrapyProcessProtocol(slot, project, msg['_spider'], msg['_job'], env)
        pp.deferred.addBoth(self._process_finished, slot)
        pp.msg = msg

        reactor.spawnProcess(pp, sys.executable, args=args, env=env)
        self.processes[slot] = pp

    def _process_finished(self, _, slot):
        process = self.processes.pop(slot)

        if process.msg.__contains__("loop"):
            loop = int(process.msg["loop"])
            if 0 == loop or loop > 1:
                if loop > 1: loop -= 1
                process.msg["loop"] = str(loop)
                self.scheduler.schedule(process.project, process.spider, **process.msg)

        process.end_time = datetime.now()
        self.finished.append(process)
        del self.finished[:-self.finished_to_keep]  # keep last 100 finished jobs
        self._wait_for_project(slot)

    def _get_max_proc(self, config):
        max_proc = config.getint('max_proc', 0)
        if not max_proc:
            try:
                cpus = cpu_count()
            except NotImplementedError:
                cpus = 1
            max_proc = cpus * config.getint('max_proc_per_cpu', 4)
        return max_proc


class ScrapyProcessProtocol(protocol.ProcessProtocol):
    def __init__(self, slot, project, spider, job, env):
        self.slot = slot
        self.pid = None
        self.project = project
        self.spider = spider
        self.job = job
        self.start_time = datetime.now()
        self.end_time = None
        self.env = env
        self.logfile = env.get('SCRAPY_LOG_FILE')
        self.itemsfile = env.get('SCRAPY_FEED_URI')
        self.deferred = defer.Deferred()

    def outReceived(self, data):
        log.msg(data.rstrip(), system="Launcher,%d/stdout" % self.pid)

    def errReceived(self, data):
        log.msg(data.rstrip(), system="Launcher,%d/stderr" % self.pid)

    def connectionMade(self):
        self.pid = self.transport.pid
        self.log("Process started: ")

    def processEnded(self, status):
        if isinstance(status.value, error.ProcessDone):
            self.log("Process finished: ")
            self.deferred.callback(self)
        else:
            self.log("Process died: exitstatus=%r " % status.value.exitCode)
            self.deferred.errback(self)

    def log(self, action):
        fmt = '%(action)s project=%(project)r spider=%(spider)r job=%(job)r pid=%(pid)r log=%(log)r items=%(items)r'
        log.msg(format=fmt, action=action, project=self.project, spider=self.spider,
                job=self.job, pid=self.pid, log=self.logfile, items=self.itemsfile)