import uuid
from scrapyd.utils import get_spider_list
from scrapyd.webservice import WsResource


class DeleteSchedulePersist(WsResource):
    def render_POST(self, txrequest):
        args = dict((k, v[0]) for k, v in txrequest.args.items())
        project = args.pop('project')

        if not self.root.scheduler_persist.__contains__(project):
            return {"status": "error", "message": "project '%s' not found" % project}

        self.root.scheduler_persist.pop(project)

        return {"node_name": self.root.nodename, "status": "ok", "project": project}


class Schedule(WsResource):
    def render_POST(self, txrequest):
        settings = txrequest.args.pop('setting', [])
        settings = dict(x.split('=', 1) for x in settings)
        args = dict((k, v[0]) for k, v in txrequest.args.items())
        project = args.pop('project')
        spider = args.pop('spider')
        version = args.get('_version', '')
        spiders = get_spider_list(project, version=version)
        if not spider in spiders:
            return {"status": "error", "message": "spider '%s' not found" % spider}
        args['settings'] = settings
        jobid = args.pop('jobid', uuid.uuid1().hex)
        args['_job'] = jobid

        self.root.scheduler.schedule(project, spider, **args)
        self.root.scheduler_persist[project] = {"spider": spider, "args": args}

        return {"node_name": self.root.nodename, "status": "ok", "jobid": jobid}
