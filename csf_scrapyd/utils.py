from scrapyd.utils import JsonSqliteDict
import os


def get_scheduler_persist(config):
    db_path=config.get("dbs_dir","dbs")
    if not os.path.exists(db_path):
        os.makedirs(db_path)

    return JsonSqliteDict(os.sep.join([db_path,"scheduler.db"]))


