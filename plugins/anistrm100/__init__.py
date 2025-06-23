import os
import time
from datetime import datetime, timedelta

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.utils.http import RequestUtils
from app.core.config import settings
from app.plugins import _PluginBase
from typing import Any, List, Dict, Tuple, Optional
from app.log import logger
import xml.dom.minidom
from app.utils.dom import DomUtils

def retry(ExceptionToCheck: Any,
          tries: int = 3, delay: int = 3, backoff: int = 1, logger: Any = None, ret: Any = None):
    def deco_retry(f):
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 0:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = f"未获取到文件信息，{mdelay}秒后重试 ..."
                    if logger:
                        logger.warn(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            if logger:
                logger.warn('请确保当前季度番剧文件夹存在或检查网络问题')
            return ret

        return f_retry
    return deco_retry

class ANiStrm100(_PluginBase):
    plugin_name = "ANiStrm100"
    plugin_desc = "自动获取当季所有番剧，免去下载，轻松拥有一个番剧媒体库"
    plugin_icon = "https://raw.githubusercontent.com/honue/MoviePilot-Plugins/main/icons/anistrm.png"
    plugin_version = "2.4.3"
    plugin_author = "GlowsSama"
    author_url = "https://github.com/honue"
    plugin_config_prefix = "anistrm100_"
    plugin_order = 15
    auth_level = 2

    _enabled = False
    _cron = None
    _onlyonce = False
    _fulladd = False
    _allseason = False
    _storageplace = None

    _scheduler: Optional[BackgroundScheduler] = None

    def init_plugin(self, config: dict = None):
        self.stop_service()
        if config:
            self._enabled = config.get("enabled")
            self._cron = config.get("cron")
            self._onlyonce = config.get("onlyonce")
            self._fulladd = config.get("fulladd")
            self._allseason = config.get("allseason")
            self._storageplace = config.get("storageplace")
        if self._enabled or self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            if self._enabled and self._cron:
                try:
                    self._scheduler.add_job(func=self.__task,
                                            trigger=CronTrigger.from_crontab(self._cron),
                                            name="ANiStrm100文件创建")
                    logger.info(f'ANi-Strm定时任务创建成功：{self._cron}')
                except Exception as err:
                    logger.error(f"定时任务配置错误：{str(err)}")
            if self._onlyonce:
                logger.info(f"ANi-Strm服务启动，立即运行一次")
                self._scheduler.add_job(func=self.__task,
                                        args=[self._fulladd, self._allseason],
                                        trigger='date',
                                        run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                        name="ANiStrm100文件创建")
                self._onlyonce = False
                self._fulladd = False
                self._allseason = False
            self.__update_config()
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def __get_ani_season(self, idx_month: int = None) -> str:
        current_date = datetime.now()
        current_year = current_date.year
        current_month = idx_month if idx_month else current_date.month
        for month in range(current_month, 0, -1):
            if month in [10, 7, 4, 1]:
                self._date = f'{current_year}-{month}'
                return f'{current_year}-{month}'

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_current_season_list(self) -> List[str]:
        url = f'https://openani.an-i.workers.dev/{self.__get_ani_season()}/'
        rep = RequestUtils(ua=settings.USER_AGENT, proxies=settings.PROXY).post(url=url)
        logger.debug(rep.text)
        files_json = rep.json().get('files', [])
        return [file['name'] for file in files_json]

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_latest_list(self) -> List:
        addr = 'https://api.ani.rip/ani-download.xml'
        ret = RequestUtils(ua=settings.USER_AGENT, proxies=settings.PROXY).get_res(addr)
        dom_tree = xml.dom.minidom.parseString(ret.text)
        items = dom_tree.documentElement.getElementsByTagName("item")
        result = []
        for item in items:
            title = DomUtils.tag_value(item, "title", default="")
            link = DomUtils.tag_value(item, "link", default="")
            result.append({
                'title': title,
                'link': link.replace("resources.ani.rip", "openani.an-i.workers.dev")
            })
        return result

    def get_all_season_list(self, start_year: int = 2018) -> List[Tuple[str, str]]:
        now = datetime.now()
        all_files = []
        for year in range(start_year, now.year + 1):
            for month in [1, 4, 7, 10]:
                season = f"{year}-{month}"
                logger.info(f"正在获取季度：{season}")
                try:
                    url = f'https://openani.an-i.workers.dev/{season}/'
                    rep = RequestUtils(ua=settings.USER_AGENT, proxies=settings.PROXY).post(url=url)
                    files_json = rep.json().get('files', [])
                    for file in files_json:
                        name = file.get('name')
                        if name:
                            all_files.append((season, name))
                except Exception as e:
                    logger.warn(f"获取 {season} 季度番剧失败: {e}")
        return all_files

def __touch_strm_file(self, file_name, file_url: str = None, season: str = None) -> bool:
    season_path = season if season else self._date
    src_url = file_url if file_url else f'https://openani.an-i.workers.dev/{season_path}/{file_name}?d=true'

    # ✅ 自动创建季度子目录
    dir_path = os.path.join(self._storageplace, season_path)
    os.makedirs(dir_path, exist_ok=True)

    file_path = os.path.join(dir_path, f'{file_name}.strm')
    if os.path.exists(file_path):
        logger.debug(f'{file_name}.strm 文件已存在')
        return False
    try:
        with open(file_path, 'w') as file:
            file.write(src_url)
            logger.debug(f'创建 {season_path}/{file_name}.strm 文件成功')
            return True
    except Exception as e:
        logger.error('创建strm源文件失败：' + str(e))
        return False

    def __task(self, fulladd: bool = False, allseason: bool = False):
        cnt = 0
        if allseason:
            name_list = self.get_all_season_list()
            logger.info(f'处理历史季度，共 {len(name_list)} 个文件')
            for season, file_name in name_list:
                if self.__touch_strm_file(file_name=file_name, season=season):
                    cnt += 1
        elif fulladd:
            name_list = self.get_current_season_list()
            logger.info(f'本次处理 {len(name_list)} 个文件')
            for file_name in name_list:
                if self.__touch_strm_file(file_name=file_name):
                    cnt += 1
        else:
            rss_info_list = self.get_latest_list()
            logger.info(f'本次处理 {len(rss_info_list)} 个文件')
            for rss_info in rss_info_list:
                if self.__touch_strm_file(file_name=rss_info['title'], file_url=rss_info['link']):
                    cnt += 1
        logger.info(f'新创建了 {cnt} 个strm文件')

    def get_state(self) -> bool:
        return self._enabled

    def get_command(self) -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': '立即运行一次'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VSwitch', 'props': {'model': 'fulladd', 'label': '创建当季所有番剧strm'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VSwitch', 'props': {'model': 'allseason', 'label': '创建历史所有季度番剧strm'}}]}
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'cron', 'label': '执行周期', 'placeholder': '0 0 ? ? ?'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'storageplace', 'label': 'Strm存储地址', 'placeholder': '/downloads/strm'}}]}
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "fulladd": False,
            "allseason": False,
            "storageplace": "/downloads/strm",
            "cron": "*/20 22,23,0,1 * * *",
        }

    def __update_config(self):
        self.update_config({
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "enabled": self._enabled,
            "fulladd": self._fulladd,
            "allseason": self._allseason,
            "storageplace": self._storageplace,
        })

    def get_page(self) -> List[dict]:
        pass

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error("退出插件失败：%s" % str(e))

if __name__ == "__main__":
    anistrm100 = ANiStrm100()
    name_list = anistrm100.get_all_season_list()
    print(name_list)
