import os
import time
import requests
from datetime import datetime, timedelta
from urllib.parse import quote, unquote

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
    plugin_version = "2.6.2"
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

    def __is_valid_file(self, name: str) -> bool:
        return 'ANi' in name

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_current_season_list(self) -> List[Dict]:
        """获取当前季度所有文件"""
        season = self.__get_ani_season()
        base_url = f'https://openani.an-i.workers.dev/{season}/'
        
        try:
            rep = RequestUtils(ua=settings.USER_AGENT, proxies=settings.PROXY).post(url=base_url)
            if rep is None or rep.status_code != 200:
                return []
            
            files_json = rep.json().get('files', [])
            files = []
            
            for file_info in files_json:
                # 直接处理所有项目为文件
                name = unquote(file_info.get('name', ''))
                # 跳过目录项（如果有）
                if name.endswith('/'):
                    continue
                    
                files.append({
                    'name': name,
                    'path': base_url + quote(name),
                    'relative_path': name,
                    'season': season
                })
            return files
        except Exception as e:
            logger.error(f"获取季度列表失败: {base_url}, 错误: {str(e)}")
            return []

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_latest_list(self) -> List[Dict]:
        """获取最新更新的文件列表"""
        addr = 'https://api.ani.rip/ani-download.xml'
        ret = RequestUtils(ua=settings.USER_AGENT, proxies=settings.PROXY).get_res(addr)
        dom_tree = xml.dom.minidom.parseString(ret.text)
        items = dom_tree.documentElement.getElementsByTagName("item")
        result = []
        for item in items:
            title = DomUtils.tag_value(item, "title", default="")
            link = DomUtils.tag_value(item, "link", default="")
            # 替换域名并解码URL
            link = unquote(link.replace("resources.ani.rip", "openani.an-i.workers.dev"))
            # 解析出相对路径
            parsed = link.replace("https://openani.an-i.workers.dev/", "")
            result.append({
                'title': title,
                'link': link,
                'relative_path': parsed
            })
        return result

    def get_all_season_list(self, start_year: int = 2018) -> List[Dict]:
        """获取所有季度的文件"""
        now = datetime.now()
        all_files = []
        
        # 只处理2018-2024年的季度（避免未来季度）
        for year in range(start_year, 2025):
            for month in [1, 4, 7, 10]:
                season = f"{year}-{month}"
                logger.info(f"正在获取季度：{season}")
                
                try:
                    base_url = f'https://openani.an-i.workers.dev/{season}/'
                    rep = RequestUtils(ua=settings.USER_AGENT, proxies=settings.PROXY).post(url=base_url)
                    if rep.status_code != 200:
                        continue
                        
                    files_json = rep.json().get('files', [])
                    for file_info in files_json:
                        name = unquote(file_info.get('name', ''))
                        # 跳过目录项（如果有）
                        if name.endswith('/'):
                            continue
                            
                        all_files.append({
                            'name': name,
                            'path': base_url + quote(name),
                            'relative_path': name,
                            'season': season
                        })
                except Exception as e:
                    logger.warn(f"获取 {season} 季度番剧失败: {e}")
        return all_files

    def __touch_strm_file(self, file_info: Dict, season: str = None) -> bool:
        """创建STRM文件"""
        # 获取文件信息
        file_name = file_info.get('name')
        file_url = file_info.get('path')
        relative_path = file_info.get('relative_path', file_name)
        
        # 确定季度
        season_path = season if season else self._date
        
        # 处理相对路径中的目录部分
        if '/' in relative_path:
            # 移除文件名部分，保留目录路径
            dir_path = os.path.dirname(relative_path)
        else:
            dir_path = ""
        
        # 创建本地存储目录
        full_dir = os.path.join(self._storageplace, season_path, dir_path)
        os.makedirs(full_dir, exist_ok=True)
        
        # 创建STRM文件路径
        file_path = os.path.join(full_dir, f"{file_name}.strm")
        if os.path.exists(file_path):
            logger.debug(f'{file_path} 文件已存在')
            return False
        
        # 添加?d=true参数
        src_url = f"{file_url}?d=true" if '?' not in file_url else f"{file_url}&d=true"
        
        try:
            with open(file_path, 'w') as file:
                file.write(src_url)
                logger.info(f'创建 {file_path} 文件成功')
                return True
        except Exception as e:
            logger.error(f'创建strm文件失败: {str(e)}')
            return False

    def __task(self, fulladd: bool = False, allseason: bool = False):
        cnt = 0
        if allseason:
            files = self.get_all_season_list()
            logger.info(f'处理历史季度，共 {len(files)} 个文件')
            for file in files:
                if not self.__is_valid_file(file['name']):
                    continue
                if self.__touch_strm_file(file, season=file.get('season')):
                    cnt += 1
        elif fulladd:
            files = self.get_current_season_list()
            logger.info(f'本次处理 {len(files)} 个文件')
            for file in files:
                if not self.__is_valid_file(file['name']):
                    continue
                if self.__touch_strm_file(file):
                    cnt += 1
        else:
            files = self.get_latest_list()
            logger.info(f'本次处理 {len(files)} 个文件')
            for file in files:
                if not self.__is_valid_file(file['title']):
                    continue
                # 构建文件信息字典
                file_info = {
                    'name': file['title'],
                    'path': file['link'],
                    'relative_path': file['relative_path']
                }
                if self.__touch_strm_file(file_info):
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
