import os
import time
from datetime import datetime, timedelta
import re
import shutil
import tempfile
from urllib.parse import urlparse, unquote
import urllib.parse

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

# ---------------- 重试装饰器 ----------------
def retry(ExceptionToCheck: Any, tries: int = 3, delay: int = 3, backoff: int = 1, logger: Any = None, ret: Any = None):
    def deco_retry(f):
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 0:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = f"发生错误，将在 {mdelay} 秒后重试... 错误详情: {e}"
                    if logger:
                        logger.warn(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            if logger:
                logger.warn('多次重试后仍然失败。请检查文件夹是否存在或网络问题。')
            return ret
        return f_retry
    return deco_retry

# ---------------- 插件主类 ----------------
class ANiStrm100(_PluginBase):
    plugin_name = "ANiStrm100"
    plugin_desc = "自动获取当季所有番剧，免去下载，轻松拥有一个番剧媒体库"
    plugin_icon = "https://raw.githubusercontent.com/honue/MoviePilot-Plugins/main/icons/anistrm.png"
    plugin_version = "3.2.3" # <<< 修改：版本更新
    plugin_author = "honue,GlowsSama"
    author_url = "https://github.com/GlowsSama"
    plugin_config_prefix = "anistrm100_"
    plugin_order = 15
    auth_level = 2

    _enabled = False
    _cron = None
    _onlyonce = False
    _fulladd = False
    _allseason = False
    _storageplace = None
    _overwrite = False
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
            self._overwrite = config.get("overwrite", False)

        if self._enabled or self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            if self._enabled and self._cron:
                try:
                    self._scheduler.add_job(
                        func=self.__task,
                        trigger=CronTrigger.from_crontab(self._cron),
                        name="ANiStrm100文件创建"
                    )
                    logger.info(f'ANi-Strm定时任务创建成功：{self._cron}')
                except Exception as err:
                    logger.error(f"定时任务配置错误：{str(err)}")

            if self._onlyonce:
                logger.info("ANi-Strm服务启动，立即运行一次")
                self._scheduler.add_job(
                    func=self.__task,
                    args=[self._fulladd, self._allseason],
                    trigger='date',
                    run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                    name="ANiStrm100文件创建"
                )
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

        if 1 <= current_month <= 3:
            season_month = 1
        elif 4 <= current_month <= 6:
            season_month = 4
        elif 7 <= current_month <= 9:
            season_month = 7
        else:
            season_month = 10

        self._date = f'{current_year}-{season_month}'
        return self._date

    def __is_valid_file(self, name: str) -> bool:
        return 'ANi' in name

    @retry(Exception, tries=3, logger=logger, ret=[])
    def __traverse_directory(self, path_parts: List[str]) -> List[Tuple[str, List[str], str]]:
        all_files = []
        current_path_str = "/".join(path_parts)
        url = f'https://ani.v300.eu.org/{current_path_str}/'

        logger.debug(f"正在遍历: {url}")
        rep = RequestUtils(ua=settings.USER_AGENT, proxies=settings.PROXY).post(url=url)
        if rep and hasattr(rep, 'json'):
            items = rep.json().get('files', [])
        else:
            logger.warn(f"无法获取有效的响应或响应无json方法，URL: {url}")
            items = []

        base_folder = path_parts[0]
        sub_path_list = path_parts[1:]

        for item in items:
            item_name = item.get('name')
            if not item_name: continue

            if self.__is_valid_file(item_name):
                all_files.append((base_folder, sub_path_list, item_name))
            elif '.' not in item_name:
                all_files.extend(self.__traverse_directory(path_parts + [item_name]))

        return all_files

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_latest_list(self) -> List:
        addr = 'https://aniapi.v300.eu.org/ani-download.xml'
        logger.info(f"正在尝试从 RSS 源获取最新文件: {addr}")
        ret = RequestUtils(ua=settings.USER_AGENT, proxies=settings.PROXY).get_res(addr)
        
        if not (ret and hasattr(ret, 'text')):
            logger.warn(f"无法获取有效的RSS响应或响应无text属性，URL: {addr}。")
            return []

        dom_tree = xml.dom.minidom.parseString(ret.text)
        items = dom_tree.documentElement.getElementsByTagName("item")
        result = []
        for item in items:
            link = DomUtils.tag_value(item, "link", default="").strip()

            if not link or not link.startswith(('http://', 'https://')):
                continue

            if link.endswith('?d=mp4'):
                link = link.removesuffix('?d=mp4') + '?d=true'
                logger.debug(f"修正了错误的URL后缀: {link}")

            if link.endswith('?d=true') and not link.endswith('.mp4?d=true'):
                link = link.removesuffix('?d=true') + '.mp4?d=true'
                logger.debug(f"为URL添加了缺失的 .mp4 扩展名: {link}")
            
            try:
                parsed_url = urlparse(link)
                decoded_path = unquote(parsed_url.path)
                path_components = decoded_path.strip('/').split('/')
                
                if len(path_components) >= 2:
                    season = path_components[0]
                    sub_paths = path_components[1:-1]
                    authoritative_filename = path_components[-1]
                    
                    if not authoritative_filename: continue

                    result.append({
                        'season': season,
                        'path_parts': sub_paths,
                        'title': authoritative_filename,
                        'link': link
                    })
                else:
                    logger.warn(f"RSS项目链接无法解析出有效路径，跳过: {link}")
                    
            except Exception as e:
                logger.error(f"解析RSS item时发生未知错误: link={link}, error={e}")

        logger.info(f"成功从 RSS 源获取到 {len(result)} 个项目。")
        return result

    def get_current_season_list(self) -> List[Tuple[str, List[str], str]]:
        season = self.__get_ani_season()
        logger.info(f"正在获取当前季度的文件列表: {season}")
        return self.__traverse_directory([season])

    def get_all_season_list(self, start_year: int = 2019) -> List[Tuple[str, List[str], str]]:
        now = datetime.now()
        all_files = []

        for year in range(start_year, now.year + 1):
            for month in [1, 4, 7, 10]:
                if year == now.year and month > now.month:
                    continue
                season = f"{year}-{month}"
                logger.info(f"正在获取季度 {season} 的文件列表")
                try:
                    season_files = self.__traverse_directory([season])
                    all_files.extend(season_files)
                except Exception as e:
                    logger.warn(f"获取季度 {season} 的番剧失败: {e}")

        logger.info("正在获取 'ANi' 根目录的文件列表")
        try:
            ani_files = self.__traverse_directory(['ANi'])
            all_files.extend(ani_files)
        except Exception as e:
            logger.warn(f"获取 'ANi' 目录的文件失败: {e}")

        return all_files
    
    # <<< 修改：统一处理URL的编解码 >>>
    def __touch_strm_file(self, file_name: str, season: str, sub_paths: List[str] = None, file_url: str = None, overwrite: bool = False) -> bool:
        sub_paths = sub_paths or []
        
        # --- 统一的URL处理逻辑 ---
        if file_url:
            # 如果URL来自外部(RSS)，它可能是编码过的，进行解码以获得可读URL
            src_url = unquote(file_url)
        else:
            # 如果URL是内部构建的，需要对各部分进行编码，以生成合规的URL
            # 注意：这里我们不对'/'进行编码
            encoded_path_parts = [urllib.parse.quote(part) for part in [season] + sub_paths + [file_name]]
            src_url = f'https://ani.v300.eu.org/{"/".join(encoded_path_parts)}?d=true'

        # --- 统一的本地路径和文件名处理逻辑 ---
        target_dir_path = os.path.join(self._storageplace, season)
        
        target_file_name_final = file_name
        if sub_paths:
            prefix = " - ".join(sub_paths)
            target_file_name_final = f"{prefix} - {file_name}"

        os.makedirs(target_dir_path, exist_ok=True)
        target_file_path = os.path.join(target_dir_path, f"{target_file_name_final}.strm")

        if not overwrite and os.path.exists(target_file_path):
            logger.debug(f'{target_file_name_final}.strm 文件已存在，跳过创建。')
            return False

        # --- 统一的文件写入逻辑 ---
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_file_path = os.path.join(temp_dir, os.path.basename(target_file_path))
                with open(temp_file_path, 'w', encoding='utf-8') as file:
                    # 写入我们处理过的、人类可读的src_url
                    file.write(src_url)
                shutil.move(temp_file_path, target_file_path)
                logger.info(f'成功创建 .strm 文件: {target_file_path}')
            return True
        except Exception as e:
            logger.error(f'创建 .strm 文件失败: {target_file_name_final}, 错误: {e}')
            return False

    def __task(self, fulladd: bool = False, allseason: bool = False):
        cnt = 0
        overwrite_mode = self._overwrite # 这正确反映了插件的覆盖设置

        if allseason or fulladd:
            if allseason:
                logger.info(f"开始任务：为所有历史季度创建strm文件 (强制覆盖: {overwrite_mode})")
                file_list = self.get_all_season_list()
            else: # fulladd
                logger.info(f"开始任务：为当前季度创建strm文件 (强制覆盖: {overwrite_mode})")
                file_list = self.get_current_season_list()

            for season, path_parts, file_name in file_list:
                if self.__is_valid_file(file_name):
                    # 从遍历目录获取的文件，file_url为None，函数内部会构建和编码URL
                    if self.__touch_strm_file(file_name, season, path_parts, overwrite=overwrite_mode):
                        cnt += 1
        else:
            # 📢 这里的日志也更新了，显示当前的覆盖模式
            logger.info(f"开始任务：从RSS获取最新文件 (模式: 增量更新, 强制覆盖: {overwrite_mode})")
            rss_info_list = self.get_latest_list()
            for rss_info in rss_info_list:
                if self.__is_valid_file(rss_info['title']):
                    # 从RSS获取的文件，传入file_url，函数内部会解码URL
                    if self.__touch_strm_file(
                        file_name=rss_info['title'],
                        file_url=rss_info['link'],
                        season=rss_info['season'],
                        sub_paths=rss_info['path_parts'],
                        overwrite=overwrite_mode # ✅ 已将此参数更改为 overwrite_mode
                    ):
                        cnt += 1

        logger.info(f'任务完成，共创建 {cnt} 个 .strm 文件。')

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
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VSwitch', 'props': {'model': 'fulladd', 'label': '创建当季所有番剧'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VSwitch', 'props': {'model': 'allseason', 'label': '补全历史所有番剧'}}]}
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'cron', 'label': '执行周期 (Cron)', 'placeholder': '例如: 0 22 * * *'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'storageplace', 'label': 'Strm存储路径', 'placeholder': '/downloads/strm'}}]}
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSwitch', 'props': {'model': 'overwrite', 'label': '强制覆盖已存在的Strm文件(补全历史/当季时生效)'}}]}
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
            "overwrite": False,
        }

    def __update_config(self):
        self.update_config({
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "enabled": self._enabled,
            "fulladd": self._fulladd,
            "allseason": self._allseason,
            "storageplace": self._storageplace,
            "overwrite": self._overwrite,
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
    class MockLogger:
        def info(self, msg): print(f"信息: {msg}")
        def warn(self, msg): print(f"警告: {msg}")
        def error(self, msg): print(f"错误: {msg}")
        def debug(self, msg): print(f"调试: {msg}")

    logger = MockLogger()

    anistrm100 = ANiStrm100()
    anistrm100._storageplace = "./strm_test_cn"
    
    class MockSettings:
        USER_AGENT = "Mozilla/5.0"
        PROXY = None
        TZ = "Asia/Shanghai"
    settings = MockSettings()

    print("\n--- 模拟任务运行 (RSS模式) ---")
    anistrm100._overwrite = True
    anistrm100.__task(allseason=False, fulladd=False)
