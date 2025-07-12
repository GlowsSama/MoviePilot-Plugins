import os
import time
from datetime import datetime, timedelta
import re
import shutil
import tempfile
from urllib.parse import urlparse, unquote
import urllib.parse # Added this import

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

# 重试装饰器
def retry(ExceptionToCheck: Any,
          tries: int = 3, delay: int = 3, backoff: int = 1, logger: Any = None, ret: Any = None):
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

class ANiStrm100(_PluginBase):
    plugin_name = "ANiStrm100"
    plugin_desc = "自动获取当季所有番剧，免去下载，轻松拥有一个番剧媒体库"
    plugin_icon = "https://raw.githubusercontent.com/honue/MoviePilot-Plugins/main/icons/anistrm.png"
    plugin_version = "3.2.1" # 版本更新，以体现新功能
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
    _overwrite = False # 新增：强制覆盖选项

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
            self._overwrite = config.get("overwrite", False) # 读取配置，默认为 False

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

        if 1 <= current_month <= 3: season_month = 1
        elif 4 <= current_month <= 6: season_month = 4
        elif 7 <= current_month <= 9: season_month = 7
        else: season_month = 10
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
        # 增强健壮性：检查 rep 是否有效，以及是否有 .json() 方法
        if rep and hasattr(rep, 'json'):
            items = rep.json().get('files', [])
        else:
            logger.warn(f"无法获取有效的响应或响应无json方法，URL: {url}")
            items = [] # 返回空列表以避免后续错误

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

    def get_current_season_list(self) -> List[Tuple[str, List[str], str]]:
        season = self.__get_ani_season()
        logger.info(f"正在获取当前季度的文件列表: {season}")
        return self.__traverse_directory([season])

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_latest_list(self) -> List:
        addr = 'https://aniapi.v300.eu.org/ani-download.xml'
        logger.info(f"正在尝试从 RSS 源获取最新文件: {addr}")
        ret = RequestUtils(ua=settings.USER_AGENT, proxies=settings.PROXY).get_res(addr)

        if ret and hasattr(ret, 'text'):
            dom_tree = xml.dom.minidom.parseString(ret.text)
            items = dom_tree.documentElement.getElementsByTagName("item")
            result = []

            for item in items:
                title = DomUtils.tag_value(item, "title", default="")
                link = DomUtils.tag_value(item, "link", default="")

                # 确保 link 是有效的 URL
                if not link.startswith(('http://', 'https://')):
                    logger.warn(f"RSS 项目链接无效，跳过: {link}")
                    continue

                season_match = re.search(r'/(\d{4}-\d{1,2})/', link)
                if not season_match:
                    logger.debug(f"RSS 项目链接未找到季度信息，跳过: {link}")
                    continue

                # ==== 👇 处理文件名与 query 参数补后缀逻辑 👇 ====
                parsed = urllib.parse.urlparse(link)
                name = os.path.basename(parsed.path)  # /…/X -> X
                qs = urllib.parse.parse_qs(parsed.query).get('d', [''])[0]  # 'true' 或 'mp4'

                if qs and not name.endswith(f'.{qs}') and qs not in ['true', '']:
                    name = f"{name}.{qs}"  # 补成 [ANi] XXX - 01.mp4 等

                decoded_name = urllib.parse.unquote(name)

                # ==== 👇 检查 title 是否匹配 👇 ====
                if title in decoded_name:
                    result.append({
                        'season': season_match.group(1),
                        'path_parts': [],
                        'title': title,
                        'link': link
                    })
                else:
                    logger.debug(f"RSS 项目名称不匹配，跳过。Title: '{title}', Link Filename: '{decoded_name}'")

            logger.info(f"成功从 RSS 源获取到 {len(result)} 个项目。")
            return result
        else:
            logger.warn(f"无法获取有效的RSS响应或响应无text属性，URL: {addr}。这可能是网络问题或RSS源暂时不可用。")
            return []




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
                    if season_files:
                        all_files.extend(season_files)
                except Exception as e:
                    logger.warn(f"获取季度 {season} 的番剧失败: {e}")

        logger.info("正在获取 'ANi' 根目录的文件列表")
        try:
            ani_files = self.__traverse_directory(['ANi'])
            if ani_files:
                all_files.extend(ani_files)
        except Exception as e:
            logger.warn(f"获取 'ANi' 目录的文件失败: {e}")

        return all_files

    # <<< 修改：新增 overwrite 参数，并根据其决定是否跳过文件存在检查 >>>
    def __touch_strm_file(self, file_name: str, season: str, sub_paths: List[str] = None, file_url: str = None, overwrite: bool = False) -> bool:
        sub_paths = sub_paths or []

        target_dir_path = os.path.join(self._storageplace, season)
        os.makedirs(target_dir_path, exist_ok=True)

        target_file_name = f'{file_name}.strm'
        target_file_path = os.path.join(target_dir_path, target_file_name)

        # 检查最终文件是否已存在，如果不是强制覆盖模式，则跳过
        if not overwrite and os.path.exists(target_file_path):
            logger.debug(f'{target_file_name} 文件已存在于最终目录，跳过创建。')
            return False

        if file_url:
            src_url = file_url
        else:
            remote_path = "/".join([season] + sub_paths + [file_name])
            src_url = f'https://ani.v300.eu.org/{remote_path}?d=true'

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_file_path = os.path.join(temp_dir, target_file_name)
                with open(temp_file_path, 'w', encoding='utf-8') as file:
                    file.write(src_url)
                logger.debug(f'成功在临时目录创建 .strm 文件: {temp_file_path}')

                # shutil.move 会自动处理目标文件已存在时的覆盖（如果是文件）
                shutil.move(temp_file_path, target_file_path)
                logger.info(f'成功将文件从临时目录移动到: {target_file_path}') # 修改为info级别，更明确地表示成功

            return True
        except Exception as e:
            logger.error(f'创建或移动 .strm 文件 {target_file_name} 失败: {e}')
            return False

    def __task(self, fulladd: bool = False, allseason: bool = False):
        cnt = 0

        # 将 self._overwrite 传递给 __touch_strm_file
        overwrite_mode = self._overwrite

        if allseason:
            logger.info("开始任务：为所有历史季度和'ANi'目录创建strm文件。")
            file_list = self.get_all_season_list()
            logger.info(f"处理所有历史内容，共找到 {len(file_list)} 个文件。")
            for season, path_parts, file_name in file_list:
                if self.__is_valid_file(file_name):
                    if self.__touch_strm_file(file_name=file_name, season=season, sub_paths=path_parts, overwrite=overwrite_mode):
                        cnt += 1
        elif fulladd:
            logger.info("开始任务：为当前季度的所有文件创建strm文件。")
            file_list = self.get_current_season_list()
            logger.info(f'处理当前季度，共找到 {len(file_list)} 个文件。')
            for season, path_parts, file_name in file_list:
                if self.__is_valid_file(file_name):
                    if self.__touch_strm_file(file_name=file_name, season=season, sub_paths=path_parts, overwrite=overwrite_mode):
                        cnt += 1
        else:
            logger.info("开始任务：从RSS源获取最新文件。")
            rss_info_list = self.get_latest_list()
            logger.info(f'处理RSS源，找到 {len(rss_info_list)} 个新项目。')
            for rss_info in rss_info_list:
                if self.__is_valid_file(rss_info['title']):
                    if self.__touch_strm_file(file_name=rss_info['title'],
                                              file_url=rss_info['link'],
                                              season=rss_info['season'],
                                              sub_paths=rss_info['path_parts'],
                                              overwrite=overwrite_mode):
                        cnt += 1

        logger.info(f'任务完成。共创建了 {cnt} 个新的 .strm 文件。')

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
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSwitch', 'props': {'model': 'overwrite', 'label': '强制覆盖已存在的Strm文件'}}]} # 新增覆盖开关
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
            "overwrite": False, # 默认不强制覆盖
        }

    def __update_config(self):
        self.update_config({
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "enabled": self._enabled,
            "fulladd": self._fulladd,
            "allseason": self._allseason,
            "storageplace": self._storageplace,
            "overwrite": self._overwrite, # 保存覆盖选项
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
    # Mock settings object
    class MockSettings:
        USER_AGENT = "Mozilla/5.0"
        PROXY = None
        TZ = "Asia/Shanghai" # Assuming a timezone for testing purposes
    anistrm100.settings = MockSettings()


    print("--- 测试 get_all_season_list (起始年份2019，包含 'ANi' 目录) ---")
    all_files = anistrm100.get_all_season_list() # 使用默认起始年份
    print(f"--- 总共找到 {len(all_files)} 个文件 ---")

    # 打印一些结果作为示例
    for season, path_parts, file_name in all_files[:3]:
        print(f"根目录: {season}, 子路径: {'/'.join(path_parts)}, 文件: {file_name}")
    if len(all_files) > 3:
        print("...")
        for season, path_parts, file_name in all_files[-3:]:
            print(f"根目录: {season}, 子路径: {'/'.join(path_parts)}, 文件: {file_name}")

    print("\n--- 模拟任务运行 (allseason模式) ---")
    anistrm100._overwrite = True # 模拟强制覆盖
    anistrm100.__task(allseason=True)
    print("\n--- 模拟任务运行 (RSS模式，不覆盖) ---")
    anistrm100._overwrite = False # 模拟不覆盖
    anistrm100.__task(allseason=False, fulladd=False)
