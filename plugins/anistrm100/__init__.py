import os
import time
from datetime import datetime, timedelta
import re
import shutil
import tempfile
from urllib.parse import urlparse, unquote, urljoin, quote
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
    plugin_version = "3.3.5"
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
                # 在调度一次性任务后重置这些标志
                self._onlyonce = False
                self._fulladd = False
                self._allseason = False

            self.__update_config()
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def __get_ani_season(self, idx_month: int = None) -> str:
        """
        根据当前月份或指定月份获取番剧季度（例如：2023-10）。
        """
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
        """
        检查文件名是否包含“ANi”，用于筛选相关文件。
        """
        return 'ANi' in name

    @retry(Exception, tries=3, logger=logger, ret=[])
    def __traverse_directory(self, path_parts: List[str]) -> List[Dict[str, Any]]:
        """
        递归遍历 ANi 服务器目录以查找有效的视频文件。
        返回一个字典列表，包含 'season'、'path_parts' 和 'title'。
        **关键修正：在此方法中对从 API 获取的文件名进行解码。**
        """
        all_files = []
        base_ani_url = 'https://ani.v300.eu.org/'
        # 构建当前 URL，确保路径部分经过 URL 编码以保证 URL 安全性
        current_url = urljoin(base_ani_url, "/".join(quote(p) for p in path_parts) + '/')

        logger.debug(f"正在遍历: {current_url}")
        rep = RequestUtils(ua=settings.USER_AGENT, proxies=settings.PROXY).post(url=current_url)
        if rep and hasattr(rep, 'json'):
            items = rep.json().get('files', [])
        else:
            logger.warn(f"无法获取有效的响应或响应无json方法，URL: {current_url}")
            return []

        # path_parts 的第一部分被视为“season”
        season = path_parts[0]
        # 剩余部分是该季度内的子路径
        sub_path_list = path_parts[1:]

        for item in items:
            raw_item_name = item.get('name')
            if not raw_item_name:
                continue
            
            # --- 关键修正：在这里对从 API 获取的原始文件名进行解码 ---
            # 这样，item_name 在后续处理中都是人类可读的字符串
            item_name = unquote(raw_item_name)
            
            if self.__is_valid_file(item_name):
                # 找到一个有效的视频文件
                all_files.append({
                    'season': season,
                    'path_parts': sub_path_list,
                    'title': item_name,
                    'link': None # 链接将在 __touch_strm_file 中构建
                })
            elif '.' not in item_name and item.get('type') == 'directory': # 确保它是一个目录
                # 这是一个子目录，递归进入，使用已解码的 item_name
                all_files.extend(self.__traverse_directory(path_parts + [item_name]))

        return all_files

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_latest_list(self) -> List[Dict[str, Any]]:
        """
        从 RSS feed 获取最新的文件列表。
        返回一个字典列表，包含 'season'、'path_parts'、'title' 和 'link'。
        """
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

            # 修正 RSS feed 中常见的 URL 问题
            if link.endswith('?d=mp4'):
                link = link.removesuffix('?d=mp4') + '?d=true'
                logger.debug(f"修正了错误的URL后缀: {link}")
            if link.endswith('?d=true') and not link.endswith('.mp4?d=true'):
                link = link.removesuffix('?d=true') + '.mp4?d=true'
                logger.debug(f"为URL添加了缺失的 .mp4 扩展名: {link}")

            try:
                parsed_url = urlparse(link)
                # 解码路径以正确分割组件
                decoded_path = unquote(parsed_url.path)
                path_components = decoded_path.strip('/').split('/')

                if len(path_components) >= 2:
                    season = path_components[0]
                    sub_paths = path_components[1:-1]
                    authoritative_filename = path_components[-1]

                    if not authoritative_filename:
                        continue

                    result.append({
                        'season': season,
                        'path_parts': sub_paths,
                        'title': authoritative_filename,
                        'link': link # 保留 RSS 中原始的、已修正的链接
                    })
                else:
                    logger.warn(f"RSS项目链接无法解析出有效路径，跳过: {link}")

            except Exception as e:
                logger.error(f"解析RSS item时发生未知错误: link={link}, error={e}")

        logger.info(f"成功从 RSS 源获取到 {len(result)} 个项目。")
        return result

    def get_current_season_list(self) -> List[Dict[str, Any]]:
        """获取当前动漫季度的文件。"""
        season = self.__get_ani_season()
        logger.info(f"正在获取当前季度的文件列表: {season}")
        return self.__traverse_directory([season])

    def get_all_season_list(self, start_year: int = 2019) -> List[Dict[str, Any]]:
        """获取从 start_year 开始的所有动漫季度的文件。"""
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

        logger.info("正在获取 'ANi' 根目录的文件列表 (如果有)")
        try:
            # 同时检查顶层 'ANi' 目录（如果存在并包含文件）
            ani_files = self.__traverse_directory(['ANi'])
            all_files.extend(ani_files)
        except Exception as e:
            logger.warn(f"获取 'ANi' 目录的文件失败: {e}")

        return all_files

    def __touch_strm_file(self, file_data: Dict[str, Any], overwrite: bool = False) -> bool:
        """
        为给定的动漫剧集创建或更新 .strm 文件。
        确保所有路径/文件名在处理前都已被解码。

        Args:
            file_data (Dict[str, Any]): 包含文件信息的字典，键包括 'season'、
                                        'path_parts'、'title' 和可选的 'link'。
            overwrite (bool): 如果文件已存在，是否强制覆盖。

        Returns:
            bool: 如果文件成功创建或已存在并跳过，则为 True；否则为 False。
        """
        season = file_data['season']
        sub_paths = file_data.get('path_parts', [])
        file_name = file_data['title']
        file_url = file_data.get('link')

        # --- 统一解码输入参数，确保它们是未编码的字符串 ---
        # 即使上游方法已解码，这里再解码一次作为防御性编程，确保无编码残留
        file_name = unquote(file_name)
        season = unquote(season)
        sub_paths = [unquote(p) for p in sub_paths]

        base_ani_url = 'https://ani.v300.eu.org/'

        # --- 统一的 URL 处理逻辑 ---
        # 如果提供了 file_url (通常来自 RSS)，直接使用并确保解码
        if file_url:
            src_url = unquote(file_url)
        else:
            # 如果是内部构建的 URL (来自目录遍历)，需要对各部分进行编码
            # 因为 file_name, season, sub_paths 此时已是解码后的字符串
            encoded_path_parts = [quote(part) for part in [season] + sub_paths + [file_name]]
            src_url = urljoin(base_ani_url, "/".join(encoded_path_parts)) + '?d=true'

        # --- 统一的本地路径和文件名处理逻辑 ---
        # 本地存储路径现在包含 season 和所有 sub_paths
        target_dir_path = os.path.join(self._storageplace, season, *sub_paths)
        
        # 构建最终的文件名，如果有子路径则添加前缀
        target_file_name_final = file_name
        if sub_paths:
            prefix = " - ".join(sub_paths)
            target_file_name_final = f"{prefix} - {file_name}"

        # 确保目标目录存在
        os.makedirs(target_dir_path, exist_ok=True)
        target_file_path = os.path.join(target_dir_path, f"{target_file_name_final}.strm")

        if not overwrite and os.path.exists(target_file_path):
            logger.debug(f'{target_file_name_final}.strm 文件已存在，跳过创建。')
            return False

        # --- 统一的文件写入逻辑 ---
        try:
            # 使用临时文件写入，然后原子性地移动，避免文件损坏
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False) as temp_file:
                temp_file.write(src_url)
            shutil.move(temp_file.name, target_file_path)
            logger.info(f'成功创建 .strm 文件: {target_file_path}')
            return True
        except Exception as e:
            logger.error(f'创建 .strm 文件失败: {target_file_name_final}, 错误: {e}')
            # 确保临时文件在出错时被删除
            if os.path.exists(temp_file.name):
                os.remove(temp_file.name)
            return False


    def __task(self, fulladd: bool = False, allseason: bool = False):
        """
        插件的主要任务执行函数，根据配置模式创建 .strm 文件。
        """
        cnt = 0
        overwrite_mode = self._overwrite # 这正确反映了插件的覆盖设置

        file_list: List[Dict[str, Any]] = []

        if allseason:
            logger.info(f"开始任务：为所有历史季度创建strm文件 (强制覆盖: {overwrite_mode})")
            file_list = self.get_all_season_list()
        elif fulladd:
            logger.info(f"开始任务：为当前季度创建strm文件 (强制覆盖: {overwrite_mode})")
            file_list = self.get_current_season_list()
        else:
            logger.info(f"开始任务：从RSS获取最新文件 (模式: 增量更新, 强制覆盖: {overwrite_mode})")
            file_list = self.get_latest_list()
        
        for file_data in file_list:
            if self.__is_valid_file(file_data['title']):
                if self.__touch_strm_file(file_data, overwrite=overwrite_mode):
                    cnt += 1

        logger.info(f'任务完成，共创建 {cnt} 个 .strm 文件。')

    def get_state(self) -> bool:
        return self._enabled

    def get_command(self) -> List[Dict[str, Any]]:
        # 如果没有要暴露的命令，可以返回空列表或 None
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        # 如果没有要暴露的 API，可以返回空列表或 None
        return []

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
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSwitch', 'props': {'model': 'overwrite', 'label': '强制覆盖已存在的Strm文件'}}]}
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
        """将当前配置状态保存到插件配置中。"""
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
        # 如果没有要显示的页面，可以返回空列表或 None
        return []

    def stop_service(self):
        """停止调度器服务。"""
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error(f"退出插件失败：{str(e)}")

if __name__ == "__main__":
    class MockLogger:
        """用于测试的模拟日志器。"""
        def info(self, msg): print(f"信息: {msg}")
        def warn(self, msg): print(f"警告: {msg}")
        def error(self, msg): print(f"错误: {msg}")
        def debug(self, msg): print(f"调试: {msg}")

    logger = MockLogger()

    anistrm100 = ANiStrm100()
    anistrm100._storageplace = "./strm_test_cn"
    
    class MockSettings:
        """用于测试的模拟设置。"""
        USER_AGENT = "Mozilla/5.0"
        PROXY = None
        TZ = "Asia/Shanghai"
    settings = MockSettings()

    print("\n--- 模拟任务运行 (RSS模式) ---")
    anistrm100._overwrite = True
    # 模拟 RSS 模式运行，file_list 将从 get_latest_list() 获取
    anistrm100.__task(allseason=False, fulladd=False)
    
    print("\n--- 模拟任务运行 (完整添加当季模式) ---")
    anistrm100._overwrite = False # 切换为不覆盖模式
    anistrm100.__task(fulladd=True, allseason=False)

    print("\n--- 模拟任务运行 (补全历史模式) ---")
    anistrm100._overwrite = True # 切换回覆盖模式
    anistrm100.__task(fulladd=False, allseason=True)
