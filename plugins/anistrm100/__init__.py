import os
import time
from datetime import datetime, timedelta
import re # 导入正则表达式库，用于从URL中解析季度信息

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
    plugin_version = "2.6.7" # 插件版本
    plugin_author = "GlowsSama"
    author_url = "https://github.com/GlowsSama"
    plugin_config_prefix = "anistrm100_"
    plugin_order = 15
    auth_level = 2

    # 插件配置项
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
        
        # 此逻辑确定当前季度的*开始*月份
        if 1 <= current_month <= 3:
            season_month = 1
        elif 4 <= current_month <= 6:
            season_month = 4
        elif 7 <= current_month <= 9:
            season_month = 7
        else: # 10, 11, 12
            season_month = 10
        self._date = f'{current_year}-{season_month}'
        return self._date

    def __is_valid_file(self, name: str) -> bool:
        # 这是你的规则：一个可下载的文件名必须包含 "ANi"
        return 'ANi' in name

    # <<< 新增：核心的递归遍历函数 >>>
    @retry(Exception, tries=3, logger=logger, ret=[])
    def __traverse_directory(self, path_parts: List[str]) -> List[Tuple[str, List[str], str]]:
        """
        递归遍历目录以查找所有有效文件。
        :param path_parts: 路径部分的列表，例如 ['2024-4'] 或 ['2024-4', '番剧名']。
        :return: 一个元组列表，每个元组包含 (季度, 子路径列表, 文件名)。
        """
        all_files = []
        current_path_str = "/".join(path_parts)
        url = f'https://openani.an-i.workers.dev/{current_path_str}/'
        
        logger.debug(f"正在遍历: {url}")
        rep = RequestUtils(ua=settings.USER_AGENT, proxies=settings.PROXY).post(url=url)
        # 假设API即使在没有文件时也会优雅地返回一个包含'files'键的JSON对象
        items = rep.json().get('files', [])

        season_str = path_parts[0] # 列表的第一个元素总是季度, e.g., '2024-4'
        sub_path_list = path_parts[1:] # 剩下的是子目录

        for item in items:
            item_name = item.get('name')
            if not item_name:
                continue

            if self.__is_valid_file(item_name):
                # 这是一个文件，将其添加到我们的结果中
                all_files.append((season_str, sub_path_list, item_name))
            else:
                # 这很可能是一个目录，需要递归进入
                # 通常文件夹的 'size' 为0或不存在，但根据你定义的规则，我们检查文件名
                # 为避免在奇怪的文件夹名上产生无限循环，我们增加一个简单的检查
                if '.' not in item_name: # 一个简单的启发式方法来猜测它是否是文件夹
                     all_files.extend(self.__traverse_directory(path_parts + [item_name]))
        
        return all_files

    # <<< 修改：现在使用递归遍历函数 >>>
    def get_current_season_list(self) -> List[Tuple[str, List[str], str]]:
        season = self.__get_ani_season()
        logger.info(f"正在获取当前季度的文件列表: {season}")
        return self.__traverse_directory([season])

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
            # 尝试从链接本身提取季度和路径，以便更好地分类
            # 示例链接: https://.../2024-4/番剧名/文件名.mp4
            season_match = re.search(r'/(\d{4}-\d{1,2})/', link)
            if season_match:
                full_path = link.split(season_match.group(0))[-1]
                path_parts = full_path.split('/')
                file_name_from_link = path_parts.pop() # 移除并获取最后一个元素（文件名）
                # 确保RSS的标题和链接中的文件名匹配
                if title in file_name_from_link:
                     result.append({
                        'season': season_match.group(1),
                        'path_parts': path_parts,
                        'title': title, # 保留原始标题
                        'link': link.replace("resources.ani.rip", "openani.an-i.workers.dev")
                    })
        return result

    # <<< 修改：现在使用递归遍历函数 >>>
    def get_all_season_list(self, start_year: int = 2018) -> List[Tuple[str, List[str], str]]:
        now = datetime.now()
        all_files = []
        for year in range(start_year, now.year + 1):
            for month in [1, 4, 7, 10]:
                if year == now.year and month > now.month:
                    continue # 不检查未来的季度
                season = f"{year}-{month}"
                logger.info(f"正在获取季度 {season} 的文件列表")
                try:
                    season_files = self.__traverse_directory([season])
                    if season_files:
                        all_files.extend(season_files)
                except Exception as e:
                    logger.warn(f"获取季度 {season} 的番剧失败: {e}")
        return all_files

    # <<< 修改：更新以处理子目录列表 >>>
    def __touch_strm_file(self, file_name: str, season: str, sub_paths: List[str] = None, file_url: str = None) -> bool:
        sub_paths = sub_paths or []
        
        # 构建本地目录路径，包括所有子目录
        # os.path.join 通过 * 操作符可以正确处理子路径列表
        dir_path = os.path.join(self._storageplace, season, *sub_paths)
        os.makedirs(dir_path, exist_ok=True)

        # 构建 .strm 文件内容的源URL
        if file_url:
            src_url = file_url
        else:
            remote_path = "/".join([season] + sub_paths + [file_name])
            src_url = f'https://openani.an-i.workers.dev/{remote_path}?d=true'

        file_path = os.path.join(dir_path, f'{file_name}.strm')
        if os.path.exists(file_path):
            logger.debug(f'{file_name}.strm 文件已存在')
            return False
        
        try:
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(src_url)
            logger.debug(f'成功创建 .strm 文件: {file_path}')
            return True
        except Exception as e:
            logger.error(f'创建 .strm 文件 {file_path} 失败: {e}')
            return False

    # <<< 修改：更新主任务逻辑以适应新的数据结构 >>>
    def __task(self, fulladd: bool = False, allseason: bool = False):
        cnt = 0
        file_list = []

        if allseason:
            logger.info("开始任务：为所有历史季度创建strm文件。")
            file_list = self.get_all_season_list()
            logger.info(f'处理所有季度，共找到 {len(file_list)} 个文件。')
            for season, path_parts, file_name in file_list:
                if self.__is_valid_file(file_name):
                    if self.__touch_strm_file(file_name=file_name, season=season, sub_paths=path_parts):
                        cnt += 1
        elif fulladd:
            logger.info("开始任务：为当前季度的所有文件创建strm文件。")
            file_list = self.get_current_season_list()
            logger.info(f'处理当前季度，共找到 {len(file_list)} 个文件。')
            for season, path_parts, file_name in file_list:
                if self.__is_valid_file(file_name):
                    if self.__touch_strm_file(file_name=file_name, season=season, sub_paths=path_parts):
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
                                              sub_paths=rss_info['path_parts']):
                        cnt += 1
        
        logger.info(f'任务完成。共创建了 {cnt} 个新的 .strm 文件。')

    def get_state(self) -> bool:
        return self._enabled

    def get_command(self) -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        # 这部分是UI界面定义
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
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'cron', 'label': '执行周期 (Cron)', 'placeholder': '例如: 0 22 * * *'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'storageplace', 'label': 'Strm存储路径', 'placeholder': '/downloads/strm'}}]}
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
    # 用于本地测试的示例代码
    class MockLogger:
        def info(self, msg): print(f"信息: {msg}")
        def warn(self, msg): print(f"警告: {msg}")
        def error(self, msg): print(f"错误: {msg}")
        def debug(self, msg): print(f"调试: {msg}")

    logger = MockLogger()
    
    anistrm100 = ANiStrm100()
    # 为测试配置插件
    anistrm100._storageplace = "./strm_test_cn" # 测试用的strm输出目录
    anistrm100.settings = lambda: None # 模拟settings对象
    anistrm100.settings.USER_AGENT = "Mozilla/5.0"
    anistrm100.settings.PROXY = None
    
    print("--- 测试 get_all_season_list (为加快速度，从最近的年份开始) ---")
    all_files = anistrm100.get_all_season_list(start_year=2024)
    # 打印前5个结果作为示例
    for season, path_parts, file_name in all_files[:5]: 
        print(f"季度: {season}, 路径: {'/'.join(path_parts)}, 文件: {file_name}")
    
    print("\n--- 模拟任务运行 (fulladd模式) ---")
    anistrm100.__task(fulladd=True)
