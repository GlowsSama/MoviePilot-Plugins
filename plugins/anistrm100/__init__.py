import os
import time
from datetime import datetime, timedelta
import re 
import shutil 
import tempfile 

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
          tries: int = 3, delay: int = 5, backoff: int = 1, logger: Any = None, ret: Any = None): 
    def deco_retry(f):
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 0:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = f"发生错误，将在 {tries - mtries + 1}/{tries} 次重试中，等待 {mdelay} 秒... 错误详情: {e}" # 优化日志
                    if logger:
                        logger.warn(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            if logger:
                logger.error('多次重试后仍然失败。请检查网络连接、代理设置或目标服务是否可用。') 
            return ret
        return f_retry
    return deco_retry

class ANiStrm100(_PluginBase):
    plugin_name = "ANiStrm100"
    plugin_desc = "自动获取当季所有番剧，免去下载，轻松拥有一个番剧媒体库"
    plugin_icon = "https://raw.githubusercontent.com/honue/MoviePilot-Plugins/main/icons/anistrm.png"
    plugin_version = "3.0.1" # 版本更新，重大功能调整
    plugin_author = "GlowsSama & Gemini"
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
    _overwrite = False 
    _onlyonce_mode = "rss" # 新增：立即运行一次的模式，默认从RSS获取

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
            self._onlyonce_mode = config.get("onlyonce_mode", "rss") # 读取新配置
        
        if self._enabled or self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            if self._enabled and self._cron:
                try:
                    # Cron 任务直接使用持久化配置的 _fulladd 和 _allseason
                    self._scheduler.add_job(func=self.__task,
                                             args=[self._fulladd, self._allseason, False, "rss"], # fulladd, allseason, is_onlyonce_run, onlyonce_mode (for cron, onlyonce_mode is irrelevant)
                                             trigger=CronTrigger.from_crontab(self._cron),
                                             name="ANiStrm100文件创建")
                    logger.info(f'ANi-Strm定时任务创建成功：{self._cron}')
                except Exception as err:
                    logger.error(f"定时任务配置错误：{str(err)}")
            
            if self._onlyonce:
                logger.info(f"ANi-Strm服务启动，立即运行一次，模式：{self._onlyonce_mode}")
                # 立即运行一次的任务，根据 _onlyonce_mode 传递参数
                # fulladd 和 allseason 设为 False，由 __task 内部根据 onlyonce_mode 决定
                self._scheduler.add_job(func=self.__task,
                                         args=[False, False, True, self._onlyonce_mode], 
                                         trigger='date',
                                         run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                         name="ANiStrm100文件创建")
                # 立即运行一次后，将 onlyonce 标志重置为 False，避免重复触发
                self._onlyonce = False 
            
            self.__update_config() # 保存最新的配置状态，包括重置后的 _onlyonce
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

    @retry(Exception, tries=3, delay=5, logger=logger, ret=[]) 
    def __traverse_directory(self, path_parts: List[str]) -> List[Tuple[str, List[str], str]]:
        all_files = []
        current_path_str = "/".join(path_parts)
        url = f'https://openani.an-i.workers.dev/{current_path_str}/'
        
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

    @retry(Exception, tries=3, delay=5, logger=logger, ret=[]) 
    def get_latest_list(self) -> List:
        addr = 'https://api.ani.rip/ani-download.xml'
        logger.info(f"正在尝试从 RSS 源获取最新文件: {addr}") 
        ret = RequestUtils(ua=settings.USER_AGENT, proxies=settings.PROXY).get_res(addr)
        if ret and hasattr(ret, 'text'):
            dom_tree = xml.dom.minidom.parseString(ret.text)
            items = dom_tree.documentElement.getElementsByTagName("item")
            result = []
            for item in items:
                title = DomUtils.tag_value(item, "title", default="")
                link = DomUtils.tag_value(item, "link", default="")
                season_match = re.search(r'/(\d{4}-\d{1,2})/', link)
                if season_match:
                    full_path = link.split(season_match.group(0))[-1]
                    path_parts = full_path.split('/')
                    file_name_from_link = path_parts.pop() 
                    if title in file_name_from_link:
                         result.append({
                            'season': season_match.group(1),
                            'path_parts': path_parts, 
                            'title': title, 
                            'link': link.replace("resources.ani.rip", "openani.an-i.workers.dev")
                        })
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
    
    def __touch_strm_file(self, file_name: str, season: str, sub_paths: List[str] = None, file_url: str = None, overwrite: bool = False) -> bool:
        target_dir_path = os.path.join(self._storageplace, season)
        os.makedirs(target_dir_path, exist_ok=True)

        target_file_name = f'{file_name}.strm'
        target_file_path = os.path.join(target_dir_path, target_file_name)
        
        if not overwrite and os.path.exists(target_file_path):
            logger.debug(f'{target_file_name} 文件已存在于最终目录，跳过创建。')
            return False

        if file_url:
            src_url = file_url
        else:
            remote_path = "/".join([season] + (sub_paths or []) + [file_name])
            src_url = f'https://openani.an-i.workers.dev/{remote_path}?d=true'

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_file_path = os.path.join(temp_dir, target_file_name)
                with open(temp_file_path, 'w', encoding='utf-8') as file:
                    file.write(src_url)
                logger.debug(f'成功在临时目录创建 .strm 文件: {temp_file_path}')
                
                shutil.move(temp_file_path, target_file_path)
                logger.info(f'成功将文件从临时目录移动到: {target_file_path}')

            return True
        except Exception as e:
            logger.error(f'创建或移动 .strm 文件 {target_file_name} 失败: {e}')
            return False

    # <<< 核心修改：__task 函数现在可以根据 is_onlyonce_run 和 onlyonce_mode 调整行为 >>>
    def __task(self, fulladd: bool = False, allseason: bool = False, is_onlyonce_run: bool = False, onlyonce_mode: str = 'rss'):
        cnt = 0
        overwrite_mode = self._overwrite 

        # 如果是“立即运行一次”的调用，根据 onlyonce_mode 覆盖 fulladd 和 allseason
        if is_onlyonce_run:
            logger.info(f"执行“立即运行一次”任务，模式：{onlyonce_mode}")
            if onlyonce_mode == 'allseason':
                allseason = True
                fulladd = False # 确保只有一个模式激活
            elif onlyonce_mode == 'fulladd':
                fulladd = True
                allseason = False # 确保只有一个模式激活
            else: # 默认为 'rss'
                fulladd = False
                allseason = False
        else:
            logger.info("执行定时任务。") # 区分定时任务和立即运行任务

        # 以下逻辑保持不变，它会使用上面确定好的 fulladd 和 allseason 值
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
                            # 移除原有的 fulladd 和 allseason 开关，因为它们现在由 onlyonce_mode 或 cron 控制
                            # {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VSwitch', 'props': {'model': 'fulladd', 'label': '创建当季所有番剧strm'}}]},
                            # {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VSwitch', 'props': {'model': 'allseason', 'label': '创建历史所有季度番剧strm'}}]}
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [ # 新增立即运行一次的模式选择
                                {'component': 'VSelect', 
                                 'props': {
                                     'model': 'onlyonce_mode', 
                                     'label': '立即运行一次模式', 
                                     'items': [
                                         {'text': 'RSS更新', 'value': 'rss'},
                                         {'text': '创建当季所有番剧strm', 'value': 'fulladd'},
                                         {'text': '创建历史所有季度番剧strm', 'value': 'allseason'}
                                     ]
                                 }}
                            ]}
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
            "fulladd": False, # 默认值，但现在主要由 cron 或 onlyonce_mode 控制
            "allseason": False, # 默认值，但现在主要由 cron 或 onlyonce_mode 控制
            "storageplace": "/downloads/strm",
            "cron": "*/20 22,23,0,1 * * *",
            "overwrite": False, 
            "onlyonce_mode": "rss", # 新增默认值
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
            "onlyonce_mode": self._onlyonce_mode, # 保存新配置
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
    anistrm100.settings = lambda: None
    anistrm100.settings.USER_AGENT = "Mozilla/5.0"
    anistrm100.settings.PROXY = None
    
    print("--- 测试 get_all_season_list ---")
    all_files = anistrm100.get_all_season_list() 
    print(f"--- 总共找到 {len(all_files)} 个文件 ---")

    for i, (season, path_parts, file_name) in enumerate(all_files[:5]): 
        print(f"[{i+1}] 原始信息：根目录: {season}, 子路径: {'/'.join(path_parts) if path_parts else '无'}, 文件: {file_name}")
    if len(all_files) > 5:
        print("...")
        for i, (season, path_parts, file_name) in enumerate(all_files[-5:], start=len(all_files)-4): 
             print(f"[{i}] 原始信息：根目录: {season}, 子路径: {'/'.join(path_parts) if path_parts else '无'}, 文件: {file_name}")

    print("\n--- 模拟任务运行 (allseason模式，通过 onlyonce_mode 触发) ---")
    anistrm100._overwrite = True 
    anistrm100._onlyonce_mode = "allseason" # 模拟用户选择“历史全量”
    anistrm100.__task(is_onlyonce_run=True, onlyonce_mode=anistrm100._onlyonce_mode)
    
    print("\n--- 模拟任务运行 (RSS模式，通过 onlyonce_mode 触发) ---")
    anistrm100._overwrite = False 
    anistrm100._onlyonce_mode = "rss" # 模拟用户选择“RSS更新”
    anistrm100.__task(is_onlyonce_run=True, onlyonce_mode=anistrm100._onlyonce_mode)

    print("\n--- 模拟定时任务运行 (假设配置为 fulladd=False, allseason=False) ---")
    anistrm100._fulladd = False
    anistrm100._allseason = False
    anistrm100.__task(fulladd=anistrm100._fulladd, allseason=anistrm100._allseason, is_onlyonce_run=False)
