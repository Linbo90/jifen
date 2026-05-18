import re
import asyncio
import os
import sys
import time
from asyncio import Event, Lock, wait_for, TimeoutError
from datetime import datetime
from collections import deque
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.extensions import html as tl_html
import psycopg2
from psycopg2.extras import DictCursor

# ========= 配置（全部改为环境变量，Railway 后台设置）=========
API_ID = int(os.getenv("API_ID", "25559912"))
API_HASH = os.getenv("API_HASH", "22d3bb9665ad7e6a86e89c1445672e07")
SESSION = os.getenv("SESSION", "session")
RESTART_TIME = int(os.getenv("RESTART_TIME", "43200"))  # 12小时
TAIL_TEXT = os.getenv("TAIL_TEXT", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")

# ========= 新增：同步11.py的稳定性配置 =========
MAX_CACHE_SIZE = 2000  # 已处理消息ID缓存上限
FORWARD_INTERVAL = 8  # 转发间隔（秒），降低限流风险
MAX_RETRY = 5  # 发送失败最大重试次数

# ========= 回复联动配置 =========
ALLOW_REPLY_WITHOUT_MAPPING = True
client = TelegramClient(SESSION, API_ID, API_HASH)

# ========== 优雅关闭与自重启核心状态 ==========
stop_event = Event()
shutdown_lock = Lock()
is_shutting_down = False
is_restarting = False  # 防重复重启标记
active_tasks = set()

# ========= 全局缓存 =========
CHANNEL_MAP = {}
SOURCE_ENTITY_CACHE = {}
MESSAGE_ID_MAP = {}  # 回复映射依然保留数据库持久化，内存仅做缓存
DB_LOCK = asyncio.Lock()  # 数据库操作锁，防止并发冲突

# ========= 新增：同步11.py的内存管理与限流状态 =========
processed_msg_ids = deque(maxlen=MAX_CACHE_SIZE)  # 已处理消息ID缓存，自动淘汰旧数据
forward_lock = asyncio.Lock()  # 转发限流锁
last_forward_time = 0  # 上次转发时间戳

# ========= 日志 =========
def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# ========== 自重启与优雅关闭工具函数 ==========
def track_task(task):
    """跟踪活跃协程任务，优雅关闭时等待完成"""
    active_tasks.add(task)
    task.add_done_callback(active_tasks.discard)

def restart_program():
    """进程内无缝自重启，PID不变，不依赖外部平台"""
    global is_restarting
    if is_restarting:
        return
    is_restarting = True
    log("🔄 开始执行进程内自重启...")
    python = sys.executable
    os.execv(python, [python] + sys.argv)  # 替换当前进程镜像

async def graceful_shutdown():
    """终极修复：彻底删除任务等待与取消逻辑，直接断开连接重启，根除死锁和无限递归"""
    global is_shutting_down
    async with shutdown_lock:
        if is_shutting_down or is_restarting:
            return
        is_shutting_down = True
    
    log("🔌 开始快速优雅关闭，跳过任务等待直接重启...")
    
    try:
        # 直接断开连接，Telethon会自动终止所有未完成的请求
        await client.disconnect()
        log("✅ 客户端已正常断开，即将执行自重启")
    except Exception as e:
        log(f"⚠️  断开连接时出错：{str(e)}，强制执行自重启")
    
    restart_program()

async def stop_watcher():
    """监听停止事件，触发优雅关闭"""
    await stop_event.wait()
    await graceful_shutdown()

# ========= 新增：连接保活任务 =========
async def keep_alive():
    while True:
        await asyncio.sleep(300)  # 每5分钟发一个心跳
        try:
            await client.get_me()
            log("💓 连接保活成功")
        except Exception as e:
            log(f"⚠️  连接保活失败，准备重启: {str(e)}")
            stop_event.set()
            break

# ========= 新增：同步11.py的限流控制函数 =========
async def rate_limit_wait():
    """转发间隔控制，主动降低限流风险"""
    global last_forward_time
    async with forward_lock:
        now = time.time()
        wait_time = FORWARD_INTERVAL - (now - last_forward_time)
        if wait_time > 0:
            await asyncio.sleep(wait_time)
        last_forward_time = time.time()

# ========= 数据库初始化 =========
def init_db_sync():
    """同步数据库初始化，启动时执行一次"""
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS channel_configs (
            id SERIAL PRIMARY KEY,
            source_channel TEXT UNIQUE NOT NULL,
            target_channel TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS message_mappings (
            id SERIAL PRIMARY KEY,
            source_channel_id BIGINT NOT NULL,
            source_msg_id BIGINT NOT NULL,
            target_channel_id BIGINT NOT NULL,
            target_msg_id BIGINT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_channel_id, source_msg_id)
        )
    ''')
    
    default_channels = [
        ("@wenan77", "@wnffx"),    # 原默认映射
        ("@hotchigua", "@hrgxx"),      # 新增映射1
    ]
    cursor.executemany('''
        INSERT INTO channel_configs (source_channel, target_channel)
        VALUES (%s, %s)
        ON CONFLICT (source_channel) DO NOTHING
    ''', default_channels)
    log(f"已初始化 {len(default_channels)} 组默认频道映射")
    
    conn.commit()
    conn.close()
    log("数据库初始化完成")

# ========= 数据库操作封装 =========
async def db_execute(query: str, params: tuple = (), fetch: bool = False):
    """异步执行SQL，自动处理连接和锁"""
    async with DB_LOCK:
        return await asyncio.to_thread(_db_execute_sync, query, params, fetch)

def _db_execute_sync(query: str, params: tuple, fetch: bool):
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor(cursor_factory=DictCursor)
    cursor.execute(query, params)
    result = None
    if fetch:
        result = cursor.fetchall()
    conn.commit()
    conn.close()
    return result

# ========= 配置加载 =========
async def load_channel_config() -> dict:
    """从数据库加载频道配置"""
    config_map = {}
    rows = await db_execute(
        "SELECT source_channel, target_channel FROM channel_configs",
        fetch=True
    )
    for row in rows:
        source, target = row["source_channel"], row["target_channel"]
        config_map[source] = target
    log(f"频道配置加载完成，共加载 {len(config_map)} 组频道映射")
    return config_map

# ========= 消息映射持久化（完全保留原数据库逻辑，未修改）=========
async def load_message_mapping():
    """启动时加载历史消息ID映射到内存缓存"""
    global MESSAGE_ID_MAP
    rows = await db_execute(
        "SELECT source_channel_id, source_msg_id, target_channel_id, target_msg_id FROM message_mappings",
        fetch=True
    )
    for row in rows:
        map_key = f"{row['source_channel_id']}|{row['source_msg_id']}"
        map_value = f"{row['target_channel_id']}|{row['target_msg_id']}"
        MESSAGE_ID_MAP[map_key] = map_value
    log(f"消息映射加载完成，共加载 {len(MESSAGE_ID_MAP)} 条历史消息映射")

async def save_message_mapping(source_channel_id: int, source_msg_id: int, target_channel_id: int, target_msg_id: int):
    """保存消息ID映射到数据库，同时更新内存缓存"""
    map_key = f"{source_channel_id}|{source_msg_id}"
    map_value = f"{target_channel_id}|{target_msg_id}"
    MESSAGE_ID_MAP[map_key] = map_value
    
    await db_execute('''
        INSERT INTO message_mappings 
        (source_channel_id, source_msg_id, target_channel_id, target_msg_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (source_channel_id, source_msg_id) DO NOTHING
    ''', (source_channel_id, source_msg_id, target_channel_id, target_msg_id))

# ========= 回复目标ID获取（完全保留原数据库逻辑，未修改）=========
def get_reply_target_id(source_channel_id: int, msg) -> int | None:
    if not hasattr(msg, "reply_to") or not msg.reply_to:
        return None
    reply_to_msg_id = getattr(msg.reply_to, "reply_to_msg_id", None)
    if not reply_to_msg_id:
        return None
    map_key = f"{source_channel_id}|{reply_to_msg_id}"
    target_map_value = MESSAGE_ID_MAP.get(map_key)
    if not target_map_value:
        return None
    _, target_msg_id = target_map_value.split("|")
    return int(target_msg_id)

# ========= 业务逻辑函数 =========
def has_link(text: str) -> bool:
    if not text:
        return False
    # 匹配@、http://、https://、t.me 任意一种
    return bool(re.search(r"(@|https?://|t\.me)", text, re.IGNORECASE))

def has_paid_ad(text: str) -> bool:
    return bool(text and "付费广告" in text)

def is_forwarded_msg(msg) -> bool:
    return bool(getattr(msg, "fwd_from", None))

def count_buttons(msg) -> int:
    if not msg:
        return 0
    reply_markup = getattr(msg, "reply_markup", None)
    if reply_markup and hasattr(reply_markup, "rows"):
        return sum(len(row.buttons) for row in reply_markup.rows)
    if getattr(msg, "buttons", None):
        return sum(len(row) for row in msg.buttons)
    return 0

def pick_text_from_message(msg):
    txt = getattr(msg, "message", None) or getattr(msg, "raw_text", None) or ""
    return txt, getattr(msg, "entities", None)

def pick_caption_from_album(event, sorted_msgs=None):
    if not event.messages:
        log("相册事件异常：无任何媒体消息")
        return "", []
    
    if sorted_msgs is None:
        sorted_msgs = sorted(event.messages, key=lambda m: m.id)
    
    # 遍历所有消息查找文本，不局限于第一条
    for idx, msg in enumerate(sorted_msgs):
        txt = None
        entities = None
        
        # 严格按照优先级检查，确保文本和实体一一对应
        if hasattr(msg, 'caption') and msg.caption and msg.caption.strip():
            txt = msg.caption
            entities = getattr(msg, 'caption_entities', None)
            source = "caption"
        elif hasattr(msg, 'message') and msg.message and msg.message.strip():
            txt = msg.message
            entities = getattr(msg, 'entities', None)
            source = "message"
        elif hasattr(msg, 'text') and msg.text and msg.text.strip():
            txt = msg.text
            entities = getattr(msg, 'entities', None)
            source = "text"
        elif hasattr(msg, 'raw_text') and msg.raw_text and msg.raw_text.strip():
            txt = msg.raw_text
            entities = getattr(msg, 'entities', None)
            source = "raw_text"
        
        if txt:
            log(f"✅ 相册文本提取成功 | 位置:第{idx+1}条消息 | 来源:{source} | 消息ID:{msg.id} | 文本长度:{len(txt)} | 格式实体数:{len(entities or [])}")
            return txt, list(entities or [])
    
    log("⚠️  相册所有消息均未提取到文本内容")
    return "", []

def to_html(text: str, entities):
    if not text:
        return ""
    try:
        return tl_html.unparse(text, entities or [])
    except Exception:
        return text

# ========= 修改：同步11.py的错误重试逻辑（单条消息）=========
async def safe_send_single(*, target, text_html, media, reply_to=None):
    retry_count = 0
    send_success = False
    sent_msg = None
    
    while retry_count < MAX_RETRY and not send_success:
        try:
            await client.send_file(
                target,
                file=media,
                caption=text_html,
                parse_mode="html",
                link_preview=False,
                reply_to=reply_to
            )
            send_success = True
            break
        except FloodWaitError as e:
            retry_count += 1
            wait_time = e.seconds + 5
            log(f"⚠️  触发限流，等待{wait_time}秒后重试（第{retry_count}次）")
            await asyncio.sleep(wait_time)
        except Exception as e:
            retry_count += 1
            log(f"❌ 单媒体转发失败，第{retry_count}次重试 | 详情：{str(e)}")
            await asyncio.sleep(3)
    
    if send_success:
        return await client.get_messages(target, limit=1)
    return None

# ========= 修改：同步11.py的错误重试逻辑（相册）=========
async def safe_send_album(*, target, files, captions_html, reply_to=None):
    retry_count = 0
    send_success = False
    sent_msg = None
    
    while retry_count < MAX_RETRY and not send_success:
        try:
            await client.send_file(
                target,
                file=files,
                caption=captions_html,
                parse_mode="html",
                link_preview=False,
                reply_to=reply_to,
                force_album=True,
                force_document=False,
                use_cache=False,
                allow_cache=False
            )
            send_success = True
            break
        except FloodWaitError as e:
            retry_count += 1
            wait_time = e.seconds + 5
            log(f"⚠️  触发限流，等待{wait_time}秒后重试（第{retry_count}次）")
            await asyncio.sleep(wait_time)
        except Exception as e:
            retry_count += 1
            log(f"❌ 相册转发失败，第{retry_count}次重试 | 详情：{str(e)}")
            await asyncio.sleep(3)
    
    if send_success:
        return await client.get_messages(target, limit=1)
    return None

async def message_handler(event):
    try:
        if event.grouped_id:
            return
        
        # ✅ 新增：兜底检测延迟到达的相册消息
        if hasattr(event.message, 'grouped_id') and event.message.grouped_id:
            log(f"⏳ 检测到延迟到达的相册消息，等待5秒后由相册处理器处理 | 消息ID:{event.message.id}")
            await asyncio.sleep(5)
            return
        
        msg = event.message
        source_channel_id = event.chat_id
        target_entity = CHANNEL_MAP.get(source_channel_id)
        if not target_entity:
            log(f"拦截: 未找到该频道的目标映射 | 频道ID: {source_channel_id}")
            return
        
        if is_forwarded_msg(msg):
            log("拦截: 其他地方转发的单条消息")
            return
        
        if (source_channel_id, msg.id) in processed_msg_ids:
            log(f"⏭️  已跳过 | 原消息ID: {msg.id} | 同一条消息已转发")
            return
        
        text, entities = pick_text_from_message(msg)
        btn_count = count_buttons(msg)
        log(f"收到消息 | 文本长度:{len(text)} | 按钮:{btn_count} | 有媒体:{bool(msg.media)}")
        if not msg.media:
            log("拦截: 无媒体")
            return
        if has_paid_ad(text):
            log("拦截: 含付费广告")
            return
        if btn_count >= 1:
            log(f"拦截: 检测到按钮（数量:{btn_count}），全部禁止")
            return
        if has_link(text):
            log(f"拦截: 全文本包含违规内容（@/链接）| 原消息ID: {msg.id}")
            return
        
        new_text, new_entities = text, entities
        text_html = to_html(new_text, new_entities)
        
        reply_to_target_id = get_reply_target_id(source_channel_id, msg)
        if msg.reply_to and not reply_to_target_id and not ALLOW_REPLY_WITHOUT_MAPPING:
            log(f"拦截: 回复消息未找到原消息映射 | 消息ID: {msg.id} | 回复的原消息ID: {msg.reply_to.reply_to_msg_id}")
            return
        
        await rate_limit_wait()
        
        sent_msg = await safe_send_single(
            target=target_entity,
            text_html=text_html,
            media=msg.media,
            reply_to=reply_to_target_id
        )
        
        if sent_msg:
            sent_msg = sent_msg[0]
            await save_message_mapping(
                source_channel_id=source_channel_id,
                source_msg_id=msg.id,
                target_channel_id=target_entity.id,
                target_msg_id=sent_msg.id
            )
            processed_msg_ids.append((source_channel_id, msg.id))
            log(f"转发成功: 单条消息 | 原消息ID: {msg.id} | 目标消息ID: {sent_msg.id}" + (f" | 回复目标ID: {reply_to_target_id}" if reply_to_target_id else ""))
    except Exception as e:
        log(f"消息处理错误: {e}")

async def album_handler(event):
    try:
        await asyncio.sleep(5)
        msgs = event.messages
        sorted_msgs = sorted(msgs, key=lambda m: m.id)
        source_channel_id = event.chat_id
        target_entity = CHANNEL_MAP.get(source_channel_id)
        if not target_entity:
            log(f"拦截: 未找到该频道的目标映射 | 频道ID: {source_channel_id}")
            return
        
        if any(is_forwarded_msg(m) for m in msgs):
            log("拦截: 其他地方转发的相册消息")
            return
        
        first = sorted_msgs[0]
        # 新增：同步11.py的重复转发检查
        if (source_channel_id, first.id) in processed_msg_ids:
            log(f"⏭️  已跳过 | 原首条消息ID: {first.id} | 同一条相册已转发")
            return
        
        btn_count = sum(count_buttons(m) for m in msgs)
        text, entities = pick_caption_from_album(event, sorted_msgs)
        log(f"收到相册 | 原相册媒体数:{len(msgs)} | 最终提取文本长度:{len(text)} | 按钮:{btn_count}")
        if has_paid_ad(text):
            log("拦截: 含付费广告")
            return
        if btn_count >= 1:
            log(f"拦截: 相册检测到按钮（总数量:{btn_count}），全部禁止")
            return
        # 全文本违规检查：只要有任何@/链接，直接拦截，不做任何截断
        if has_link(text):
            log(f"拦截: 相册全文本包含违规内容（@/链接）| 原首条消息ID: {first.id}")
            return
        # 无违规，直接使用原文本和原始格式，不做任何修改
        new_text, new_entities = text, entities
        first_caption_html = to_html(new_text, new_entities)
        
        # 统一使用排序后的消息列表，确保媒体顺序与原相册一致
        valid_media_list = []
        for m in sorted_msgs:
            # 兼容1.42.0：明确判断有效媒体类型，过滤MediaEmpty等无效对象
            if hasattr(m, 'media') and m.media:
                if hasattr(m.media, 'photo') and m.media.photo:
                    valid_media_list.append(m.media)
                elif hasattr(m.media, 'document') and m.media.document:
                    valid_media_list.append(m.media)
        
        if len(valid_media_list) == 0:
            log("拦截: 相册无有效媒体文件")
            return
        
        valid_captions_list = [first_caption_html] + [""] * (len(valid_media_list) - 1)
        log(f"相册有效媒体数:{len(valid_media_list)} | 已完成长度匹配，保证相册不拆分")
        
        reply_to_target_id = get_reply_target_id(source_channel_id, first)
        if first.reply_to and not reply_to_target_id and not ALLOW_REPLY_WITHOUT_MAPPING:
            log(f"拦截: 回复相册未找到原消息映射 | 首条消息ID: {first.id} | 回复的原消息ID: {first.reply_to.reply_to_msg_id}")
            return
        
        # 新增：同步11.py的转发间隔控制
        await rate_limit_wait()
        
        sent_msg = await safe_send_album(
            target=target_entity,
            files=valid_media_list,
            captions_html=valid_captions_list,
            reply_to=reply_to_target_id
        )
        
        if sent_msg:
            sent_msg = sent_msg[0]
            await save_message_mapping(
                source_channel_id=source_channel_id,
                source_msg_id=first.id,
                target_channel_id=target_entity.id,
                target_msg_id=sent_msg.id
            )
            # 新增：同步11.py的已处理消息缓存
            processed_msg_ids.append((source_channel_id, first.id))
            log(f"转发成功: 相册 | 原首条消息ID: {first.id} | 目标消息ID: {sent_msg.id}" + (f" | 回复目标ID: {reply_to_target_id}" if reply_to_target_id else ""))
    except Exception as e:
        log(f"相册处理错误: {e}")

async def edit_handler(event):
    try:
        msg = event.message
        source_channel_id = event.chat_id
        btn_count = count_buttons(msg)
        text, _ = pick_text_from_message(msg)
        if btn_count >= 1 or has_paid_ad(text) or has_link(text):
            log(f"编辑后触发拦截 | 频道ID:{source_channel_id} | 消息ID:{msg.id} | 检测到按钮/付费广告/违规链接")
            return
    except Exception as e:
        log(f"编辑事件处理错误: {e}")

# ========== 定时重启 ==========
async def auto_restart():
    while True:
        await asyncio.sleep(RESTART_TIME)
        if is_shutting_down or is_restarting:
            break
        log(f"⏰ 到达定时重启时间（{RESTART_TIME//3600}小时），准备优雅重启...")
        stop_event.set()
        break

async def resolve_and_bind():
    global CHANNEL_MAP, SOURCE_ENTITY_CACHE
    config_map = await load_channel_config()
    
    temp_channel_map = {}
    temp_source_cache = {}
    for source_str, target_str in config_map.items():
        try:
            source_entity = await client.get_entity(source_str)
            target_entity = await client.get_entity(target_str)
            full_source_id = int(f"-100{source_entity.id}")
            temp_channel_map[full_source_id] = target_entity
            temp_source_cache[full_source_id] = source_entity
            log(f"频道解析成功 | 监听频道: {source_entity.title} (完整ID:{full_source_id}) | 目标频道: {target_entity.title} (ID:{target_entity.id})")
        except Exception as e:
            log(f"频道解析失败 | 监听标识: {source_str} | 目标标识: {target_str} | 错误: {e}")
            raise SystemExit(1)
    
    CHANNEL_MAP = temp_channel_map
    SOURCE_ENTITY_CACHE = temp_source_cache
    
    me = await client.get_me()
    log(f"启动成功 | 登录账号: {me.username or me.id}")
    log(f"共监听 {len(CHANNEL_MAP)} 个频道，全部绑定完成")
    
    source_chats = list(temp_source_cache.values())
    client.add_event_handler(album_handler, events.Album(chats=source_chats))
    client.add_event_handler(message_handler, events.NewMessage(chats=source_chats))
    client.add_event_handler(edit_handler, events.MessageEdited(chats=source_chats))

async def main():
    if not DATABASE_URL:
        raise ValueError("请在Railway环境变量中配置DATABASE_URL")
    
    await asyncio.to_thread(init_db_sync)
    await client.start()
    await load_message_mapping()
    await resolve_and_bind()
    
    # 启动后台任务
    track_task(asyncio.create_task(stop_watcher()))
    restart_task = asyncio.create_task(auto_restart())
    track_task(restart_task)
    track_task(asyncio.create_task(keep_alive()))  # ✅ 新增这一行，启动连接保活
    await client.run_until_disconnected()

if __name__ == "__main__":
    try:
        log("🚀 程序启动中，开启内置进程自重启模式...")
        asyncio.run(main())
    except KeyboardInterrupt:
        log("\n✅ 程序已手动停止，取消自重启")
    except Exception as e:
        log(f"❌ 程序异常退出：{str(e)}，5秒后自动重启...")
        time.sleep(5)
        restart_program()
