import asyncio
import re
import os
from datetime import datetime, timedelta
from pytz import timezone
from telegram import Update
from telegram.ext import (
    Application,
    MessageHandler,
    CommandHandler,
    filters,
    ContextTypes,
)
import psycopg2
from psycopg2.extras import DictCursor

# ===================== 环境变量配置（Railway后台设置，无需改代码）=====================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
INIT_ADMIN_USERNAME = os.getenv("INIT_ADMIN_USERNAME", "lmdoi")
DATABASE_URL = os.getenv("DATABASE_URL", "")
TZ = timezone(os.getenv("TZ", "Asia/Shanghai"))
# 规则配置（也可在Railway环境变量自定义）
VALID_MESSAGE_MIN_LENGTH = int(os.getenv("VALID_MESSAGE_MIN_LENGTH", 5))
SIGN_IN_POINTS = int(os.getenv("SIGN_IN_POINTS", 80))
DAILY_SPEECH_TARGET = int(os.getenv("DAILY_SPEECH_TARGET", 288))
DAILY_BONUS_POINTS = int(os.getenv("DAILY_BONUS_POINTS", 288))
WEEKLY_SPEECH_TARGET = int(os.getenv("WEEKLY_SPEECH_TARGET", 2888))
WEEKLY_BONUS_POINTS = int(os.getenv("WEEKLY_BONUS_POINTS", 1688))
RANK_SHOW_LIMIT = int(os.getenv("RANK_SHOW_LIMIT", 10))
# ========================================================================================

# 数据库初始化
def init_db():
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    # 用户表：存储用户信息、积分、权限
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            points INTEGER DEFAULT 0,
            has_permission INTEGER DEFAULT 0
        )
    ''')

    # 发言统计表：记录所有有效发言，用于周期统计
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS message_stats (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            chat_id BIGINT,
            message_time TIMESTAMP,
            is_valid INTEGER DEFAULT 1,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
    ''')

    # 签到记录表：防止重复签到
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS check_in (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            check_in_date DATE,
            UNIQUE(user_id, check_in_date),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
    ''')

    # 奖励发放记录表：防止重复发放周期奖励
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bonus_records (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            bonus_type TEXT,
            period TEXT,
            UNIQUE(user_id, bonus_type, period),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
    ''')

    conn.commit()
    conn.close()

# ===================== 辅助工具函数 =====================
# 获取周期时间范围（今日/本周/本月）
def get_time_range(period: str):
    now = datetime.now(TZ)
    if period == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    elif period == "week":
        start = now - timedelta(days=now.weekday())
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=6, hours=23, minutes=59, seconds=59)
    elif period == "month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if start.month == 12:
            next_month = start.replace(year=start.year+1, month=1, day=1)
        else:
            next_month = start.replace(month=start.month+1, day=1)
        end = next_month - timedelta(seconds=1)
    return start, end

# 判断是否为有效发言
def is_valid_message(message) -> bool:
    if not message.text:
        return False
    clean_text = message.text.strip()
    if len(clean_text) < VALID_MESSAGE_MIN_LENGTH:
        return False
    exclude_keywords = ["签到", "今日排名", "本周排名", "本月排名", "我的数据", "添加积分", "减少积分", "添加权限"]
    for keyword in exclude_keywords:
        if clean_text == keyword or clean_text.startswith(f"/{keyword}"):
            return False
    return True

# 更新/新增用户信息到数据库
def update_user_info(user):
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, has_permission FROM users WHERE user_id = %s", (user.id,))
    result = cursor.fetchone()

    if not result:
        is_admin = 1 if user.username and user.username.lower() == INIT_ADMIN_USERNAME.lower() else 0
        cursor.execute('''
            INSERT INTO users (user_id, username, full_name, has_permission)
            VALUES (%s, %s, %s, %s)
        ''', (user.id, user.username, user.full_name, is_admin))
    else:
        is_admin = result[1]
        if user.username and user.username.lower() == INIT_ADMIN_USERNAME.lower():
            is_admin = 1
        cursor.execute('''
            UPDATE users SET username = %s, full_name = %s, has_permission = %s WHERE user_id = %s
        ''', (user.username, user.full_name, is_admin, user.id))

    conn.commit()
    conn.close()

# 检查用户是否有管理权限
def check_user_permission(user_id: int) -> bool:
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute("SELECT has_permission FROM users WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result and result[0] == 1

# 获取用户周期内的有效发言数
def get_user_speech_count(user_id: int, period: str) -> int:
    start, end = get_time_range(period)
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT COUNT(*) FROM message_stats
        WHERE user_id = %s AND message_time BETWEEN %s AND %s AND is_valid = 1
    ''', (user_id, start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")))
    count = cursor.fetchone()[0]
    conn.close()
    return count

# ===================== 核心功能处理函数 =====================
# 群消息主处理函数（统计发言、积分、自动奖励）
async def handle_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type not in ["group", "supergroup"]:
        return

    update_user_info(user)
    user_id = user.id
    now = datetime.now(TZ)

    # 权限/积分管理指令（回复消息触发）
    if message.reply_to_message and message.text:
        target_user = message.reply_to_message.from_user
        if not target_user or target_user.is_bot:
            return
        update_user_info(target_user)
        target_user_id = target_user.id
        text = message.text.strip()

        # 添加权限
        if text == "添加权限":
            if not check_user_permission(user_id):
                await message.reply_text("❌ 操作失败：您没有权限管理功能")
                return
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET has_permission = 1 WHERE user_id = %s", (target_user_id,))
            conn.commit()
            conn.close()
            await message.reply_text(f"✅ 已成功为 @{target_user.username or target_user.full_name} 添积分管理权限")
            return

        # 添加积分
        add_match = re.match(r"^添加积分\s+(\d+)$", text)
        if add_match:
            if not check_user_permission(user_id):
                await message.reply_text("❌ 操作失败：您没有积分管理权限")
                return
            add_points = int(add_match.group(1))
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET points = points + %s WHERE user_id = %s", (add_points, target_user_id))
            conn.commit()
            cursor.execute("SELECT points FROM users WHERE user_id = %s", (target_user_id,))
            new_points = cursor.fetchone()[0]
            conn.close()
            await message.reply_text(f"✅ 已成功为 @{target_user.username or target_user.full_name} 添加 {add_points} 积分\n当前总积分：{new_points}")
            return

        # 减少积分
        reduce_match = re.match(r"^减少积分\s+(\d+)$", text)
        if reduce_match:
            if not check_user_permission(user_id):
                await message.reply_text("❌ 操作失败：您没有积分管理权限")
                return
            reduce_points = int(reduce_match.group(1))
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET points = GREATEST(points - %s, 0) WHERE user_id = %s", (reduce_points, target_user_id))
            conn.commit()
            cursor.execute("SELECT points FROM users WHERE user_id = %s", (target_user_id,))
            new_points = cursor.fetchone()[0]
            conn.close()
            await message.reply_text(f"✅ 已成功为 @{target_user.username or target_user.full_name} 扣除 {reduce_points} 积分\n当前总积分：{new_points}")
            return

    # 有效发言统计与积分增加
    if is_valid_message(message):
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO message_stats (user_id, chat_id, message_time)
            VALUES (%s, %s, %s)
        ''', (user_id, chat.id, now.strftime("%Y-%m-%d %H:%M:%S")))
        cursor.execute("UPDATE users SET points = points + 1 WHERE user_id = %s", (user_id,))
        conn.commit()
        conn.close()

        # 每日达标奖励
        today_date = now.strftime("%Y-%m-%d")
        today_count = get_user_speech_count(user_id, "today")
        if today_count > DAILY_SPEECH_TARGET:
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id FROM bonus_records
                WHERE user_id = %s AND bonus_type = 'daily' AND period = %s
            ''', (user_id, today_date))
            if not cursor.fetchone():
                cursor.execute("UPDATE users SET points = points + %s WHERE user_id = %s", (DAILY_BONUS_POINTS, user_id))
                cursor.execute('''
                    INSERT INTO bonus_records (user_id, bonus_type, period)
                    VALUES (%s, 'daily', %s)
                ''', (user_id, today_date))
                conn.commit()
                cursor.execute("SELECT points FROM users WHERE user_id = %s", (user_id,))
                new_points = cursor.fetchone()[0]
                conn.close()
                await message.reply_text(f"🎉 恭喜您！今日有效发言突破{DAILY_SPEECH_TARGET}条，获得{DAILY_BONUS_POINTS}积分奖励\n当前总积分：{new_points}")
            else:
                conn.close()

        # 每周达标奖励
        week_period = now.strftime("%Y-%W")
        week_count = get_user_speech_count(user_id, "week")
        if week_count > WEEKLY_SPEECH_TARGET:
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id FROM bonus_records
                WHERE user_id = %s AND bonus_type = 'weekly' AND period = %s
            ''', (user_id, week_period))
            if not cursor.fetchone():
                cursor.execute("UPDATE users SET points = points + %s WHERE user_id = %s", (WEEKLY_BONUS_POINTS, user_id))
                cursor.execute('''
                    INSERT INTO bonus_records (user_id, bonus_type, period)
                    VALUES (%s, 'weekly', %s)
                ''', (user_id, week_period))
                conn.commit()
                cursor.execute("SELECT points FROM users WHERE user_id = %s", (user_id,))
                new_points = cursor.fetchone()[0]
                conn.close()
                await message.reply_text(f"🏆 恭喜您！本周有效发言突破{WEEKLY_SPEECH_TARGET}条，获得{WEEKLY_BONUS_POINTS}积分奖励\n当前总积分：{new_points}")
            else:
                conn.close()

# 签到功能
async def sign_in(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or user.is_bot:
        return
    update_user_info(user)
    user_id = user.id
    today_date = datetime.now(TZ).strftime("%Y-%m-%d")

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM check_in WHERE user_id = %s AND check_in_date = %s", (user_id, today_date))
    if cursor.fetchone():
        await message.reply_text("✅ 您今日已完成签到，请勿重复操作")
        conn.close()
        return
    cursor.execute("INSERT INTO check_in (user_id, check_in_date) VALUES (%s, %s)", (user_id, today_date))
    cursor.execute("UPDATE users SET points = points + %s WHERE user_id = %s", (SIGN_IN_POINTS, user_id))
    conn.commit()
    cursor.execute("SELECT points FROM users WHERE user_id = %s", (user_id,))
    new_points = cursor.fetchone()[0]
    conn.close()

    await message.reply_text(f"✅ 签到成功！获得{SIGN_IN_POINTS}积分\n当前总积分：{new_points}")

# 个人数据查询
async def get_my_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or user.is_bot:
        return
    update_user_info(user)
    user_id = user.id

    today_count = get_user_speech_count(user_id, "today")
    week_count = get_user_speech_count(user_id, "week")
    month_count = get_user_speech_count(user_id, "month")
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute("SELECT points FROM users WHERE user_id = %s", (user_id,))
    total_points = cursor.fetchone()[0]
    conn.close()

    text = f"📊 您的个人数据统计\n\n"
    text += f"👤 用户：@{user.username or user.full_name}\n"
    text += f"💰 当前总积分：{total_points}\n\n"
    text += f"📅 今日有效发言：{today_count} 条\n"
    text += f"📆 本周有效发言：{week_count} 条\n"
    text += f"📅 本月有效发言：{month_count} 条"

    await message.reply_text(text)

# 排名查询通用函数
async def get_rank(update: Update, context: ContextTypes.DEFAULT_TYPE, period: str, title: str):
    chat = update.effective_chat
    message = update.effective_message
    if not chat or chat.type not in ["group", "supergroup"]:
        return

    start, end = get_time_range(period)
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT u.user_id, u.username, u.full_name, COUNT(m.id) as speech_count
        FROM message_stats m
        LEFT JOIN users u ON m.user_id = u.user_id
        WHERE m.chat_id = %s AND m.message_time BETWEEN %s AND %s AND m.is_valid = 1
        GROUP BY m.user_id, u.user_id, u.username, u.full_name
        ORDER BY speech_count DESC
        LIMIT %s
    ''', (chat.id, start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S"), RANK_SHOW_LIMIT))
    rank_list = cursor.fetchall()
    conn.close()

    text = f"🏆 {title} 有效发言排行榜\n\n"
    if not rank_list:
        text += "暂无有效发言数据"
    else:
        for idx, (_, username, full_name, count) in enumerate(rank_list, 1):
            display_name = f"@{username}" if username else full_name
            text += f"第{idx}名：{display_name} | {count} 条\n"

    await message.reply_text(text)

# 排名指令
async def today_rank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await get_rank(update, context, "today", "今日")
async def week_rank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await get_rank(update, context, "week", "本周")
async def month_rank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await get_rank(update, context, "month", "本月")

# ===================== 机器人启动入口 =====================
def main():
    if not BOT_TOKEN:
        raise ValueError("请在Railway环境变量中配置BOT_TOKEN")
    if not DATABASE_URL:
        raise ValueError("请在Railway环境变量中配置DATABASE_URL")
    init_db()
    print("数据库初始化完成，机器人启动中...")

    # 创建机器人应用
    application = Application.builder().token(BOT_TOKEN).build()

    # 仅保留英文指令（符合Telegram API规范）
    application.add_handler(CommandHandler("sign", sign_in))
    application.add_handler(CommandHandler("mystats", get_my_stats))
    application.add_handler(CommandHandler("todayrank", today_rank))
    application.add_handler(CommandHandler("weekrank", week_rank))
    application.add_handler(CommandHandler("monthrank", month_rank))

    # 中文关键词触发处理器（完全兼容原功能，不触发报错）
    application.add_handler(MessageHandler(filters.Regex(r"^签到$") & filters.ChatType.GROUPS, sign_in))
    application.add_handler(MessageHandler(filters.Regex(r"^我的数据$") & filters.ChatType.GROUPS, get_my_stats))
    application.add_handler(MessageHandler(filters.Regex(r"^今日排名$") & filters.ChatType.GROUPS, today_rank))
    application.add_handler(MessageHandler(filters.Regex(r"^本周排名$") & filters.ChatType.GROUPS, week_rank))
    application.add_handler(MessageHandler(filters.Regex(r"^本月排名$") & filters.ChatType.GROUPS, month_rank))

    # 注册群消息通用处理器（必须放在最后）
    application.add_handler(MessageHandler(filters.ChatType.GROUPS & ~filters.StatusUpdate.ALL, handle_group_message))

    # 启动长轮询（Railway兼容模式，忽略未处理更新）
    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
