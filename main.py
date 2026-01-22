import os
import asyncio
import secrets
import time

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import CommandStart, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

from aiogram.exceptions import (
    TelegramRetryAfter,
    TelegramForbiddenError,
    TelegramBadRequest,
    TelegramNotFound,
)

import psycopg
from psycopg_pool import AsyncConnectionPool


# =========================
# CONFIG (Railway Variables)
# =========================
BOT_TOKEN = (os.getenv("BOT_TOKEN") or "7014624336:AAHpS83ifdvuy5PiZ9cUTjB9SKRIWznCxnI").strip()
CHANNEL_ID = int((os.getenv("CHANNEL_ID") or "-1002442552494").strip() or "0")
BOT_USERNAME = (os.getenv("BOT_USERNAME") or "hepini_notif_bot").strip().lstrip("@")
DATABASE_URL = (os.getenv("DATABASE_URL") or "postgresql://postgres:dgpQXfnVKQcDNnRgOIuZBYnVaXmqHOGF@interchange.proxy.rlwy.net:52980/railway").strip()

OWNER_IDS = set()
_raw_owner = (os.getenv("OWNER_IDS") or "6016383456").strip()
for part in _raw_owner.split(","):
    part = part.strip()
    if part:
        OWNER_IDS.add(int(part))

BROADCAST_RATE = float((os.getenv("BROADCAST_RATE") or "20").strip())   # msg/sec
BROADCAST_BATCH = int((os.getenv("BROADCAST_BATCH") or "2000").strip()) # fetch per batch

# =========================
# REQUIRED JOIN CHANNELS (MAX 5)
# id bisa int (-100xxx) atau username "@channel"
# =========================
REQUIRED_CHANNELS = [
    {"id": "-1002268843879", "name": "HEPINI OFFICIAL", "url": "https://t.me/hepiniofc"},
    {"id": "-1003692828104", "name": "Ruang Backup", "url": "https://t.me/hepini_ofcl"},
    # maksimal 5 item
]

# =========================
# VALIDATION
# =========================
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN belum di-set.")
if CHANNEL_ID == 0:
    raise RuntimeError("CHANNEL_ID belum di-set.")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL belum di-set.")
if not OWNER_IDS:
    raise RuntimeError("OWNER_IDS belum di-set.")
if len(REQUIRED_CHANNELS) > 5:
    raise RuntimeError("REQUIRED_CHANNELS maksimal 5 item.")


# =========================
# HELPERS
# =========================
def is_owner(user_id: int) -> bool:
    return user_id in OWNER_IDS

def make_slug() -> str:
    return secrets.token_urlsafe(8).replace("-", "").replace("_", "")[:12]

def gate_text() -> str:
    if not REQUIRED_CHANNELS:
        return ""
    names = "\n".join([f"• {c['name']}" for c in REQUIRED_CHANNELS])
    return (
        "⚠️ Untuk melanjutkan, kamu wajib join dulu ke channel berikut:\n\n"
        f"{names}\n\n"
        "Setelah join, klik tombol **✅ Saya sudah join**."
    )

def join_keyboard(slug: str | None):
    kb = InlineKeyboardBuilder()
    for ch in REQUIRED_CHANNELS:
        kb.button(text=f"Join {ch['name']}", url=ch["url"])
    cb = f"check_join:{slug}" if slug else "check_join:"
    kb.button(text="✅ Saya sudah join", callback_data=cb)
    kb.adjust(1)
    return kb.as_markup()

async def is_joined_all(bot: Bot, user_id: int) -> bool:
    if not REQUIRED_CHANNELS:
        return True

    for ch in REQUIRED_CHANNELS:
        chat = ch["id"]
        try:
            member = await bot.get_chat_member(chat_id=chat, user_id=user_id)
            status = getattr(member, "status", None)
            if status in ("left", "kicked") or status is None:
                return False
        except Exception:
            # bot gak bisa cek membership (umumnya bot tidak ada di channel private tsb)
            return False

    return True

class RateLimiter:
    """Global rate limiter: target N msg/sec."""
    def __init__(self, per_sec: float):
        self.min_interval = 1.0 / max(per_sec, 1.0)
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def wait(self):
        async with self._lock:
            now = time.monotonic()
            wait_for = self.min_interval - (now - self._last)
            if wait_for > 0:
                await asyncio.sleep(wait_for)
            self._last = time.monotonic()


# =========================
# DB SCHEMA
# =========================
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS files (
  slug TEXT PRIMARY KEY,
  channel_id BIGINT NOT NULL,
  channel_message_id BIGINT NOT NULL,
  uploaded_by BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users (
  user_id BIGINT PRIMARY KEY,
  username TEXT,
  first_name TEXT,
  last_name TEXT,
  last_start TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_users_last_start ON users(last_start);
CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id);
"""


# =========================
# MAIN
# =========================
async def main():
    # ✅ IMPORTANT: create pool inside loop / open later
    pool = AsyncConnectionPool(
        conninfo=DATABASE_URL,
        min_size=1,
        max_size=10,
        timeout=30,
        open=False,  # <= FIX utama
    )
    await pool.open()

    async def db_init():
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(SCHEMA_SQL)
            await conn.commit()

    async def db_upsert_user(user_id: int, username: str | None, first_name: str | None, last_name: str | None):
        sql = """
        INSERT INTO users (user_id, username, first_name, last_name, last_start)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (user_id) DO UPDATE SET
          username = EXCLUDED.username,
          first_name = EXCLUDED.first_name,
          last_name = EXCLUDED.last_name,
          last_start = NOW();
        """
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, (user_id, username, first_name, last_name))
            await conn.commit()

    async def db_count_users() -> int:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM users;")
                row = await cur.fetchone()
                return int(row[0]) if row else 0

    async def db_delete_user(user_id: int):
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM users WHERE user_id = %s;", (user_id,))
            await conn.commit()

    async def db_put_file(slug: str, channel_id: int, channel_message_id: int, uploaded_by: int):
        sql = """
        INSERT INTO files (slug, channel_id, channel_message_id, uploaded_by)
        VALUES (%s, %s, %s, %s);
        """
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, (slug, channel_id, channel_message_id, uploaded_by))
            await conn.commit()

    async def db_get_file(slug: str):
        sql = "SELECT channel_id, channel_message_id FROM files WHERE slug = %s;"
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, (slug,))
                row = await cur.fetchone()
                if not row:
                    return None
                return int(row[0]), int(row[1])

    async def db_iter_user_ids(batch_size: int = 2000):
        # tanpa OFFSET (lebih aman untuk 100k+)
        last_id = 0
        while True:
            sql = """
            SELECT user_id
            FROM users
            WHERE user_id > %s
            ORDER BY user_id ASC
            LIMIT %s;
            """
            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql, (last_id, batch_size))
                    rows = await cur.fetchall()

            if not rows:
                break

            for (uid,) in rows:
                uid = int(uid)
                yield uid
                last_id = uid

    await db_init()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    limiter = RateLimiter(BROADCAST_RATE)

    async def send_file_to_user(user_chat_id: int, slug: str, origin_msg: Message | None = None):
        found = await db_get_file(slug)
        if not found:
            text = "❌ File tidak ditemukan / link sudah tidak valid."
            if origin_msg:
                await origin_msg.answer(text)
            else:
                await bot.send_message(user_chat_id, text)
            return

        ch_id, ch_msg_id = found
        try:
            await bot.copy_message(
                chat_id=user_chat_id,
                from_chat_id=ch_id,
                message_id=ch_msg_id,
            )
        except Exception as e:
            text = f"❌ Gagal mengirim file. ({type(e).__name__})"
            if origin_msg:
                await origin_msg.answer(text)
            else:
                await bot.send_message(user_chat_id, text)

    # -------- /start --------
    @dp.message(CommandStart())
    async def start_handler(message: Message):
        if message.from_user:
            await db_upsert_user(
                user_id=message.from_user.id,
                username=message.from_user.username,
                first_name=message.from_user.first_name,
                last_name=message.from_user.last_name,
            )

        parts = (message.text or "").split(maxsplit=1)
        slug = parts[1].strip() if len(parts) > 1 else None
        uid = message.from_user.id if message.from_user else 0

        # gate join untuk non-owner
        if (not is_owner(uid)) and REQUIRED_CHANNELS:
            ok = await is_joined_all(bot, uid)
            if not ok:
                await message.answer(gate_text(), reply_markup=join_keyboard(slug), parse_mode="Markdown")
                return

        if not slug:
            await message.answer(
                "📦 Kirim file ke bot ini (khusus owner).\n"
                "Kalau kamu punya link file, buka dari link tersebut ya."
            )
            return

        await send_file_to_user(message.chat.id, slug, origin_msg=message)

    # -------- callback join verify --------
    @dp.callback_query(F.data.startswith("check_join"))
    async def check_join_cb(call: CallbackQuery):
        uid = call.from_user.id if call.from_user else 0
        data = call.data or "check_join:"
        slug = data.split(":", 1)[1].strip() if ":" in data else ""

        if is_owner(uid):
            await call.answer("✅ Owner bypass", show_alert=False)
            if slug:
                try:
                    await call.message.delete()
                except Exception:
                    pass
                await send_file_to_user(uid, slug)
            else:
                try:
                    await call.message.edit_text("✅ Kamu owner.")
                except Exception:
                    pass
            return

        ok = await is_joined_all(call.bot, uid)
        if not ok:
            await call.answer("Masih belum join semua channel.", show_alert=True)
            return

        await call.answer("✅ Verifikasi berhasil!", show_alert=False)

        if slug:
            try:
                await call.message.delete()
            except Exception:
                pass
            await send_file_to_user(uid, slug)
            return

        try:
            await call.message.edit_text("✅ Verifikasi berhasil.")
        except Exception:
            pass

    # -------- /users (owner) --------
    @dp.message(Command("users"))
    async def users_cmd(message: Message):
        uid = message.from_user.id if message.from_user else 0
        if not is_owner(uid):
            return
        total = await db_count_users()
        await message.answer(f"👤 Total user tersimpan: {total}")

    # -------- /broadcast (owner, reply) --------
    @dp.message(Command("broadcast"))
    async def broadcast_cmd(message: Message):
        uid = message.from_user.id if message.from_user else 0
        if not is_owner(uid):
            return

        if not message.reply_to_message:
            await message.answer("Cara pakai:\nReply pesan/file yang mau dikirim, lalu ketik /broadcast")
            return

        total = await db_count_users()
        if total <= 0:
            await message.answer("Database user masih kosong.")
            return

        await message.answer(
            f"🚀 Broadcast dimulai ke {total} user.\n"
            f"Rate: ~{int(BROADCAST_RATE)} msg/detik.\n"
            "User yang block bot / invalid akan DIHAPUS dari database."
        )

        sent = 0
        deleted = 0
        failed = 0
        processed = 0

        src_chat_id = message.chat.id
        src_msg_id = message.reply_to_message.message_id

        async for target_id in db_iter_user_ids(batch_size=BROADCAST_BATCH):
            processed += 1
            await limiter.wait()

            try:
                await bot.copy_message(
                    chat_id=target_id,
                    from_chat_id=src_chat_id,
                    message_id=src_msg_id,
                )
                sent += 1

            except TelegramRetryAfter as e:
                await asyncio.sleep(float(e.retry_after) + 0.5)
                try:
                    await bot.copy_message(
                        chat_id=target_id,
                        from_chat_id=src_chat_id,
                        message_id=src_msg_id,
                    )
                    sent += 1
                except (TelegramForbiddenError, TelegramNotFound, TelegramBadRequest):
                    await db_delete_user(target_id)
                    deleted += 1
                except Exception:
                    failed += 1

            except (TelegramForbiddenError, TelegramNotFound, TelegramBadRequest):
                await db_delete_user(target_id)
                deleted += 1

            except Exception:
                failed += 1

            if processed % 1000 == 0:
                await message.answer(
                    f"Progress: {processed}/{total}\n"
                    f"✅ Terkirim: {sent}\n"
                    f"🗑️ Dihapus: {deleted}\n"
                    f"⚠️ Gagal lain: {failed}"
                )

        await message.answer(
            "✅ Broadcast selesai.\n"
            f"✅ Terkirim: {sent}\n"
            f"🗑️ Dihapus: {deleted}\n"
            f"⚠️ Gagal lain: {failed}\n"
            f"🎯 Total target awal: {total}"
        )

    # -------- owner upload --------
    @dp.message(
        F.content_type.in_({"document", "video", "audio", "voice", "photo", "animation", "sticker"})
        | F.video_note
    )
    async def upload_handler(message: Message):
        uid = message.from_user.id if message.from_user else 0
        if not is_owner(uid):
            await message.answer("⛔ Kamu tidak punya akses upload.")
            return

        # Ambil "nama file/judul" yang paling masuk akal dari pesan
        def extract_title(m: Message) -> str:
            if m.document:
                return m.document.file_name or "document"
            if m.video:
                return m.video.file_name or "video"
            if m.audio:
                # audio kadang gak punya file_name tapi punya title/performer
                if m.audio.file_name:
                    return m.audio.file_name
                if m.audio.performer and m.audio.title:
                    return f"{m.audio.performer} - {m.audio.title}"
                if m.audio.title:
                    return m.audio.title
                return "audio"
            if m.voice:
                return "voice"
            if m.video_note:
                return "video_note"
            if m.photo:
                return "photo"
            if m.animation:
                return m.animation.file_name or "animation"
            if m.sticker:
                # sticker biasanya ga ada file name
                return f"sticker ({m.sticker.emoji or '🙂'})"
            return "file"

        file_title = extract_title(message)

        try:
            copied = await bot.copy_message(
                chat_id=CHANNEL_ID,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
            )
        except Exception as e:
            await message.answer(f"❌ Gagal menyimpan ke channel DB. ({type(e).__name__})")
            return

        slug = make_slug()
        for _ in range(3):
            try:
                await db_put_file(slug, int(CHANNEL_ID), int(copied.message_id), int(uid))
                break
            except psycopg.errors.UniqueViolation:
                slug = make_slug()
        else:
            await message.answer("❌ Gagal membuat slug unik. Coba lagi.")
            return

        if BOT_USERNAME:
            link = f"https://t.me/{BOT_USERNAME}?start={slug}"
            await message.answer(
                "✅ Tersimpan!\n"
                f"Nama file: {file_title}\n"
                "🔗 Link publik:\n"
                f"{link}"
            )
        else:
            await message.answer(
                "✅ Tersimpan!\n"
                f"Nama file: {file_title}\n"
                f"Slug: {slug}\n"
                "(Set BOT_USERNAME untuk link otomatis)"
            )

    @dp.message()
    async def fallback(message: Message):
        uid = message.from_user.id if message.from_user else 0
        if is_owner(uid):
            await message.answer("Kirim file untuk disimpan, atau reply lalu /broadcast.")
        else:
            await message.answer("Buka link file yang kamu punya ya (t.me/<bot>?start=...).")

    try:
        await dp.start_polling(bot)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
