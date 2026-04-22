import asyncio
import sys
import traceback
import time
from datetime import datetime
from vkbottle.bot import Bot, Message
from vkbottle import Keyboard, Callback, KeyboardButtonColor, OpenLink, VKAPIError, PhotoMessageUploader
from config import (
    TOKEN, GROUP_ID, ADMIN_IDS, APP_URL,
    MODERATION_ACCESS_MINUTES, MODERATION_MODE, MODERATION_SECRET
)
from database import (
    db_init, get_material, get_next_ticket_id, get_product_at, 
    log_purchase, get_product_by_id, register_user, get_stats, 
    add_ticket, get_open_tickets, close_ticket, add_keyword,
    get_all_keywords, delete_keyword_by_id, update_keyword_fields,
    get_latest_user_question, add_comment_log, has_commented_recently,
    create_order, get_paid_undelivered_orders, mark_order_delivered
)
from payment_server import start_payment_server
from robokassa import build_payment_url, is_configured as robokassa_is_configured

bot = Bot(TOKEN)
photo_uploader = PhotoMessageUploader(bot.api)

# Генерируем уникальный random_id для каждого сообщения
def get_rand():
    import random
    return random.getrandbits(31)


def extract_market_amount(market):
    price = getattr(market, "price", None)
    if price is None:
        return None
    amount = getattr(price, "amount", None)
    if amount is not None:
        try:
            return float(amount) / 100
        except (TypeError, ValueError):
            pass
    text = getattr(price, "text", "") or ""
    digits = "".join(ch for ch in text.replace(",", ".") if ch.isdigit() or ch == ".")
    try:
        return float(digits) if digits else None
    except ValueError:
        return None


async def process_paid_orders():
    while True:
        try:
            for order_id, user_id, product_name, amount in get_paid_undelivered_orders():
                await deliver_product(user_id, product_name)
                mark_order_delivered(order_id)
        except Exception as e:
            print(f"Paid order processing error: {e}")
            traceback.print_exc()
        await asyncio.sleep(10)

# Кнопка возврата в каталог
keyboard_main = (
    Keyboard(inline=True)
    .add(OpenLink(link=APP_URL, label="🧘‍♀️ Весь каталог"))
    .get_json()
)


async def deliver_product(user_id, product_name):
    """Выдает контент товара пользователю"""
    print(f"DEBUG: Пытаемся выдать товар '{product_name}' пользователю {user_id}")
    try:
        content = get_product_at(product_name)
        if not content:
            msg = f"🚨 ТОВАР НЕ НАЙДЕН!\nID{user_id} купил '{product_name}', но в базе пусто. Марианна, посмотри!"
            for admin_id in ADMIN_IDS:
                await bot.api.messages.send(peer_id=admin_id, message=msg, random_id=get_rand())
            
            user_msg = f"✅ Оплата «{product_name}» прошла! Марианна пришлет ссылку вручную в ближайшее время. 😊"
            await bot.api.messages.send(peer_id=user_id, message=user_msg, keyboard=keyboard_main, random_id=get_rand())
            return

        text, attachment = content
        msg = f"🎉 Спасибо! Ваши материалы по программе «{product_name}»:\n{text}\nЗанимайтесь с удовольствием! ✨"
        
        # Отправляем только если вложение реально есть
        params = {"peer_id": user_id, "message": msg, "keyboard": keyboard_main, "random_id": get_rand()}
        if attachment:
            params["attachment"] = attachment
            
        await bot.api.messages.send(**params)
        log_purchase(user_id, product_name)
        print(f"SUCCESS: Товар '{product_name}' выдан пользователю {user_id}")
    except Exception as e:
        print(f"CRITICAL ERROR in deliver_product: {e}")
        traceback.print_exc()

async def get_user_greeting(user_id):
    """Определяет приветствие по времени суток и получает имя"""
    try:
        users = await bot.api.users.get(user_ids=[user_id])
        name = users[0].first_name
    except:
        name = "Друг"
    
    hour = datetime.now().hour
    if 4 <= hour < 11:
        greet = "доброе утро! ☀️"
    elif 11 <= hour < 16:
        greet = "добрый день! 🌤️"
    elif 16 <= hour < 23:
        greet = "добрый вечер! 🌆"
    else:
        greet = "доброго времени ночи! 🌙"
    
    return f"{name}, {greet}"

async def check_user_subscriptions(user_id):
    """Проверяет подписку на группу и включенные уведомления. Возвращает (is_member, is_allowed)"""
    try:
        # Проверка подписки на группу
        # groups.is_member для одного юзера возвращает 1 или 0
        resp_member = await bot.api.groups.is_member(group_id=GROUP_ID, user_id=user_id)
        is_member = bool(resp_member)
        
        # Проверка разрешенных сообщений
        resp_allowed = await bot.api.messages.is_messages_from_group_allowed(group_id=GROUP_ID, user_id=user_id)
        # У vkbottle это обычно объект с полем is_allowed
        is_allowed = bool(getattr(resp_allowed, "is_allowed", resp_allowed))
        
        return is_member, is_allowed
    except Exception as e:
        print(f"Sub check error for {user_id}: {e}")
        return True, True # Если ошибка API, не блокируем

async def send_sub_request(user_id, is_member, is_allowed, greeting="", edit_cmid=None):
    """Отправляет или редактирует просьбу подписаться и/или включить уведомления"""
    base_msg = "Остался последний маленький шаг к получению материалов! ✨\n\n"
    
    if not is_member:
        req_msg = (
            "Чтобы бот мог отправить Вам материалы, пожалуйста:\n"
            "1. Подпишитесь на наше сообщество\n"
            "2. Включите уведомления (колокольчик 🔔)\n\n"
        )
    else:
        req_msg = "Чтобы бот мог отправить Вам материалы, пожалуйста включите уведомления (колокольчик 🔔)\n\n"

    # Если уже нажимали, но не подписались — чуть меняем текст
    if edit_cmid:
        fail_prefix = "Кажется, какой-то из шагов пропущен... 🤫\n\n"
        full_msg = f"{greeting}\n\n{fail_prefix}{req_msg}Как только всё сделаете — жмите кнопку еще раз!"
    else:
        full_msg = f"{greeting}\n\n{base_msg}{req_msg}После этого нажмите кнопку ниже:"
    
    # Загружаем картинку только для нового сообщения (в редактировании она останется)
    photo = None
    if not edit_cmid:
        try:
            photo = await photo_uploader.upload("картинки/уведомления.jpg")
        except: pass

    if edit_cmid:
        try:
            await bot.api.messages.edit(
                peer_id=user_id,
                message=full_msg,
                conversation_message_id=edit_cmid,
                keyboard=get_sub_kb()
            )
        except:
            # Если не вышло отредактировать (например сообщение старое), просто шлем новое
            await bot.api.messages.send(peer_id=user_id, message=full_msg, keyboard=get_sub_kb(), random_id=get_rand())
    else:
        await bot.api.messages.send(
            peer_id=user_id, 
            message=full_msg, 
            attachment=photo, 
            keyboard=get_sub_kb(), 
            random_id=get_rand()
        )

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

async def clear_chat(peer_id, cmids):
    """Отключаем удаление сообщений, чтобы сохранить контекст беседы"""
    return

# Храним состояния и временные данные
# USER_STATES = {user_id: "state_name"}
# ADMIN_DATA = {user_id: {"word": "...", "msg_id": 123, "edit_kw_id": 1}}
USER_STATES = {}
ADMIN_DATA = {}
MODERATION_ACCESS = {}


def has_moderation_access(user_id):
    if user_id in ADMIN_IDS:
        return True
    expires_at = MODERATION_ACCESS.get(user_id, 0)
    if expires_at <= time.time():
        MODERATION_ACCESS.pop(user_id, None)
        return False
    return True


def grant_moderation_access(user_id):
    expires_at = time.time() + MODERATION_ACCESS_MINUTES * 60
    MODERATION_ACCESS[user_id] = expires_at
    return datetime.fromtimestamp(expires_at).strftime("%H:%M")


def get_payment_unavailable_kb():
    return (
        Keyboard(inline=True)
        .add(OpenLink(link=APP_URL, label="Открыть каталог"))
        .row()
        .add(Callback("Задать вопрос", {"cmd": "support_menu"}), color=KeyboardButtonColor.PRIMARY)
        .get_json()
    )

# --- КЛАВИАТУРЫ АДМИНА ---

def get_admin_main_kb():
    return (
        Keyboard(inline=True)
        .add(Callback("Статистика", {"admin": "stats"}), color=KeyboardButtonColor.PRIMARY).row()
        .add(Callback("Обращения", {"admin": "tickets"}), color=KeyboardButtonColor.PRIMARY).row()
        .add(Callback("Управление кодовыми словами", {"admin": "keywords"}), color=KeyboardButtonColor.SECONDARY)
        .get_json()
    )

def get_sub_kb():
    return (
        Keyboard(inline=True)
        .add(Callback("✅ Всё готово!", {"cmd": "check_sub_again"}), color=KeyboardButtonColor.POSITIVE)
        .get_json()
    )

def get_kw_menu_kb():
    return (
        Keyboard(inline=True)
        .add(Callback("Добавить слово", {"admin": "kw_add_start"}), color=KeyboardButtonColor.POSITIVE).row()
        .add(Callback("Весь список", {"admin": "kw_list"}), color=KeyboardButtonColor.PRIMARY).row()
        .add(Callback("Вернуться назад", {"admin": "main"}), color=KeyboardButtonColor.SECONDARY)
        .get_json()
    )

def get_back_kb(target="main"):
    return (
        Keyboard(inline=True)
        .add(Callback("Вернуться назад", {"admin": target}), color=KeyboardButtonColor.SECONDARY)
        .get_json()
    )

def get_user_main_kb():
    return (
        Keyboard(inline=True)
        .add(OpenLink(link=APP_URL, label="🧘‍♀️ Весь каталог")).row()
        .add(Callback("❓ Есть вопрос", {"cmd": "support_menu"}), color=KeyboardButtonColor.PRIMARY)
        .get_json()
    )

def get_support_kb():
    # Список из 4 частых вопросов
    return (
        Keyboard(inline=True)
        .add(Callback("Как получить доступ к урокам?", {"faq": 1}), color=KeyboardButtonColor.SECONDARY).row()
        .add(Callback("Где найти оплаченные курсы?", {"faq": 2}), color=KeyboardButtonColor.SECONDARY).row()
        .add(Callback("Можно ли заниматься беременным?", {"faq": 3}), color=KeyboardButtonColor.SECONDARY).row()
        .add(Callback("Как работает личный кабинет?", {"faq": 4}), color=KeyboardButtonColor.SECONDARY).row()
        .add(Callback("✍ Другой вопрос", {"cmd": "ask_custom"}), color=KeyboardButtonColor.PRIMARY)
        .get_json()
    )

def get_faq_back_kb():
    return (
        Keyboard(inline=True)
        .add(Callback("🔙 Назад к списку", {"cmd": "support_menu_edit"}), color=KeyboardButtonColor.SECONDARY).row()
        .add(Callback("✍ Другой вопрос", {"cmd": "ask_custom"}), color=KeyboardButtonColor.PRIMARY)
        .get_json()
    )

def get_post_reply_kb():
    return (
        Keyboard(inline=True)
        .add(Callback("❓ Задать вопрос еще", {"cmd": "ask_custom"}), color=KeyboardButtonColor.PRIMARY).row()
        .add(Callback("🙏 Спасибо за ответ!", {"cmd": "thanks"}), color=KeyboardButtonColor.POSITIVE)
        .get_json()
    )

def get_edit_kw_kb():
    return (
        Keyboard(inline=True)
        .add(Callback("Изменить слово", {"admin": "kw_edit_word"}), color=KeyboardButtonColor.PRIMARY).row()
        .add(Callback("Изменить текст", {"admin": "kw_edit_text"}), color=KeyboardButtonColor.PRIMARY).row()
        .add(Callback("Удалить команду", {"admin": "kw_delete_confirm"}), color=KeyboardButtonColor.NEGATIVE).row()
        .add(Callback("Вернуться назад", {"admin": "kw_list"}), color=KeyboardButtonColor.SECONDARY)
        .get_json()
    )

@bot.on.message()
async def main_handler(message: Message):
    user_id = message.from_id
    text = message.text.strip()
    cmid = message.conversation_message_id
    register_user(user_id)

    if MODERATION_MODE and text.lower() == MODERATION_SECRET:
        expires_at = grant_moderation_access(user_id)
        await message.answer(
            f"Тестовый доступ к сценарию оплаты включен до {expires_at}.\n\n"
            "Теперь откройте товар в сообществе и нажмите «Написать продавцу» — бот создаст тестовую ссылку оплаты Robokassa.",
            keyboard=get_user_main_kb()
        )
        return

    if text.lower() in ["каталог", "товары", "купить", "оплата"]:
        await message.answer(
            "Каталог программ доступен в мини-приложении. Выберите товар и нажмите кнопку покупки в карточке или в разделе товаров сообщества.",
            keyboard=get_user_main_kb()
        )
        return
    
    # --- ЛОГИКА АДМИНА ---
    if user_id in ADMIN_IDS:
        state = USER_STATES.get(user_id)
        
        # Обработка ввода ответа на вопрос
        if state and state.startswith("admin_wait_reply_"):
            parts = state.split("_")
            target_id = int(parts[4])
            t_id = int(parts[3])
            
            try:
                # Ищем текст вопроса клиента
                q_data = get_latest_user_question(target_id)
                q_text = q_data[0] if q_data else "ваш вопрос"
                
                await bot.api.messages.send(
                    peer_id=target_id, 
                    message=f"✉ Ответ на вопрос «{q_text}»:\n\n{text}", 
                    keyboard=get_post_reply_kb(),
                    random_id=get_rand()
                )
                close_ticket(t_id)
                USER_STATES[user_id] = None
                await message.answer(f"✅ Ответ отправлен пользователю [id{target_id}|@id{target_id}]")
                return
            except Exception as e:
                print(f"Reply error: {e}")
                await message.answer("Ошибка при отправке ответа.")
                return

        # Старая команда (оставляем для совместимости, но чистим от звезд)
        if text.lower().startswith("/ответ "):
            try:
                parts = text.split(" ", 2)
                target_id = int(parts[1])
                reply_text = parts[2]
                q_data = get_latest_user_question(target_id)
                q_text = q_data[0] if q_data else "ваш вопрос"
                t_id = q_data[1] if q_data else None
                
                await bot.api.messages.send(
                    peer_id=target_id, 
                    message=f"✉ Ответ на вопрос «{q_text}»:\n\n{reply_text}", 
                    keyboard=get_post_reply_kb(),
                    random_id=get_rand()
                )
                if t_id: close_ticket(t_id)
                await message.answer(f"✅ Ответ отправлен пользователю [id{target_id}|@id{target_id}]")
                return
            except:
                await message.answer("Ошибка! Формат: /ответ [ID] [Текст]")
                return

        state = USER_STATES.get(user_id)
        if state == "kw_wait_word":
            last_bot_msg = ADMIN_DATA.get(user_id, {}).get("msg_id")
            await clear_chat(user_id, [cmid, last_bot_msg])
            ADMIN_DATA[user_id] = ADMIN_DATA.get(user_id, {}); ADMIN_DATA[user_id]["word"] = text.upper()
            USER_STATES[user_id] = "kw_wait_text"
            sent = await message.answer("Напишите текст сообщения и ссылку на урок", keyboard=Keyboard(inline=True).add(Callback("Отменить", {"admin": "main"}), color=KeyboardButtonColor.NEGATIVE).get_json())
            ADMIN_DATA[user_id]["msg_id"] = sent.conversation_message_id
            return

        if state == "kw_wait_text":
            word = ADMIN_DATA[user_id]["word"]; await clear_chat(user_id, [cmid, ADMIN_DATA[user_id].get("msg_id")])
            add_keyword(word, text, ""); USER_STATES[user_id] = None
            await message.answer(f"Новая команда добавлена:\nСлово: {word}\nТекст: {text}", keyboard=get_admin_main_kb())
            return

        if state == "kw_wait_index":
            await clear_chat(user_id, [cmid, ADMIN_DATA.get(user_id, {}).get("msg_id")])
            try:
                idx = int(text) - 1; kws = get_all_keywords()
                if 0 <= idx < len(kws):
                    kw = kws[idx]; ADMIN_DATA[user_id]["edit_kw_id"] = kw[0]; USER_STATES[user_id] = None
                    sent = await message.answer(f"Редактирование\nСлово: {kw[1].upper()}\nТекст: {kw[2]}", keyboard=get_edit_kw_kb())
                    ADMIN_DATA[user_id]["msg_id"] = sent.conversation_message_id
                else: await message.answer("Неверный номер.", keyboard=get_back_kb("kw_list"))
            except: pass
            return

        if state == "kw_wait_edit_word":
            await clear_chat(user_id, [cmid, ADMIN_DATA.get(user_id, {}).get("msg_id")])
            update_keyword_fields(ADMIN_DATA[user_id]["edit_kw_id"], word=text); USER_STATES[user_id] = None
            await message.answer(f"Слово изменено: {text.upper()}", keyboard=get_back_kb("kw_list"))
            return

        if state == "kw_wait_edit_text":
            await clear_chat(user_id, [cmid, ADMIN_DATA.get(user_id, {}).get("msg_id")])
            update_keyword_fields(ADMIN_DATA[user_id]["edit_kw_id"], content=text); USER_STATES[user_id] = None
            await message.answer(f"Текст обновлен!", keyboard=get_back_kb("kw_list"))
            return

        if text.lower() == "/админ":
            await clear_chat(user_id, cmid) # Чистим команду вызова
            await message.answer("Панель администратора", keyboard=get_admin_main_kb())
            return

    # --- ЛОГИКА ОБЫЧНОГО ЮЗЕРА ---

    # 1. Проверка на вложение товара (Market) — когда жмут "Написать продавцу"
    market_attachments = [a for a in message.attachments if a.market]
    if market_attachments:
        market_product = market_attachments[0].market
        product_title = market_product.title
        greeting = await get_user_greeting(user_id)
        amount = extract_market_amount(market_product)

        if MODERATION_MODE and not has_moderation_access(user_id):
            await message.answer(
                f"{greeting}\n\n"
                f"Вижу, вас заинтересовал товар: «{product_title}».\n\n"
                "Оплата сейчас подключается через Robokassa. Если хотите купить программу до запуска автоматической оплаты, напишите вопрос в поддержку.",
                keyboard=get_payment_unavailable_kb()
            )
            return

        if not robokassa_is_configured():
            await message.answer(
                f"{greeting}\n\n"
                f"Вижу, вас заинтересовал товар: «{product_title}».\n\n"
                "Оплата через Robokassa сейчас настраивается. Напишите в поддержку, и мы поможем с покупкой вручную.",
                keyboard=get_user_main_kb()
            )
            return

        if amount is None:
            await message.answer(
                f"{greeting}\n\n"
                f"Вижу, вас заинтересовал товар: «{product_title}».\n\n"
                "Не получилось определить цену товара для оплаты. Напишите в поддержку, и мы проверим карточку товара.",
                keyboard=get_user_main_kb()
            )
            return

        order_id = create_order(user_id, product_title, amount)
        payment_url = build_payment_url(order_id, product_title, amount)
        
        # Генерируем кнопку для оплаты (пока просто кнопка-заглушка или информация)
        pay_kb = (
            Keyboard(inline=True)
            .add(Callback(f"💳 Оплатить {product_title}", {"cmd": "pay_product", "title": product_title}), color=KeyboardButtonColor.POSITIVE)
            .get_json()
        )
        pay_kb = (
            Keyboard(inline=True)
            .add(OpenLink(link=payment_url, label=f"Оплатить {amount:.0f} ₽"))
            .get_json()
        )

        await message.answer(
            f"🛒 Вижу, вас заинтересовал товар: «{product_title}».\n\n"
            "Нажмите кнопку ниже для оплаты, и я мгновенно пришлю материалы!",
            keyboard=pay_kb
        )
        return

    # Если юзер пишет свой "Другой вопрос"
    if USER_STATES.get(user_id) == "waiting_for_custom_question":
        d_id = add_ticket(user_id, text)
        del USER_STATES[user_id]
        
        await message.answer("Ваш вопрос принят, администратор группы ответит вам в ближайшее время.", keyboard=get_user_main_kb())
        
        for a_id in ADMIN_IDS:
            admin_msg = f"❓ Новый вопрос (№{d_id})\nОт: [id{user_id}|@id{user_id}]\nВопрос: {text}"
            kb = (
                Keyboard(inline=True)
                .add(Callback("Ответить", {"admin": "reply_start", "tid": d_id, "uid": user_id}), color=KeyboardButtonColor.POSITIVE).row()
                .add(Callback("Проигнорировать", {"admin": "ticket_ignore", "tid": d_id}), color=KeyboardButtonColor.SECONDARY)
                .get_json()
            )
            await bot.api.messages.send(peer_id=a_id, message=admin_msg, keyboard=kb, random_id=get_rand())
        return

    # --- ТЕСТОВЫЕ КОМАНДЫ ДЛЯ АДМИНА ---
    if user_id in ADMIN_IDS and text.upper() in ["ТЕСТ ПОДПИСКИ", "ТЕСТ УВЕДОМЛЕНИЙ"]:
        is_member, is_allowed = await check_user_subscriptions(user_id)
        # Для теста "обманываем", если человек реально подписан
        if text.upper() == "ТЕСТ ПОДПИСКИ": is_member = False
        if text.upper() == "ТЕСТ УВЕДОМЛЕНИЙ": is_allowed = False
        
        greeting = await get_user_greeting(user_id)
        await send_sub_request(user_id, is_member, is_allowed, greeting)
        return

    # Проверка кодовых слов
    kw_data = get_material(text)
    if kw_data:
        # Получаем данные
        is_member, is_allowed = await check_user_subscriptions(user_id)
        print(f"DEBUG: Check sub for {user_id}: {is_member}, {is_allowed}")
        greeting = await get_user_greeting(user_id)

        # --- КУСОК ДЛЯ ПРОВЕРКИ КОММЕНТАРИЯ (30 мин) ---
        commented = has_commented_recently(user_id, hours=0.5)
        if not commented:
            await message.answer(
                f"{greeting}\n\n"
                "Чтобы получить этот бесплатный материал, пожалуйста, оставьте любой осознанный комментарий "
                "под любым видео или клипом в нашей группе прямо сейчас! 🎥\n\n"
                "Это помогает нам развиваться. Как только оставите — напишите слово еще раз!"
            ); return
        # --------------------------------------------

        # Если что-то не так — просим подписаться
        if not is_member or not is_allowed:
            # Запоминаем, какое слово человек хотел получить
            ADMIN_DATA[user_id] = ADMIN_DATA.get(user_id, {})
            ADMIN_DATA[user_id]["pending_kw"] = text
            await send_sub_request(user_id, is_member, is_allowed, greeting)
            return

        t, a = kw_data
        # Собираем сообщение БЕЗ благодарности за подписку (уже подписан)
        full_msg = f"{greeting}\n{t}"
        
        # Добавляем пожелание, если его еще нет в тексте из базы
        if "занимайтесь с удовольствием" not in t.lower() and "занимайся с удовольствием" not in t.lower():
            full_msg += "\nЗанимайтесь с удовольствием:"
        
        if "http" not in t.lower():
            full_msg += "\n\n🔗 Материалы: http://материалы-будут-добавлены-позже"
        
        # Добавляем призыв к каталогу в самый конец
        full_msg += "\n\nЕсли хотите больше полезных программ — загляните в наш каталог! 🧘‍♀️"
            
        await bot.api.messages.send(peer_id=user_id, message=full_msg, attachment=a, random_id=get_rand(), keyboard=get_user_main_kb())
        return

    if text.lower() in ["привет", "старт"]:
        greeting = await get_user_greeting(user_id)
        await message.answer(f"{greeting}! Я помощник Марианны. Выберите нужный раздел ниже:", keyboard=get_user_main_kb())
async def deliver_product(user_id, product_name):
    """Выдает контент товара пользователю"""
    try:
        content = get_product_at(product_name)
        if not content: return
        text, attachment = content
        
        # Компактный формат
        msg = f"🎉 Спасибо! Ваши материалы по программе «{product_name}»:\n{text}"
        
        # Добавляем пожелание, если его нет
        if "занимайтесь с удовольствием" not in text.lower() and "занимайся с удовольствием" not in text.lower():
            msg += "\nЗанимайтесь с удовольствием:"
            
        params = {"peer_id": user_id, "message": msg, "keyboard": keyboard_main, "random_id": get_rand()}
        if attachment: params["attachment"] = attachment
        await bot.api.messages.send(**params)
        log_purchase(user_id, product_name)
    except: pass

@bot.on.raw_event("message_event", dict)
async def handle_callback(event: dict):
    # Сразу отвечаем ВК, чтобы убрать загрузку на кнопке
    try:
        await bot.api.messages.send_message_event_answer(
            event_id=event["object"]["event_id"],
            user_id=event["object"]["user_id"],
            peer_id=event["object"]["peer_id"]
        )
    except: pass

    payload = event["object"].get("payload")
    if not payload: return
    user_id = event["object"]["user_id"]
    peer_id = event["object"]["peer_id"]
    cmid = event["object"]["conversation_message_id"]

    if payload.get("cmd") == "check_sub_again":
        is_member, is_allowed = await check_user_subscriptions(user_id)
        if is_member and is_allowed:
            # Ищем, какое слово человек запрашивал
            kw_name = ADMIN_DATA.get(user_id, {}).get("pending_kw")
            if kw_name:
                kw_data = get_material(kw_name)
                if kw_data:
                    t, a = kw_data
                    # Убираем дублирование названия урока, оставляем только Спасибо и текст из БД
                    full_msg = f"Спасибо за подписку! ❤️\n{t}"
                    
                    if "занимайтесь с удовольствием" not in t.lower() and "занимайся с удовольствием" not in t.lower():
                        full_msg += "\nЗанимайтесь с удовольствием:"
                    
                    if "http" not in t.lower():
                        full_msg += "\n\n🔗 Материалы: http://материалы-будут-добавлены-позже"

                    full_msg += "\n\nЕсли хотите больше полезных программ — загляните в наш каталог! 🧘‍♀️"

                    await bot.api.messages.send(
                        peer_id=user_id, 
                        message=full_msg, 
                        attachment=a,
                        random_id=get_rand(),
                        keyboard=get_user_main_kb()
                    )
                    return

            await bot.api.messages.send(
                peer_id=peer_id, 
                message="Отлично! Теперь все готово. Напишите кодовое слово еще раз, чтобы получить материалы. ✨", 
                random_id=get_rand(),
                keyboard=get_user_main_kb()
            )
        else:
            greeting = await get_user_greeting(user_id)
            await send_sub_request(user_id, is_member, is_allowed, greeting, edit_cmid=cmid)
        return

    if payload.get("cmd") == "pay_product":
        title = payload.get("title")
        try: await bot.api.messages.send_message_event_answer(event_id=event["object"]["event_id"], user_id=user_id, peer_id=peer_id, event_data='{"type": "show_snackbar", "text": "Оплата скоро будет доступна"}')
        except: pass
        await bot.api.messages.send(
            peer_id=peer_id,
            message=f"Оплата товара «{title}» сейчас подключается через Robokassa. Материалы будут выдаваться автоматически только после подтверждения оплаты.",
            random_id=get_rand(),
            keyboard=get_user_main_kb()
        )
        return

    # ЮЗЕР-МЕНЮ
    user_cmd = payload.get("cmd")
    if user_cmd == "support_menu":
        # ПЕРВЫЙ вход в меню - всегда НОВОЕ сообщение, чтобы материалы НЕ пропадали
        await bot.api.messages.send(
            peer_id=peer_id, 
            message="Какой у вас вопрос? Выберите из частых или напишите свой:", 
            keyboard=get_support_kb(),
            random_id=get_rand()
        )
        return

    if user_cmd == "support_menu_edit":
        # Навигация ВНУТРИ меню (назад от вопроса) - ЗАМЕНА содержимого
        new_text = "Какой у вас вопрос? Выберите из частых или напишите свой:"
        try:
            await bot.api.messages.edit(
                peer_id=peer_id, 
                message=new_text, 
                conversation_message_id=cmid, 
                keyboard=get_support_kb()
            )
        except:
            await bot.api.messages.send(peer_id=peer_id, message=new_text, keyboard=get_support_kb(), random_id=get_rand())
        return

    if user_cmd == "ask_custom":
        await bot.api.messages.edit(
            peer_id=peer_id, 
            message="Пожалуйста, напишите свой вопрос одним сообщением:", 
            conversation_message_id=cmid, 
            keyboard=Keyboard(inline=True).add(Callback("🔙 Назад", {"cmd": "support_menu_edit"}), color=KeyboardButtonColor.SECONDARY).get_json()
        )
        USER_STATES[user_id] = "waiting_for_custom_question"
        ADMIN_DATA[user_id] = {"msg_id": cmid}
        return

    if user_cmd == "thanks":
        await bot.api.messages.send(
            peer_id=peer_id, 
            message="Всегда рада помочь! Если появятся еще вопросы — я на связи. ✨", 
            random_id=get_rand(),
            keyboard=get_user_main_kb()
        )
        return

    faq_id = payload.get("faq")
    if faq_id:
        faqs = {
            1: "📖 Как получить доступ?\n\nВсе просто: после оплаты бот моментально пришлет ссылку на урок прямо в этот чат. Также вы можете найти материалы в разделе «Весь каталог» в любое время.",
            2: "📖 Где мои курсы?\n\nНажмите кнопку «Весь каталог» под любым сообщением. Там отображаются все ваши приобретенные программы и бесплатные уроки.",
            3: "📖 Можно ли беременным?\n\nДа, у Марианны есть специальные мягкие практики. Однако мы всегда рекомендуем проконсультироваться с вашим врачом перед началом занятий.",
            4: "📖 Личный кабинет\n\nЭто мини-приложение внутри ВК, где ваши уроки структурированы. Ссылка на него всегда есть в главном меню бота."
        }
        await bot.api.messages.edit(
            peer_id=peer_id, 
            message=faqs.get(faq_id, "Информация скоро появится."), 
            conversation_message_id=cmid, 
            keyboard=get_faq_back_kb()
        )
        return

    # АДМИН-МЕНЮ
    admin_cmd = payload.get("admin")
    if admin_cmd and user_id in ADMIN_IDS:
        # Новые команды обработки тикетов
        if admin_cmd == "reply_start":
            t_id = payload.get("tid")
            u_id = payload.get("uid")
            USER_STATES[user_id] = f"admin_wait_reply_{t_id}_{u_id}"
            await bot.api.messages.send(
                peer_id=user_id,
                message=f"Напишите ответ на вопрос №{t_id} (или нажмите кнопку Назад)",
                keyboard=Keyboard(inline=True).add(Callback("🔙 Назад", {"admin": "main"}), color=KeyboardButtonColor.SECONDARY).get_json(),
                random_id=get_rand()
            )
            return

        if admin_cmd == "ticket_ignore":
            t_id = payload.get("tid")
            close_ticket(t_id)
            await bot.api.messages.edit(
                peer_id=peer_id,
                message=f"Вопрос №{t_id} проигнорирован и закрыт.",
                conversation_message_id=cmid,
                keyboard=get_admin_main_kb()
            )
            return
        new_text = ""
        new_kb = get_back_kb()

        if admin_cmd == "main":
            new_text = "Панель администратора\nВыберите интересующий раздел:"
            new_kb = get_admin_main_kb()
        
        elif admin_cmd == "stats":
            u, p = get_stats()
            new_text = f"Статистика системы\n\nВсего пользователей: {u}\nЗафиксировано покупок: {p}"
        
        elif admin_cmd == "tickets":
            tickets = get_open_tickets()
            if not tickets: new_text = "Активные обращения отсутствуют."
            else:
                new_text = "Список активных обращений:\n\n"
                for tid, did, uid, q in tickets: new_text += f"- [{did}] ID{uid}: {q}\n"
                new_text += "\nДля закрытия: /ок НОМЕР"
        
        elif admin_cmd == "keywords":
            new_text = "Управление кодовыми словами"
            new_kb = get_kw_menu_kb()

        elif admin_cmd == "kw_add_start":
            new_text = "Напишите новое кодовое слово"
            USER_STATES[user_id] = "kw_wait_word"
            ADMIN_DATA[user_id] = {"msg_id": cmid}
            new_kb = get_back_kb("keywords")

        elif admin_cmd == "kw_list":
            USER_STATES[user_id] = None # Сброс состояний ввода
            kws = get_all_keywords()
            if not kws: new_text = "Кодовых слов пока нет."
            else:
                new_text = "Список кодовых слов:\n\n"
                for i, kw in enumerate(kws): new_text += f"{i+1}) {kw[1].upper()} — {kw[2][:40]}...\n"
                new_kb = Keyboard(inline=True).add(Callback("Редактировать кодовое слово", {"admin": "kw_edit_pick"}), color=KeyboardButtonColor.PRIMARY).row()
                new_kb.add(Callback("Вернуться назад", {"admin": "keywords"}), color=KeyboardButtonColor.SECONDARY).get_json()

        elif admin_cmd == "kw_edit_pick":
            new_text = "Отправьте номер кодового слова, за которым требуется редактирование"
            USER_STATES[user_id] = "kw_wait_index"
            ADMIN_DATA[user_id] = {"msg_id": cmid}
            new_kb = get_back_kb("kw_list")

        elif admin_cmd == "kw_edit_word":
            new_text = "Напишите Новое слово"
            USER_STATES[user_id] = "kw_wait_edit_word"
            ADMIN_DATA[user_id]["msg_id"] = cmid
            new_kb = get_back_kb("kw_list")

        elif admin_cmd == "kw_edit_text":
            new_text = "Напишите Новый текст"
            USER_STATES[user_id] = "kw_wait_edit_text"
            ADMIN_DATA[user_id]["msg_id"] = cmid
            new_kb = get_back_kb("kw_list")

        elif admin_cmd == "kw_delete_confirm":
            new_text = "Вы уверены? Удалить эту команду?"
            new_kb = Keyboard(inline=True).add(Callback("Да", {"admin": "kw_delete_yes"}), color=KeyboardButtonColor.NEGATIVE).row()
            new_kb.add(Callback("Нет", {"admin": "kw_list"}), color=KeyboardButtonColor.SECONDARY).get_json()

        elif admin_cmd == "kw_delete_yes":
            kw_id = ADMIN_DATA.get(user_id, {}).get("edit_kw_id")
            if kw_id:
                delete_keyword_by_id(kw_id)
                try:
                    await bot.api.messages.send_message_event_answer(
                        event_id=event["object"]["event_id"], 
                        user_id=user_id, 
                        peer_id=peer_id, 
                        event_data='{"type": "show_snackbar", "text": "✅ Команда удалена"}'
                    )
                except: pass
            
            # В любом случае возвращаем к списку, чтобы не "висеть"
            kws = get_all_keywords()
            new_text = "Список кодовых слов:\n\n"
            if not kws:
                new_text += "Список пока пуст."
            for i, kw in enumerate(kws):
                new_text += f"{i+1}) {kw[1].upper()} — {kw[2][:40]}...\n"
            
            new_kb = (
                Keyboard(inline=True)
                .add(Callback("Редактировать другое слово", {"admin": "kw_edit_pick"}), color=KeyboardButtonColor.PRIMARY).row()
                .add(Callback("Вернуться в меню", {"admin": "keywords"}), color=KeyboardButtonColor.SECONDARY)
                .get_json()
            )

        try:
            await bot.api.messages.edit(peer_id=peer_id, message=new_text, conversation_message_id=cmid, keyboard=new_kb)
        except: pass

# --- ОБРАБОТКА КОММЕНТАРИЕВ ДЛЯ ПРОВЕРКИ УСЛОВИЯ ---

# Слушаем новые комментарии на стене
@bot.on.raw_event("wall_reply_new", dict)
async def handle_wall_comment(event: dict):
    # В vkbottle это объект dict, достаем через ["object"]
    user_id = event["object"].get("from_id") or event["object"].get("user_id")
    if user_id > 0:
        add_comment_log(user_id)
        print(f"DEBUG: Лог комментария на стене от ID{user_id}")

# Слушаем новые комментарии к видео/клипам
@bot.on.raw_event("video_comment_new", dict)
async def handle_video_comment(event: dict):
    user_id = event["object"].get("from_id") or event["object"].get("user_id")
    if user_id > 0:
        add_comment_log(user_id)
        print(f"DEBUG: Лог комментария к видео от ID{user_id}")

if __name__ == "__main__":
    db_init()
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    print("\n✅ ИНТЕРАКТИВНАЯ ПАНЕЛЬ АДМИНА ГОТОВА!\n")
    bot.loop_wrapper.add_task(process_paid_orders())
    start_payment_server()
    bot.run_forever()
