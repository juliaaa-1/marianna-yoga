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

# Р“РµРЅРµСЂРёСЂСѓРµРј СѓРЅРёРєР°Р»СЊРЅС‹Р№ random_id РґР»СЏ РєР°Р¶РґРѕРіРѕ СЃРѕРѕР±С‰РµРЅРёСЏ
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

# РљРЅРѕРїРєР° РІРѕР·РІСЂР°С‚Р° РІ РєР°С‚Р°Р»РѕРі
keyboard_main = (
    Keyboard(inline=True)
    .add(OpenLink(link=APP_URL, label="рџ§вЂЌв™ЂпёЏ Р’РµСЃСЊ РєР°С‚Р°Р»РѕРі"))
    .get_json()
)


async def deliver_product(user_id, product_name):
    """Р’С‹РґР°РµС‚ РєРѕРЅС‚РµРЅС‚ С‚РѕРІР°СЂР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ"""
    print(f"DEBUG: РџС‹С‚Р°РµРјСЃСЏ РІС‹РґР°С‚СЊ С‚РѕРІР°СЂ '{product_name}' РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ {user_id}")
    try:
        content = get_product_at(product_name)
        if not content:
            msg = f"рџљЁ РўРћР’РђР  РќР• РќРђР™Р”Р•Рќ!\nID{user_id} РєСѓРїРёР» '{product_name}', РЅРѕ РІ Р±Р°Р·Рµ РїСѓСЃС‚Рѕ. РњР°СЂРёР°РЅРЅР°, РїРѕСЃРјРѕС‚СЂРё!"
            for admin_id in ADMIN_IDS:
                await bot.api.messages.send(peer_id=admin_id, message=msg, random_id=get_rand())
            
            user_msg = f"вњ… РћРїР»Р°С‚Р° В«{product_name}В» РїСЂРѕС€Р»Р°! РњР°СЂРёР°РЅРЅР° РїСЂРёС€Р»РµС‚ СЃСЃС‹Р»РєСѓ РІСЂСѓС‡РЅСѓСЋ РІ Р±Р»РёР¶Р°Р№С€РµРµ РІСЂРµРјСЏ. рџЉ"
            await bot.api.messages.send(peer_id=user_id, message=user_msg, keyboard=keyboard_main, random_id=get_rand())
            return

        text, attachment = content
        msg = f"рџЋ‰ РЎРїР°СЃРёР±Рѕ! Р’Р°С€Рё РјР°С‚РµСЂРёР°Р»С‹ РїРѕ РїСЂРѕРіСЂР°РјРјРµ В«{product_name}В»:\n{text}\nР—Р°РЅРёРјР°Р№С‚РµСЃСЊ СЃ СѓРґРѕРІРѕР»СЊСЃС‚РІРёРµРј! вњЁ"
        
        # РћС‚РїСЂР°РІР»СЏРµРј С‚РѕР»СЊРєРѕ РµСЃР»Рё РІР»РѕР¶РµРЅРёРµ СЂРµР°Р»СЊРЅРѕ РµСЃС‚СЊ
        params = {"peer_id": user_id, "message": msg, "keyboard": keyboard_main, "random_id": get_rand()}
        if attachment:
            params["attachment"] = attachment
            
        await bot.api.messages.send(**params)
        log_purchase(user_id, product_name)
        print(f"SUCCESS: РўРѕРІР°СЂ '{product_name}' РІС‹РґР°РЅ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ {user_id}")
    except Exception as e:
        print(f"CRITICAL ERROR in deliver_product: {e}")
        traceback.print_exc()

async def get_user_greeting(user_id):
    """РћРїСЂРµРґРµР»СЏРµС‚ РїСЂРёРІРµС‚СЃС‚РІРёРµ РїРѕ РІСЂРµРјРµРЅРё СЃСѓС‚РѕРє Рё РїРѕР»СѓС‡Р°РµС‚ РёРјСЏ"""
    try:
        users = await bot.api.users.get(user_ids=[user_id])
        name = users[0].first_name
    except:
        name = "Р”СЂСѓРі"
    
    hour = datetime.now().hour
    if 4 <= hour < 11:
        greet = "РґРѕР±СЂРѕРµ СѓС‚СЂРѕ! вЂпёЏ"
    elif 11 <= hour < 16:
        greet = "РґРѕР±СЂС‹Р№ РґРµРЅСЊ! рџЊ¤пёЏ"
    elif 16 <= hour < 23:
        greet = "РґРѕР±СЂС‹Р№ РІРµС‡РµСЂ! рџЊ†"
    else:
        greet = "РґРѕР±СЂРѕРіРѕ РІСЂРµРјРµРЅРё РЅРѕС‡Рё! рџЊ™"
    
    return f"{name}, {greet}"

async def check_user_subscriptions(user_id):
    """РџСЂРѕРІРµСЂСЏРµС‚ РїРѕРґРїРёСЃРєСѓ РЅР° РіСЂСѓРїРїСѓ Рё РІРєР»СЋС‡РµРЅРЅС‹Рµ СѓРІРµРґРѕРјР»РµРЅРёСЏ. Р’РѕР·РІСЂР°С‰Р°РµС‚ (is_member, is_allowed)"""
    try:
        # РџСЂРѕРІРµСЂРєР° РїРѕРґРїРёСЃРєРё РЅР° РіСЂСѓРїРїСѓ
        # groups.is_member РґР»СЏ РѕРґРЅРѕРіРѕ СЋР·РµСЂР° РІРѕР·РІСЂР°С‰Р°РµС‚ 1 РёР»Рё 0
        resp_member = await bot.api.groups.is_member(group_id=GROUP_ID, user_id=user_id)
        is_member = bool(resp_member)
        
        # РџСЂРѕРІРµСЂРєР° СЂР°Р·СЂРµС€РµРЅРЅС‹С… СЃРѕРѕР±С‰РµРЅРёР№
        resp_allowed = await bot.api.messages.is_messages_from_group_allowed(group_id=GROUP_ID, user_id=user_id)
        # РЈ vkbottle СЌС‚Рѕ РѕР±С‹С‡РЅРѕ РѕР±СЉРµРєС‚ СЃ РїРѕР»РµРј is_allowed
        is_allowed = bool(getattr(resp_allowed, "is_allowed", resp_allowed))
        
        return is_member, is_allowed
    except Exception as e:
        print(f"Sub check error for {user_id}: {e}")
        return True, True # Р•СЃР»Рё РѕС€РёР±РєР° API, РЅРµ Р±Р»РѕРєРёСЂСѓРµРј

async def send_sub_request(user_id, is_member, is_allowed, greeting="", edit_cmid=None):
    """РћС‚РїСЂР°РІР»СЏРµС‚ РёР»Рё СЂРµРґР°РєС‚РёСЂСѓРµС‚ РїСЂРѕСЃСЊР±Сѓ РїРѕРґРїРёСЃР°С‚СЊСЃСЏ Рё/РёР»Рё РІРєР»СЋС‡РёС‚СЊ СѓРІРµРґРѕРјР»РµРЅРёСЏ"""
    base_msg = "РћСЃС‚Р°Р»СЃСЏ РїРѕСЃР»РµРґРЅРёР№ РјР°Р»РµРЅСЊРєРёР№ С€Р°Рі Рє РїРѕР»СѓС‡РµРЅРёСЋ РјР°С‚РµСЂРёР°Р»РѕРІ! вњЁ\n\n"
    
    if not is_member:
        req_msg = (
            "Р§С‚РѕР±С‹ Р±РѕС‚ РјРѕРі РѕС‚РїСЂР°РІРёС‚СЊ Р’Р°Рј РјР°С‚РµСЂРёР°Р»С‹, РїРѕР¶Р°Р»СѓР№СЃС‚Р°:\n"
            "1. РџРѕРґРїРёС€РёС‚РµСЃСЊ РЅР° РЅР°С€Рµ СЃРѕРѕР±С‰РµСЃС‚РІРѕ\n"
            "2. Р’РєР»СЋС‡РёС‚Рµ СѓРІРµРґРѕРјР»РµРЅРёСЏ (РєРѕР»РѕРєРѕР»СЊС‡РёРє рџ””)\n\n"
        )
    else:
        req_msg = "Р§С‚РѕР±С‹ Р±РѕС‚ РјРѕРі РѕС‚РїСЂР°РІРёС‚СЊ Р’Р°Рј РјР°С‚РµСЂРёР°Р»С‹, РїРѕР¶Р°Р»СѓР№СЃС‚Р° РІРєР»СЋС‡РёС‚Рµ СѓРІРµРґРѕРјР»РµРЅРёСЏ (РєРѕР»РѕРєРѕР»СЊС‡РёРє рџ””)\n\n"

    # Р•СЃР»Рё СѓР¶Рµ РЅР°Р¶РёРјР°Р»Рё, РЅРѕ РЅРµ РїРѕРґРїРёСЃР°Р»РёСЃСЊ вЂ” С‡СѓС‚СЊ РјРµРЅСЏРµРј С‚РµРєСЃС‚
    if edit_cmid:
        fail_prefix = "РљР°Р¶РµС‚СЃСЏ, РєР°РєРѕР№-С‚Рѕ РёР· С€Р°РіРѕРІ РїСЂРѕРїСѓС‰РµРЅ... рџ¤«\n\n"
        full_msg = f"{greeting}\n\n{fail_prefix}{req_msg}РљР°Рє С‚РѕР»СЊРєРѕ РІСЃС‘ СЃРґРµР»Р°РµС‚Рµ вЂ” Р¶РјРёС‚Рµ РєРЅРѕРїРєСѓ РµС‰Рµ СЂР°Р·!"
    else:
        full_msg = f"{greeting}\n\n{base_msg}{req_msg}РџРѕСЃР»Рµ СЌС‚РѕРіРѕ РЅР°Р¶РјРёС‚Рµ РєРЅРѕРїРєСѓ РЅРёР¶Рµ:"
    
    # Р—Р°РіСЂСѓР¶Р°РµРј РєР°СЂС‚РёРЅРєСѓ С‚РѕР»СЊРєРѕ РґР»СЏ РЅРѕРІРѕРіРѕ СЃРѕРѕР±С‰РµРЅРёСЏ (РІ СЂРµРґР°РєС‚РёСЂРѕРІР°РЅРёРё РѕРЅР° РѕСЃС‚Р°РЅРµС‚СЃСЏ)
    photo = None
    if not edit_cmid:
        try:
            photo = await photo_uploader.upload("РєР°СЂС‚РёРЅРєРё/СѓРІРµРґРѕРјР»РµРЅРёСЏ.jpg")
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
            # Р•СЃР»Рё РЅРµ РІС‹С€Р»Рѕ РѕС‚СЂРµРґР°РєС‚РёСЂРѕРІР°С‚СЊ (РЅР°РїСЂРёРјРµСЂ СЃРѕРѕР±С‰РµРЅРёРµ СЃС‚Р°СЂРѕРµ), РїСЂРѕСЃС‚Рѕ С€Р»РµРј РЅРѕРІРѕРµ
            await bot.api.messages.send(peer_id=user_id, message=full_msg, keyboard=get_sub_kb(), random_id=get_rand())
    else:
        await bot.api.messages.send(
            peer_id=user_id, 
            message=full_msg, 
            attachment=photo, 
            keyboard=get_sub_kb(), 
            random_id=get_rand()
        )

# --- Р’РЎРџРћРњРћР“РђРўР•Р›Р¬РќР«Р• Р¤РЈРќРљР¦РР ---

async def clear_chat(peer_id, cmids):
    """РћС‚РєР»СЋС‡Р°РµРј СѓРґР°Р»РµРЅРёРµ СЃРѕРѕР±С‰РµРЅРёР№, С‡С‚РѕР±С‹ СЃРѕС…СЂР°РЅРёС‚СЊ РєРѕРЅС‚РµРєСЃС‚ Р±РµСЃРµРґС‹"""
    return

# РҐСЂР°РЅРёРј СЃРѕСЃС‚РѕСЏРЅРёСЏ Рё РІСЂРµРјРµРЅРЅС‹Рµ РґР°РЅРЅС‹Рµ
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
        .add(OpenLink(link=APP_URL, label="РћС‚РєСЂС‹С‚СЊ РєР°С‚Р°Р»РѕРі"))
        .row()
        .add(Callback("Р—Р°РґР°С‚СЊ РІРѕРїСЂРѕСЃ", {"cmd": "support_menu"}), color=KeyboardButtonColor.PRIMARY)
        .get_json()
    )

# --- РљР›РђР’РРђРўРЈР Р« РђР”РњРРќРђ ---

def get_admin_main_kb():
    return (
        Keyboard(inline=True)
        .add(Callback("РЎС‚Р°С‚РёСЃС‚РёРєР°", {"admin": "stats"}), color=KeyboardButtonColor.PRIMARY).row()
        .add(Callback("РћР±СЂР°С‰РµРЅРёСЏ", {"admin": "tickets"}), color=KeyboardButtonColor.PRIMARY).row()
        .add(Callback("РЈРїСЂР°РІР»РµРЅРёРµ РєРѕРґРѕРІС‹РјРё СЃР»РѕРІР°РјРё", {"admin": "keywords"}), color=KeyboardButtonColor.SECONDARY)
        .get_json()
    )

def get_sub_kb():
    return (
        Keyboard(inline=True)
        .add(Callback("вњ… Р’СЃС‘ РіРѕС‚РѕРІРѕ!", {"cmd": "check_sub_again"}), color=KeyboardButtonColor.POSITIVE)
        .get_json()
    )

def get_kw_menu_kb():
    return (
        Keyboard(inline=True)
        .add(Callback("Р”РѕР±Р°РІРёС‚СЊ СЃР»РѕРІРѕ", {"admin": "kw_add_start"}), color=KeyboardButtonColor.POSITIVE).row()
        .add(Callback("Р’РµСЃСЊ СЃРїРёСЃРѕРє", {"admin": "kw_list"}), color=KeyboardButtonColor.PRIMARY).row()
        .add(Callback("Р’РµСЂРЅСѓС‚СЊСЃСЏ РЅР°Р·Р°Рґ", {"admin": "main"}), color=KeyboardButtonColor.SECONDARY)
        .get_json()
    )

def get_back_kb(target="main"):
    return (
        Keyboard(inline=True)
        .add(Callback("Р’РµСЂРЅСѓС‚СЊСЃСЏ РЅР°Р·Р°Рґ", {"admin": target}), color=KeyboardButtonColor.SECONDARY)
        .get_json()
    )

def get_user_main_kb():
    return (
        Keyboard(inline=True)
        .add(OpenLink(link=APP_URL, label="рџ§вЂЌв™ЂпёЏ Р’РµСЃСЊ РєР°С‚Р°Р»РѕРі")).row()
        .add(Callback("вќ“ Р•СЃС‚СЊ РІРѕРїСЂРѕСЃ", {"cmd": "support_menu"}), color=KeyboardButtonColor.PRIMARY)
        .get_json()
    )

def get_support_kb():
    return (
        Keyboard(inline=True)
        .add(Callback("Как получить доступ к урокам?", {"faq": 1}), color=KeyboardButtonColor.SECONDARY).row()
        .add(Callback("Где найти оплаченные курсы?", {"faq": 2}), color=KeyboardButtonColor.SECONDARY).row()
        .add(Callback("Можно ли заниматься беременным?", {"faq": 3}), color=KeyboardButtonColor.SECONDARY).row()
        .add(Callback("Как работает личный кабинет?", {"faq": 4}), color=KeyboardButtonColor.SECONDARY).row()
        .add(Callback("Другой вопрос", {"cmd": "ask_custom"}), color=KeyboardButtonColor.PRIMARY).row()
        .add(OpenLink(link=APP_URL, label="Весь каталог"))
        .get_json()
    )

def get_faq_back_kb():
    return (
        Keyboard(inline=True)
        .add(Callback("Назад к вопросам", {"cmd": "support_menu_edit"}), color=KeyboardButtonColor.SECONDARY).row()
        .add(Callback("Другой вопрос", {"cmd": "ask_custom"}), color=KeyboardButtonColor.PRIMARY).row()
        .add(OpenLink(link=APP_URL, label="Весь каталог"))
        .get_json()
    )

def get_post_reply_kb():
    return (
        Keyboard(inline=True)
        .add(Callback("Задать вопрос еще", {"cmd": "ask_custom"}), color=KeyboardButtonColor.PRIMARY).row()
        .add(Callback("Спасибо за ответ", {"cmd": "thanks"}), color=KeyboardButtonColor.POSITIVE).row()
        .add(OpenLink(link=APP_URL, label="Весь каталог"))
        .get_json()
    )

def get_edit_kw_kb():
    return (
        Keyboard(inline=True)
        .add(Callback("РР·РјРµРЅРёС‚СЊ СЃР»РѕРІРѕ", {"admin": "kw_edit_word"}), color=KeyboardButtonColor.PRIMARY).row()
        .add(Callback("РР·РјРµРЅРёС‚СЊ С‚РµРєСЃС‚", {"admin": "kw_edit_text"}), color=KeyboardButtonColor.PRIMARY).row()
        .add(Callback("РЈРґР°Р»РёС‚СЊ РєРѕРјР°РЅРґСѓ", {"admin": "kw_delete_confirm"}), color=KeyboardButtonColor.NEGATIVE).row()
        .add(Callback("Р’РµСЂРЅСѓС‚СЊСЃСЏ РЅР°Р·Р°Рґ", {"admin": "kw_list"}), color=KeyboardButtonColor.SECONDARY)
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
            f"РўРµСЃС‚РѕРІС‹Р№ РґРѕСЃС‚СѓРї Рє СЃС†РµРЅР°СЂРёСЋ РѕРїР»Р°С‚С‹ РІРєР»СЋС‡РµРЅ РґРѕ {expires_at}.\n\n"
            "РўРµРїРµСЂСЊ РѕС‚РєСЂРѕР№С‚Рµ С‚РѕРІР°СЂ РІ СЃРѕРѕР±С‰РµСЃС‚РІРµ Рё РЅР°Р¶РјРёС‚Рµ В«РќР°РїРёСЃР°С‚СЊ РїСЂРѕРґР°РІС†СѓВ» вЂ” Р±РѕС‚ СЃРѕР·РґР°СЃС‚ С‚РµСЃС‚РѕРІСѓСЋ СЃСЃС‹Р»РєСѓ РѕРїР»Р°С‚С‹ Robokassa.",
            keyboard=get_user_main_kb()
        )
        return

    if text.lower() in ["РєР°С‚Р°Р»РѕРі", "С‚РѕРІР°СЂС‹", "РєСѓРїРёС‚СЊ", "РѕРїР»Р°С‚Р°"]:
        await message.answer(
            "РљР°С‚Р°Р»РѕРі РїСЂРѕРіСЂР°РјРј РґРѕСЃС‚СѓРїРµРЅ РІ РјРёРЅРё-РїСЂРёР»РѕР¶РµРЅРёРё. Р’С‹Р±РµСЂРёС‚Рµ С‚РѕРІР°СЂ Рё РЅР°Р¶РјРёС‚Рµ РєРЅРѕРїРєСѓ РїРѕРєСѓРїРєРё РІ РєР°СЂС‚РѕС‡РєРµ РёР»Рё РІ СЂР°Р·РґРµР»Рµ С‚РѕРІР°СЂРѕРІ СЃРѕРѕР±С‰РµСЃС‚РІР°.",
            keyboard=get_user_main_kb()
        )
        return
    
    # --- Р›РћР“РРљРђ РђР”РњРРќРђ ---
    if user_id in ADMIN_IDS:
        state = USER_STATES.get(user_id)
        
        # РћР±СЂР°Р±РѕС‚РєР° РІРІРѕРґР° РѕС‚РІРµС‚Р° РЅР° РІРѕРїСЂРѕСЃ
        if state and state.startswith("admin_wait_reply_"):
            parts = state.split("_")
            target_id = int(parts[4])
            t_id = int(parts[3])
            
            try:
                # РС‰РµРј С‚РµРєСЃС‚ РІРѕРїСЂРѕСЃР° РєР»РёРµРЅС‚Р°
                q_data = get_latest_user_question(target_id)
                q_text = q_data[0] if q_data else "РІР°С€ РІРѕРїСЂРѕСЃ"
                
                await bot.api.messages.send(
                    peer_id=target_id, 
                    message=f"вњ‰ РћС‚РІРµС‚ РЅР° РІРѕРїСЂРѕСЃ В«{q_text}В»:\n\n{text}", 
                    keyboard=get_post_reply_kb(),
                    random_id=get_rand()
                )
                close_ticket(t_id)
                USER_STATES[user_id] = None
                await message.answer(f"вњ… РћС‚РІРµС‚ РѕС‚РїСЂР°РІР»РµРЅ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ [id{target_id}|@id{target_id}]")
                return
            except Exception as e:
                print(f"Reply error: {e}")
                await message.answer("РћС€РёР±РєР° РїСЂРё РѕС‚РїСЂР°РІРєРµ РѕС‚РІРµС‚Р°.")
                return

        # РЎС‚Р°СЂР°СЏ РєРѕРјР°РЅРґР° (РѕСЃС‚Р°РІР»СЏРµРј РґР»СЏ СЃРѕРІРјРµСЃС‚РёРјРѕСЃС‚Рё, РЅРѕ С‡РёСЃС‚РёРј РѕС‚ Р·РІРµР·Рґ)
        if text.lower().startswith("/РѕС‚РІРµС‚ "):
            try:
                parts = text.split(" ", 2)
                target_id = int(parts[1])
                reply_text = parts[2]
                q_data = get_latest_user_question(target_id)
                q_text = q_data[0] if q_data else "РІР°С€ РІРѕРїСЂРѕСЃ"
                t_id = q_data[1] if q_data else None
                
                await bot.api.messages.send(
                    peer_id=target_id, 
                    message=f"вњ‰ РћС‚РІРµС‚ РЅР° РІРѕРїСЂРѕСЃ В«{q_text}В»:\n\n{reply_text}", 
                    keyboard=get_post_reply_kb(),
                    random_id=get_rand()
                )
                if t_id: close_ticket(t_id)
                await message.answer(f"вњ… РћС‚РІРµС‚ РѕС‚РїСЂР°РІР»РµРЅ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ [id{target_id}|@id{target_id}]")
                return
            except:
                await message.answer("РћС€РёР±РєР°! Р¤РѕСЂРјР°С‚: /РѕС‚РІРµС‚ [ID] [РўРµРєСЃС‚]")
                return

        state = USER_STATES.get(user_id)
        if state == "kw_wait_word":
            last_bot_msg = ADMIN_DATA.get(user_id, {}).get("msg_id")
            await clear_chat(user_id, [cmid, last_bot_msg])
            ADMIN_DATA[user_id] = ADMIN_DATA.get(user_id, {}); ADMIN_DATA[user_id]["word"] = text.upper()
            USER_STATES[user_id] = "kw_wait_text"
            sent = await message.answer("РќР°РїРёС€РёС‚Рµ С‚РµРєСЃС‚ СЃРѕРѕР±С‰РµРЅРёСЏ Рё СЃСЃС‹Р»РєСѓ РЅР° СѓСЂРѕРє", keyboard=Keyboard(inline=True).add(Callback("РћС‚РјРµРЅРёС‚СЊ", {"admin": "main"}), color=KeyboardButtonColor.NEGATIVE).get_json())
            ADMIN_DATA[user_id]["msg_id"] = sent.conversation_message_id
            return

        if state == "kw_wait_text":
            word = ADMIN_DATA[user_id]["word"]; await clear_chat(user_id, [cmid, ADMIN_DATA[user_id].get("msg_id")])
            add_keyword(word, text, ""); USER_STATES[user_id] = None
            await message.answer(f"РќРѕРІР°СЏ РєРѕРјР°РЅРґР° РґРѕР±Р°РІР»РµРЅР°:\nРЎР»РѕРІРѕ: {word}\nРўРµРєСЃС‚: {text}", keyboard=get_admin_main_kb())
            return

        if state == "kw_wait_index":
            await clear_chat(user_id, [cmid, ADMIN_DATA.get(user_id, {}).get("msg_id")])
            try:
                idx = int(text) - 1; kws = get_all_keywords()
                if 0 <= idx < len(kws):
                    kw = kws[idx]; ADMIN_DATA[user_id]["edit_kw_id"] = kw[0]; USER_STATES[user_id] = None
                    sent = await message.answer(f"Р РµРґР°РєС‚РёСЂРѕРІР°РЅРёРµ\nРЎР»РѕРІРѕ: {kw[1].upper()}\nРўРµРєСЃС‚: {kw[2]}", keyboard=get_edit_kw_kb())
                    ADMIN_DATA[user_id]["msg_id"] = sent.conversation_message_id
                else: await message.answer("РќРµРІРµСЂРЅС‹Р№ РЅРѕРјРµСЂ.", keyboard=get_back_kb("kw_list"))
            except: pass
            return

        if state == "kw_wait_edit_word":
            await clear_chat(user_id, [cmid, ADMIN_DATA.get(user_id, {}).get("msg_id")])
            update_keyword_fields(ADMIN_DATA[user_id]["edit_kw_id"], word=text); USER_STATES[user_id] = None
            await message.answer(f"РЎР»РѕРІРѕ РёР·РјРµРЅРµРЅРѕ: {text.upper()}", keyboard=get_back_kb("kw_list"))
            return

        if state == "kw_wait_edit_text":
            await clear_chat(user_id, [cmid, ADMIN_DATA.get(user_id, {}).get("msg_id")])
            update_keyword_fields(ADMIN_DATA[user_id]["edit_kw_id"], content=text); USER_STATES[user_id] = None
            await message.answer(f"РўРµРєСЃС‚ РѕР±РЅРѕРІР»РµРЅ!", keyboard=get_back_kb("kw_list"))
            return

        if text.lower() == "/Р°РґРјРёРЅ":
            await clear_chat(user_id, cmid) # Р§РёСЃС‚РёРј РєРѕРјР°РЅРґСѓ РІС‹Р·РѕРІР°
            await message.answer("РџР°РЅРµР»СЊ Р°РґРјРёРЅРёСЃС‚СЂР°С‚РѕСЂР°", keyboard=get_admin_main_kb())
            return

    # --- Р›РћР“РРљРђ РћР‘Р«Р§РќРћР“Рћ Р®Р—Р•Р Рђ ---

    # 1. РџСЂРѕРІРµСЂРєР° РЅР° РІР»РѕР¶РµРЅРёРµ С‚РѕРІР°СЂР° (Market) вЂ” РєРѕРіРґР° Р¶РјСѓС‚ "РќР°РїРёСЃР°С‚СЊ РїСЂРѕРґР°РІС†Сѓ"
    market_attachments = [a for a in message.attachments if a.market]
    if market_attachments:
        market_product = market_attachments[0].market
        product_title = market_product.title
        greeting = await get_user_greeting(user_id)
        amount = extract_market_amount(market_product)

        if MODERATION_MODE and not has_moderation_access(user_id):
            await message.answer(
                f"{greeting}\n\n"
                f"Р’РёР¶Сѓ, РІР°СЃ Р·Р°РёРЅС‚РµСЂРµСЃРѕРІР°Р» С‚РѕРІР°СЂ: В«{product_title}В».\n\n"
                "РћРїР»Р°С‚Р° СЃРµР№С‡Р°СЃ РїРѕРґРєР»СЋС‡Р°РµС‚СЃСЏ С‡РµСЂРµР· Robokassa. Р•СЃР»Рё С…РѕС‚РёС‚Рµ РєСѓРїРёС‚СЊ РїСЂРѕРіСЂР°РјРјСѓ РґРѕ Р·Р°РїСѓСЃРєР° Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРѕР№ РѕРїР»Р°С‚С‹, РЅР°РїРёС€РёС‚Рµ РІРѕРїСЂРѕСЃ РІ РїРѕРґРґРµСЂР¶РєСѓ.",
                keyboard=get_payment_unavailable_kb()
            )
            return

        if not robokassa_is_configured():
            await message.answer(
                f"{greeting}\n\n"
                f"Р’РёР¶Сѓ, РІР°СЃ Р·Р°РёРЅС‚РµСЂРµСЃРѕРІР°Р» С‚РѕРІР°СЂ: В«{product_title}В».\n\n"
                "РћРїР»Р°С‚Р° С‡РµСЂРµР· Robokassa СЃРµР№С‡Р°СЃ РЅР°СЃС‚СЂР°РёРІР°РµС‚СЃСЏ. РќР°РїРёС€РёС‚Рµ РІ РїРѕРґРґРµСЂР¶РєСѓ, Рё РјС‹ РїРѕРјРѕР¶РµРј СЃ РїРѕРєСѓРїРєРѕР№ РІСЂСѓС‡РЅСѓСЋ.",
                keyboard=get_user_main_kb()
            )
            return

        if amount is None:
            await message.answer(
                f"{greeting}\n\n"
                f"Р’РёР¶Сѓ, РІР°СЃ Р·Р°РёРЅС‚РµСЂРµСЃРѕРІР°Р» С‚РѕРІР°СЂ: В«{product_title}В».\n\n"
                "РќРµ РїРѕР»СѓС‡РёР»РѕСЃСЊ РѕРїСЂРµРґРµР»РёС‚СЊ С†РµРЅСѓ С‚РѕРІР°СЂР° РґР»СЏ РѕРїР»Р°С‚С‹. РќР°РїРёС€РёС‚Рµ РІ РїРѕРґРґРµСЂР¶РєСѓ, Рё РјС‹ РїСЂРѕРІРµСЂРёРј РєР°СЂС‚РѕС‡РєСѓ С‚РѕРІР°СЂР°.",
                keyboard=get_user_main_kb()
            )
            return

        order_id = create_order(user_id, product_title, amount)
        payment_url = build_payment_url(order_id, product_title, amount)
        
        # Р“РµРЅРµСЂРёСЂСѓРµРј РєРЅРѕРїРєСѓ РґР»СЏ РѕРїР»Р°С‚С‹ (РїРѕРєР° РїСЂРѕСЃС‚Рѕ РєРЅРѕРїРєР°-Р·Р°РіР»СѓС€РєР° РёР»Рё РёРЅС„РѕСЂРјР°С†РёСЏ)
        pay_kb = (
            Keyboard(inline=True)
            .add(Callback(f"рџ’і РћРїР»Р°С‚РёС‚СЊ {product_title}", {"cmd": "pay_product", "title": product_title}), color=KeyboardButtonColor.POSITIVE)
            .get_json()
        )
        pay_kb = (
            Keyboard(inline=True)
            .add(OpenLink(link=payment_url, label=f"РћРїР»Р°С‚РёС‚СЊ {amount:.0f} в‚Ѕ"))
            .get_json()
        )

        await message.answer(
            f"рџ›’ Р’РёР¶Сѓ, РІР°СЃ Р·Р°РёРЅС‚РµСЂРµСЃРѕРІР°Р» С‚РѕРІР°СЂ: В«{product_title}В».\n\n"
            "РќР°Р¶РјРёС‚Рµ РєРЅРѕРїРєСѓ РЅРёР¶Рµ РґР»СЏ РѕРїР»Р°С‚С‹, Рё СЏ РјРіРЅРѕРІРµРЅРЅРѕ РїСЂРёС€Р»СЋ РјР°С‚РµСЂРёР°Р»С‹!",
            keyboard=pay_kb
        )
        return

    # Р•СЃР»Рё СЋР·РµСЂ РїРёС€РµС‚ СЃРІРѕР№ "Р”СЂСѓРіРѕР№ РІРѕРїСЂРѕСЃ"
    if USER_STATES.get(user_id) == "waiting_for_custom_question":
        d_id = add_ticket(user_id, text)
        del USER_STATES[user_id]
        
        await message.answer("Р’Р°С€ РІРѕРїСЂРѕСЃ РїСЂРёРЅСЏС‚, Р°РґРјРёРЅРёСЃС‚СЂР°С‚РѕСЂ РіСЂСѓРїРїС‹ РѕС‚РІРµС‚РёС‚ РІР°Рј РІ Р±Р»РёР¶Р°Р№С€РµРµ РІСЂРµРјСЏ.", keyboard=get_user_main_kb())
        
        for a_id in ADMIN_IDS:
            admin_msg = f"вќ“ РќРѕРІС‹Р№ РІРѕРїСЂРѕСЃ (в„–{d_id})\nРћС‚: [id{user_id}|@id{user_id}]\nР’РѕРїСЂРѕСЃ: {text}"
            kb = (
                Keyboard(inline=True)
                .add(Callback("РћС‚РІРµС‚РёС‚СЊ", {"admin": "reply_start", "tid": d_id, "uid": user_id}), color=KeyboardButtonColor.POSITIVE).row()
                .add(Callback("РџСЂРѕРёРіРЅРѕСЂРёСЂРѕРІР°С‚СЊ", {"admin": "ticket_ignore", "tid": d_id}), color=KeyboardButtonColor.SECONDARY)
                .get_json()
            )
            await bot.api.messages.send(peer_id=a_id, message=admin_msg, keyboard=kb, random_id=get_rand())
        return

    # --- РўР•РЎРўРћР’Р«Р• РљРћРњРђРќР”Р« Р”Р›РЇ РђР”РњРРќРђ ---
    if user_id in ADMIN_IDS and text.upper() in ["РўР•РЎРў РџРћР”РџРРЎРљР", "РўР•РЎРў РЈР’Р•Р”РћРњР›Р•РќРР™"]:
        is_member, is_allowed = await check_user_subscriptions(user_id)
        # Р”Р»СЏ С‚РµСЃС‚Р° "РѕР±РјР°РЅС‹РІР°РµРј", РµСЃР»Рё С‡РµР»РѕРІРµРє СЂРµР°Р»СЊРЅРѕ РїРѕРґРїРёСЃР°РЅ
        if text.upper() == "РўР•РЎРў РџРћР”РџРРЎРљР": is_member = False
        if text.upper() == "РўР•РЎРў РЈР’Р•Р”РћРњР›Р•РќРР™": is_allowed = False
        
        greeting = await get_user_greeting(user_id)
        await send_sub_request(user_id, is_member, is_allowed, greeting)
        return

    # РџСЂРѕРІРµСЂРєР° РєРѕРґРѕРІС‹С… СЃР»РѕРІ
    kw_data = get_material(text)
    if kw_data:
        # РџРѕР»СѓС‡Р°РµРј РґР°РЅРЅС‹Рµ
        is_member, is_allowed = await check_user_subscriptions(user_id)
        print(f"DEBUG: Check sub for {user_id}: {is_member}, {is_allowed}")
        greeting = await get_user_greeting(user_id)

        # --- РљРЈРЎРћРљ Р”Р›РЇ РџР РћР’Р•Р РљР РљРћРњРњР•РќРўРђР РРЇ (30 РјРёРЅ) ---
        commented = has_commented_recently(user_id, hours=0.5)
        if not commented:
            await message.answer(
                f"{greeting}\n\n"
                "Р§С‚РѕР±С‹ РїРѕР»СѓС‡РёС‚СЊ СЌС‚РѕС‚ Р±РµСЃРїР»Р°С‚РЅС‹Р№ РјР°С‚РµСЂРёР°Р», РїРѕР¶Р°Р»СѓР№СЃС‚Р°, РѕСЃС‚Р°РІСЊС‚Рµ Р»СЋР±РѕР№ РѕСЃРѕР·РЅР°РЅРЅС‹Р№ РєРѕРјРјРµРЅС‚Р°СЂРёР№ "
                "РїРѕРґ Р»СЋР±С‹Рј РІРёРґРµРѕ РёР»Рё РєР»РёРїРѕРј РІ РЅР°С€РµР№ РіСЂСѓРїРїРµ РїСЂСЏРјРѕ СЃРµР№С‡Р°СЃ! рџЋҐ\n\n"
                "Р­С‚Рѕ РїРѕРјРѕРіР°РµС‚ РЅР°Рј СЂР°Р·РІРёРІР°С‚СЊСЃСЏ. РљР°Рє С‚РѕР»СЊРєРѕ РѕСЃС‚Р°РІРёС‚Рµ вЂ” РЅР°РїРёС€РёС‚Рµ СЃР»РѕРІРѕ РµС‰Рµ СЂР°Р·!"
            ); return
        # --------------------------------------------

        # Р•СЃР»Рё С‡С‚Рѕ-С‚Рѕ РЅРµ С‚Р°Рє вЂ” РїСЂРѕСЃРёРј РїРѕРґРїРёСЃР°С‚СЊСЃСЏ
        if not is_member or not is_allowed:
            # Р—Р°РїРѕРјРёРЅР°РµРј, РєР°РєРѕРµ СЃР»РѕРІРѕ С‡РµР»РѕРІРµРє С…РѕС‚РµР» РїРѕР»СѓС‡РёС‚СЊ
            ADMIN_DATA[user_id] = ADMIN_DATA.get(user_id, {})
            ADMIN_DATA[user_id]["pending_kw"] = text
            await send_sub_request(user_id, is_member, is_allowed, greeting)
            return

        t, a = kw_data
        # РЎРѕР±РёСЂР°РµРј СЃРѕРѕР±С‰РµРЅРёРµ Р‘Р•Р— Р±Р»Р°РіРѕРґР°СЂРЅРѕСЃС‚Рё Р·Р° РїРѕРґРїРёСЃРєСѓ (СѓР¶Рµ РїРѕРґРїРёСЃР°РЅ)
        full_msg = f"{greeting}\n{t}"
        
        # Р”РѕР±Р°РІР»СЏРµРј РїРѕР¶РµР»Р°РЅРёРµ, РµСЃР»Рё РµРіРѕ РµС‰Рµ РЅРµС‚ РІ С‚РµРєСЃС‚Рµ РёР· Р±Р°Р·С‹
        if "Р·Р°РЅРёРјР°Р№С‚РµСЃСЊ СЃ СѓРґРѕРІРѕР»СЊСЃС‚РІРёРµРј" not in t.lower() and "Р·Р°РЅРёРјР°Р№СЃСЏ СЃ СѓРґРѕРІРѕР»СЊСЃС‚РІРёРµРј" not in t.lower():
            full_msg += "\nР—Р°РЅРёРјР°Р№С‚РµСЃСЊ СЃ СѓРґРѕРІРѕР»СЊСЃС‚РІРёРµРј:"
        
        if "http" not in t.lower():
            full_msg += "\n\nрџ”— РњР°С‚РµСЂРёР°Р»С‹: http://РјР°С‚РµСЂРёР°Р»С‹-Р±СѓРґСѓС‚-РґРѕР±Р°РІР»РµРЅС‹-РїРѕР·Р¶Рµ"
        
        # Р”РѕР±Р°РІР»СЏРµРј РїСЂРёР·С‹РІ Рє РєР°С‚Р°Р»РѕРіСѓ РІ СЃР°РјС‹Р№ РєРѕРЅРµС†
        full_msg += "\n\nР•СЃР»Рё С…РѕС‚РёС‚Рµ Р±РѕР»СЊС€Рµ РїРѕР»РµР·РЅС‹С… РїСЂРѕРіСЂР°РјРј вЂ” Р·Р°РіР»СЏРЅРёС‚Рµ РІ РЅР°С€ РєР°С‚Р°Р»РѕРі! рџ§вЂЌв™ЂпёЏ"
            
        await bot.api.messages.send(peer_id=user_id, message=full_msg, attachment=a, random_id=get_rand(), keyboard=get_user_main_kb())
        return

    if text.lower() in ["РїСЂРёРІРµС‚", "СЃС‚Р°СЂС‚"]:
        greeting = await get_user_greeting(user_id)
        await message.answer(f"{greeting}! РЇ РїРѕРјРѕС‰РЅРёРє РњР°СЂРёР°РЅРЅС‹. Р’С‹Р±РµСЂРёС‚Рµ РЅСѓР¶РЅС‹Р№ СЂР°Р·РґРµР» РЅРёР¶Рµ:", keyboard=get_user_main_kb())
async def deliver_product(user_id, product_name):
    """Р’С‹РґР°РµС‚ РєРѕРЅС‚РµРЅС‚ С‚РѕРІР°СЂР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ"""
    try:
        content = get_product_at(product_name)
        if not content: return
        text, attachment = content
        
        # РљРѕРјРїР°РєС‚РЅС‹Р№ С„РѕСЂРјР°С‚
        msg = f"рџЋ‰ РЎРїР°СЃРёР±Рѕ! Р’Р°С€Рё РјР°С‚РµСЂРёР°Р»С‹ РїРѕ РїСЂРѕРіСЂР°РјРјРµ В«{product_name}В»:\n{text}"
        
        # Р”РѕР±Р°РІР»СЏРµРј РїРѕР¶РµР»Р°РЅРёРµ, РµСЃР»Рё РµРіРѕ РЅРµС‚
        if "Р·Р°РЅРёРјР°Р№С‚РµСЃСЊ СЃ СѓРґРѕРІРѕР»СЊСЃС‚РІРёРµРј" not in text.lower() and "Р·Р°РЅРёРјР°Р№СЃСЏ СЃ СѓРґРѕРІРѕР»СЊСЃС‚РІРёРµРј" not in text.lower():
            msg += "\nР—Р°РЅРёРјР°Р№С‚РµСЃСЊ СЃ СѓРґРѕРІРѕР»СЊСЃС‚РІРёРµРј:"
            
        params = {"peer_id": user_id, "message": msg, "keyboard": keyboard_main, "random_id": get_rand()}
        if attachment: params["attachment"] = attachment
        await bot.api.messages.send(**params)
        log_purchase(user_id, product_name)
    except: pass

@bot.on.raw_event("message_event", dict)
async def handle_callback(event: dict):
    # РЎСЂР°Р·Сѓ РѕС‚РІРµС‡Р°РµРј Р’Рљ, С‡С‚РѕР±С‹ СѓР±СЂР°С‚СЊ Р·Р°РіСЂСѓР·РєСѓ РЅР° РєРЅРѕРїРєРµ
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
            # РС‰РµРј, РєР°РєРѕРµ СЃР»РѕРІРѕ С‡РµР»РѕРІРµРє Р·Р°РїСЂР°С€РёРІР°Р»
            kw_name = ADMIN_DATA.get(user_id, {}).get("pending_kw")
            if kw_name:
                kw_data = get_material(kw_name)
                if kw_data:
                    t, a = kw_data
                    # РЈР±РёСЂР°РµРј РґСѓР±Р»РёСЂРѕРІР°РЅРёРµ РЅР°Р·РІР°РЅРёСЏ СѓСЂРѕРєР°, РѕСЃС‚Р°РІР»СЏРµРј С‚РѕР»СЊРєРѕ РЎРїР°СЃРёР±Рѕ Рё С‚РµРєСЃС‚ РёР· Р‘Р”
                    full_msg = f"РЎРїР°СЃРёР±Рѕ Р·Р° РїРѕРґРїРёСЃРєСѓ! вќ¤пёЏ\n{t}"
                    
                    if "Р·Р°РЅРёРјР°Р№С‚РµСЃСЊ СЃ СѓРґРѕРІРѕР»СЊСЃС‚РІРёРµРј" not in t.lower() and "Р·Р°РЅРёРјР°Р№СЃСЏ СЃ СѓРґРѕРІРѕР»СЊСЃС‚РІРёРµРј" not in t.lower():
                        full_msg += "\nР—Р°РЅРёРјР°Р№С‚РµСЃСЊ СЃ СѓРґРѕРІРѕР»СЊСЃС‚РІРёРµРј:"
                    
                    if "http" not in t.lower():
                        full_msg += "\n\nрџ”— РњР°С‚РµСЂРёР°Р»С‹: http://РјР°С‚РµСЂРёР°Р»С‹-Р±СѓРґСѓС‚-РґРѕР±Р°РІР»РµРЅС‹-РїРѕР·Р¶Рµ"

                    full_msg += "\n\nР•СЃР»Рё С…РѕС‚РёС‚Рµ Р±РѕР»СЊС€Рµ РїРѕР»РµР·РЅС‹С… РїСЂРѕРіСЂР°РјРј вЂ” Р·Р°РіР»СЏРЅРёС‚Рµ РІ РЅР°С€ РєР°С‚Р°Р»РѕРі! рџ§вЂЌв™ЂпёЏ"

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
                message="РћС‚Р»РёС‡РЅРѕ! РўРµРїРµСЂСЊ РІСЃРµ РіРѕС‚РѕРІРѕ. РќР°РїРёС€РёС‚Рµ РєРѕРґРѕРІРѕРµ СЃР»РѕРІРѕ РµС‰Рµ СЂР°Р·, С‡С‚РѕР±С‹ РїРѕР»СѓС‡РёС‚СЊ РјР°С‚РµСЂРёР°Р»С‹. вњЁ", 
                random_id=get_rand(),
                keyboard=get_user_main_kb()
            )
        else:
            greeting = await get_user_greeting(user_id)
            await send_sub_request(user_id, is_member, is_allowed, greeting, edit_cmid=cmid)
        return

    if payload.get("cmd") == "pay_product":
        title = payload.get("title")
        try: await bot.api.messages.send_message_event_answer(event_id=event["object"]["event_id"], user_id=user_id, peer_id=peer_id, event_data='{"type": "show_snackbar", "text": "РћРїР»Р°С‚Р° СЃРєРѕСЂРѕ Р±СѓРґРµС‚ РґРѕСЃС‚СѓРїРЅР°"}')
        except: pass
        await bot.api.messages.send(
            peer_id=peer_id,
            message=f"РћРїР»Р°С‚Р° С‚РѕРІР°СЂР° В«{title}В» СЃРµР№С‡Р°СЃ РїРѕРґРєР»СЋС‡Р°РµС‚СЃСЏ С‡РµСЂРµР· Robokassa. РњР°С‚РµСЂРёР°Р»С‹ Р±СѓРґСѓС‚ РІС‹РґР°РІР°С‚СЊСЃСЏ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё С‚РѕР»СЊРєРѕ РїРѕСЃР»Рµ РїРѕРґС‚РІРµСЂР¶РґРµРЅРёСЏ РѕРїР»Р°С‚С‹.",
            random_id=get_rand(),
            keyboard=get_user_main_kb()
        )
        return

    # Р®Р—Р•Р -РњР•РќР®
    user_cmd = payload.get("cmd")
    if user_cmd == "support_menu":
        # РџР•Р Р’Р«Р™ РІС…РѕРґ РІ РјРµРЅСЋ - РІСЃРµРіРґР° РќРћР’РћР• СЃРѕРѕР±С‰РµРЅРёРµ, С‡С‚РѕР±С‹ РјР°С‚РµСЂРёР°Р»С‹ РќР• РїСЂРѕРїР°РґР°Р»Рё
        await bot.api.messages.send(
            peer_id=peer_id, 
            message="РљР°РєРѕР№ Сѓ РІР°СЃ РІРѕРїСЂРѕСЃ? Р’С‹Р±РµСЂРёС‚Рµ РёР· С‡Р°СЃС‚С‹С… РёР»Рё РЅР°РїРёС€РёС‚Рµ СЃРІРѕР№:", 
            keyboard=get_support_kb(),
            random_id=get_rand()
        )
        return

    if user_cmd == "support_menu_edit":
        # РќР°РІРёРіР°С†РёСЏ Р’РќРЈРўР Р РјРµРЅСЋ (РЅР°Р·Р°Рґ РѕС‚ РІРѕРїСЂРѕСЃР°) - Р—РђРњР•РќРђ СЃРѕРґРµСЂР¶РёРјРѕРіРѕ
        new_text = "РљР°РєРѕР№ Сѓ РІР°СЃ РІРѕРїСЂРѕСЃ? Р’С‹Р±РµСЂРёС‚Рµ РёР· С‡Р°СЃС‚С‹С… РёР»Рё РЅР°РїРёС€РёС‚Рµ СЃРІРѕР№:"
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
            message="РџРѕР¶Р°Р»СѓР№СЃС‚Р°, РЅР°РїРёС€РёС‚Рµ СЃРІРѕР№ РІРѕРїСЂРѕСЃ РѕРґРЅРёРј СЃРѕРѕР±С‰РµРЅРёРµРј:", 
            conversation_message_id=cmid, 
            keyboard=Keyboard(inline=True).add(Callback("рџ”™ РќР°Р·Р°Рґ", {"cmd": "support_menu_edit"}), color=KeyboardButtonColor.SECONDARY).get_json()
        )
        USER_STATES[user_id] = "waiting_for_custom_question"
        ADMIN_DATA[user_id] = {"msg_id": cmid}
        return

    if user_cmd == "thanks":
        await bot.api.messages.send(
            peer_id=peer_id, 
            message="Р’СЃРµРіРґР° СЂР°РґР° РїРѕРјРѕС‡СЊ! Р•СЃР»Рё РїРѕСЏРІСЏС‚СЃСЏ РµС‰Рµ РІРѕРїСЂРѕСЃС‹ вЂ” СЏ РЅР° СЃРІСЏР·Рё. вњЁ", 
            random_id=get_rand(),
            keyboard=get_user_main_kb()
        )
        return

    faq_id = payload.get("faq")
    if faq_id:
        faqs = {
            1: "рџ“– РљР°Рє РїРѕР»СѓС‡РёС‚СЊ РґРѕСЃС‚СѓРї?\n\nР’СЃРµ РїСЂРѕСЃС‚Рѕ: РїРѕСЃР»Рµ РѕРїР»Р°С‚С‹ Р±РѕС‚ РјРѕРјРµРЅС‚Р°Р»СЊРЅРѕ РїСЂРёС€Р»РµС‚ СЃСЃС‹Р»РєСѓ РЅР° СѓСЂРѕРє РїСЂСЏРјРѕ РІ СЌС‚РѕС‚ С‡Р°С‚. РўР°РєР¶Рµ РІС‹ РјРѕР¶РµС‚Рµ РЅР°Р№С‚Рё РјР°С‚РµСЂРёР°Р»С‹ РІ СЂР°Р·РґРµР»Рµ В«Р’РµСЃСЊ РєР°С‚Р°Р»РѕРіВ» РІ Р»СЋР±РѕРµ РІСЂРµРјСЏ.",
            2: "рџ“– Р“РґРµ РјРѕРё РєСѓСЂСЃС‹?\n\nРќР°Р¶РјРёС‚Рµ РєРЅРѕРїРєСѓ В«Р’РµСЃСЊ РєР°С‚Р°Р»РѕРіВ» РїРѕРґ Р»СЋР±С‹Рј СЃРѕРѕР±С‰РµРЅРёРµРј. РўР°Рј РѕС‚РѕР±СЂР°Р¶Р°СЋС‚СЃСЏ РІСЃРµ РІР°С€Рё РїСЂРёРѕР±СЂРµС‚РµРЅРЅС‹Рµ РїСЂРѕРіСЂР°РјРјС‹ Рё Р±РµСЃРїР»Р°С‚РЅС‹Рµ СѓСЂРѕРєРё.",
            3: "рџ“– РњРѕР¶РЅРѕ Р»Рё Р±РµСЂРµРјРµРЅРЅС‹Рј?\n\nР”Р°, Сѓ РњР°СЂРёР°РЅРЅС‹ РµСЃС‚СЊ СЃРїРµС†РёР°Р»СЊРЅС‹Рµ РјСЏРіРєРёРµ РїСЂР°РєС‚РёРєРё. РћРґРЅР°РєРѕ РјС‹ РІСЃРµРіРґР° СЂРµРєРѕРјРµРЅРґСѓРµРј РїСЂРѕРєРѕРЅСЃСѓР»СЊС‚РёСЂРѕРІР°С‚СЊСЃСЏ СЃ РІР°С€РёРј РІСЂР°С‡РѕРј РїРµСЂРµРґ РЅР°С‡Р°Р»РѕРј Р·Р°РЅСЏС‚РёР№.",
            4: "рџ“– Р›РёС‡РЅС‹Р№ РєР°Р±РёРЅРµС‚\n\nР­С‚Рѕ РјРёРЅРё-РїСЂРёР»РѕР¶РµРЅРёРµ РІРЅСѓС‚СЂРё Р’Рљ, РіРґРµ РІР°С€Рё СѓСЂРѕРєРё СЃС‚СЂСѓРєС‚СѓСЂРёСЂРѕРІР°РЅС‹. РЎСЃС‹Р»РєР° РЅР° РЅРµРіРѕ РІСЃРµРіРґР° РµСЃС‚СЊ РІ РіР»Р°РІРЅРѕРј РјРµРЅСЋ Р±РѕС‚Р°."
        }
        await bot.api.messages.edit(
            peer_id=peer_id, 
            message=faqs.get(faq_id, "РРЅС„РѕСЂРјР°С†РёСЏ СЃРєРѕСЂРѕ РїРѕСЏРІРёС‚СЃСЏ."), 
            conversation_message_id=cmid, 
            keyboard=get_faq_back_kb()
        )
        return

    # РђР”РњРРќ-РњР•РќР®
    admin_cmd = payload.get("admin")
    if admin_cmd and user_id in ADMIN_IDS:
        # РќРѕРІС‹Рµ РєРѕРјР°РЅРґС‹ РѕР±СЂР°Р±РѕС‚РєРё С‚РёРєРµС‚РѕРІ
        if admin_cmd == "reply_start":
            t_id = payload.get("tid")
            u_id = payload.get("uid")
            USER_STATES[user_id] = f"admin_wait_reply_{t_id}_{u_id}"
            await bot.api.messages.send(
                peer_id=user_id,
                message=f"РќР°РїРёС€РёС‚Рµ РѕС‚РІРµС‚ РЅР° РІРѕРїСЂРѕСЃ в„–{t_id} (РёР»Рё РЅР°Р¶РјРёС‚Рµ РєРЅРѕРїРєСѓ РќР°Р·Р°Рґ)",
                keyboard=Keyboard(inline=True).add(Callback("рџ”™ РќР°Р·Р°Рґ", {"admin": "main"}), color=KeyboardButtonColor.SECONDARY).get_json(),
                random_id=get_rand()
            )
            return

        if admin_cmd == "ticket_ignore":
            t_id = payload.get("tid")
            close_ticket(t_id)
            await bot.api.messages.edit(
                peer_id=peer_id,
                message=f"Р’РѕРїСЂРѕСЃ в„–{t_id} РїСЂРѕРёРіРЅРѕСЂРёСЂРѕРІР°РЅ Рё Р·Р°РєСЂС‹С‚.",
                conversation_message_id=cmid,
                keyboard=get_admin_main_kb()
            )
            return
        new_text = ""
        new_kb = get_back_kb()

        if admin_cmd == "main":
            new_text = "РџР°РЅРµР»СЊ Р°РґРјРёРЅРёСЃС‚СЂР°С‚РѕСЂР°\nР’С‹Р±РµСЂРёС‚Рµ РёРЅС‚РµСЂРµСЃСѓСЋС‰РёР№ СЂР°Р·РґРµР»:"
            new_kb = get_admin_main_kb()
        
        elif admin_cmd == "stats":
            u, p = get_stats()
            new_text = f"РЎС‚Р°С‚РёСЃС‚РёРєР° СЃРёСЃС‚РµРјС‹\n\nР’СЃРµРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№: {u}\nР—Р°С„РёРєСЃРёСЂРѕРІР°РЅРѕ РїРѕРєСѓРїРѕРє: {p}"
        
        elif admin_cmd == "tickets":
            tickets = get_open_tickets()
            if not tickets: new_text = "РђРєС‚РёРІРЅС‹Рµ РѕР±СЂР°С‰РµРЅРёСЏ РѕС‚СЃСѓС‚СЃС‚РІСѓСЋС‚."
            else:
                new_text = "РЎРїРёСЃРѕРє Р°РєС‚РёРІРЅС‹С… РѕР±СЂР°С‰РµРЅРёР№:\n\n"
                for tid, did, uid, q in tickets: new_text += f"- [{did}] ID{uid}: {q}\n"
                new_text += "\nР”Р»СЏ Р·Р°РєСЂС‹С‚РёСЏ: /РѕРє РќРћРњР•Р "
        
        elif admin_cmd == "keywords":
            new_text = "РЈРїСЂР°РІР»РµРЅРёРµ РєРѕРґРѕРІС‹РјРё СЃР»РѕРІР°РјРё"
            new_kb = get_kw_menu_kb()

        elif admin_cmd == "kw_add_start":
            new_text = "РќР°РїРёС€РёС‚Рµ РЅРѕРІРѕРµ РєРѕРґРѕРІРѕРµ СЃР»РѕРІРѕ"
            USER_STATES[user_id] = "kw_wait_word"
            ADMIN_DATA[user_id] = {"msg_id": cmid}
            new_kb = get_back_kb("keywords")

        elif admin_cmd == "kw_list":
            USER_STATES[user_id] = None # РЎР±СЂРѕСЃ СЃРѕСЃС‚РѕСЏРЅРёР№ РІРІРѕРґР°
            kws = get_all_keywords()
            if not kws: new_text = "РљРѕРґРѕРІС‹С… СЃР»РѕРІ РїРѕРєР° РЅРµС‚."
            else:
                new_text = "РЎРїРёСЃРѕРє РєРѕРґРѕРІС‹С… СЃР»РѕРІ:\n\n"
                for i, kw in enumerate(kws): new_text += f"{i+1}) {kw[1].upper()} вЂ” {kw[2][:40]}...\n"
                new_kb = Keyboard(inline=True).add(Callback("Р РµРґР°РєС‚РёСЂРѕРІР°С‚СЊ РєРѕРґРѕРІРѕРµ СЃР»РѕРІРѕ", {"admin": "kw_edit_pick"}), color=KeyboardButtonColor.PRIMARY).row()
                new_kb.add(Callback("Р’РµСЂРЅСѓС‚СЊСЃСЏ РЅР°Р·Р°Рґ", {"admin": "keywords"}), color=KeyboardButtonColor.SECONDARY).get_json()

        elif admin_cmd == "kw_edit_pick":
            new_text = "РћС‚РїСЂР°РІСЊС‚Рµ РЅРѕРјРµСЂ РєРѕРґРѕРІРѕРіРѕ СЃР»РѕРІР°, Р·Р° РєРѕС‚РѕСЂС‹Рј С‚СЂРµР±СѓРµС‚СЃСЏ СЂРµРґР°РєС‚РёСЂРѕРІР°РЅРёРµ"
            USER_STATES[user_id] = "kw_wait_index"
            ADMIN_DATA[user_id] = {"msg_id": cmid}
            new_kb = get_back_kb("kw_list")

        elif admin_cmd == "kw_edit_word":
            new_text = "РќР°РїРёС€РёС‚Рµ РќРѕРІРѕРµ СЃР»РѕРІРѕ"
            USER_STATES[user_id] = "kw_wait_edit_word"
            ADMIN_DATA[user_id]["msg_id"] = cmid
            new_kb = get_back_kb("kw_list")

        elif admin_cmd == "kw_edit_text":
            new_text = "РќР°РїРёС€РёС‚Рµ РќРѕРІС‹Р№ С‚РµРєСЃС‚"
            USER_STATES[user_id] = "kw_wait_edit_text"
            ADMIN_DATA[user_id]["msg_id"] = cmid
            new_kb = get_back_kb("kw_list")

        elif admin_cmd == "kw_delete_confirm":
            new_text = "Р’С‹ СѓРІРµСЂРµРЅС‹? РЈРґР°Р»РёС‚СЊ СЌС‚Сѓ РєРѕРјР°РЅРґСѓ?"
            new_kb = Keyboard(inline=True).add(Callback("Р”Р°", {"admin": "kw_delete_yes"}), color=KeyboardButtonColor.NEGATIVE).row()
            new_kb.add(Callback("РќРµС‚", {"admin": "kw_list"}), color=KeyboardButtonColor.SECONDARY).get_json()

        elif admin_cmd == "kw_delete_yes":
            kw_id = ADMIN_DATA.get(user_id, {}).get("edit_kw_id")
            if kw_id:
                delete_keyword_by_id(kw_id)
                try:
                    await bot.api.messages.send_message_event_answer(
                        event_id=event["object"]["event_id"], 
                        user_id=user_id, 
                        peer_id=peer_id, 
                        event_data='{"type": "show_snackbar", "text": "вњ… РљРѕРјР°РЅРґР° СѓРґР°Р»РµРЅР°"}'
                    )
                except: pass
            
            # Р’ Р»СЋР±РѕРј СЃР»СѓС‡Р°Рµ РІРѕР·РІСЂР°С‰Р°РµРј Рє СЃРїРёСЃРєСѓ, С‡С‚РѕР±С‹ РЅРµ "РІРёСЃРµС‚СЊ"
            kws = get_all_keywords()
            new_text = "РЎРїРёСЃРѕРє РєРѕРґРѕРІС‹С… СЃР»РѕРІ:\n\n"
            if not kws:
                new_text += "РЎРїРёСЃРѕРє РїРѕРєР° РїСѓСЃС‚."
            for i, kw in enumerate(kws):
                new_text += f"{i+1}) {kw[1].upper()} вЂ” {kw[2][:40]}...\n"
            
            new_kb = (
                Keyboard(inline=True)
                .add(Callback("Р РµРґР°РєС‚РёСЂРѕРІР°С‚СЊ РґСЂСѓРіРѕРµ СЃР»РѕРІРѕ", {"admin": "kw_edit_pick"}), color=KeyboardButtonColor.PRIMARY).row()
                .add(Callback("Р’РµСЂРЅСѓС‚СЊСЃСЏ РІ РјРµРЅСЋ", {"admin": "keywords"}), color=KeyboardButtonColor.SECONDARY)
                .get_json()
            )

        try:
            await bot.api.messages.edit(peer_id=peer_id, message=new_text, conversation_message_id=cmid, keyboard=new_kb)
        except: pass

# --- РћР‘Р РђР‘РћРўРљРђ РљРћРњРњР•РќРўРђР РР•Р’ Р”Р›РЇ РџР РћР’Р•Р РљР РЈРЎР›РћР’РРЇ ---

# РЎР»СѓС€Р°РµРј РЅРѕРІС‹Рµ РєРѕРјРјРµРЅС‚Р°СЂРёРё РЅР° СЃС‚РµРЅРµ
@bot.on.raw_event("wall_reply_new", dict)
async def handle_wall_comment(event: dict):
    # Р’ vkbottle СЌС‚Рѕ РѕР±СЉРµРєС‚ dict, РґРѕСЃС‚Р°РµРј С‡РµСЂРµР· ["object"]
    user_id = event["object"].get("from_id") or event["object"].get("user_id")
    if user_id > 0:
        add_comment_log(user_id)
        print(f"DEBUG: Р›РѕРі РєРѕРјРјРµРЅС‚Р°СЂРёСЏ РЅР° СЃС‚РµРЅРµ РѕС‚ ID{user_id}")

# РЎР»СѓС€Р°РµРј РЅРѕРІС‹Рµ РєРѕРјРјРµРЅС‚Р°СЂРёРё Рє РІРёРґРµРѕ/РєР»РёРїР°Рј
@bot.on.raw_event("video_comment_new", dict)
async def handle_video_comment(event: dict):
    user_id = event["object"].get("from_id") or event["object"].get("user_id")
    if user_id > 0:
        add_comment_log(user_id)
        print(f"DEBUG: Р›РѕРі РєРѕРјРјРµРЅС‚Р°СЂРёСЏ Рє РІРёРґРµРѕ РѕС‚ ID{user_id}")

if __name__ == "__main__":
    db_init()
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    print("\nвњ… РРќРўР•Р РђРљРўРР’РќРђРЇ РџРђРќР•Р›Р¬ РђР”РњРРќРђ Р“РћРўРћР’Рђ!\n")
    bot.loop_wrapper.add_task(process_paid_orders())
    start_payment_server()
    bot.run_forever()
