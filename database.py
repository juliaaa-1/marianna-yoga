
import sqlite3
import time
from datetime import date

def db_init():
    """Создание всех нужных таблиц для нашего бота"""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()

    # 1. Таблица кодовых слов (для админки)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            word TEXT UNIQUE,
            content TEXT,      -- Текст или ссылка
            attachment TEXT    -- ID видео/фото в формате: video-123_456
        )
    ''')

    # 2. Таблица тикетов (FAQ вопросы)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tickets (
            ticket_id INTEGER PRIMARY KEY AUTOINCREMENT,
            daily_id INTEGER,   -- Тот самый номер 1, 2, 3...
            user_id INTEGER,
            question TEXT,
            status TEXT DEFAULT 'open', -- 'open' или 'closed'
            date DATE DEFAULT CURRENT_DATE
        )
    ''')
    
    # 3. Таблица для хранения порядкового номера тикета на день
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ticket_counter (
            date DATE PRIMARY KEY,
            last_id INTEGER DEFAULT 0
        )
    ''')

    # 8. Таблица пользователей
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            first_seen INTEGER
        )
    ''')

    # 4. Рандомные шаблоны ответов в комменты (анти-спам)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text TEXT,
            type TEXT -- 'comment_reply', 'greet' и т.д.
        )
    ''')

    # 5. Лог недавних комментариев (для проверки условия)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS recent_comments (
            user_id INTEGER,
            timestamp INTEGER
        )
    ''')

    # 6. Таблица товаров (для автоматической выдачи после оплаты)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            content TEXT,       -- Ссылка на облако или текст
            attachment TEXT     -- ID файла в ВК
        )
    ''')

    # 7. Таблица покупок (кто что купил)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            product_name TEXT,
            timestamp INTEGER
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            product_name TEXT NOT NULL,
            amount TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            robokassa_invoice_id TEXT,
            created_at INTEGER NOT NULL,
            paid_at INTEGER,
            delivered_at INTEGER
        )
    ''')

    conn.commit()
    conn.close()

def add_comment_log(user_id):
    """Записываем, что пользователь оставил комментарий"""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("INSERT INTO recent_comments (user_id, timestamp) VALUES (?, ?)", 
                   (user_id, int(time.time())))
    conn.commit()
    conn.close()

def has_commented_recently(user_id, hours=48):
    """Проверяем, был ли комментарий от юзера за последние X часов"""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    min_time = int(time.time()) - (hours * 3600)
    cursor.execute("SELECT 1 FROM recent_comments WHERE user_id = ? AND timestamp > ? LIMIT 1", 
                   (user_id, min_time))
    res = cursor.fetchone()
    conn.close()
    return res is not None

def clear_old_comments(hours=48):
    """Чистим базу от старых записей комментариев"""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    min_time = int(time.time()) - (hours * 3600)
    cursor.execute("DELETE FROM recent_comments WHERE timestamp < ?", (min_time,))
    conn.commit()
    conn.close()
    print(f"База комментариев очищена (удалены записи старше {hours} ч.)")

def get_next_ticket_id():
    """Получаем следующий номер вопроса (1, 2, 3...) с обнулением каждый день"""
    today = date.today()
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT last_id FROM ticket_counter WHERE date = ?", (today,))
    result = cursor.fetchone()
    
    if result:
        new_id = result[0] + 1
        cursor.execute("UPDATE ticket_counter SET last_id = ? WHERE date = ?", (new_id, today))
    else:
        new_id = 1
        cursor.execute("INSERT INTO ticket_counter (date, last_id) VALUES (?, ?)", (today, new_id))
        
    conn.commit()
    conn.close()
    return new_id

def add_keyword(word, content, attachment=""):
    """Марианна сама добавляет слово через админку"""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT OR REPLACE INTO keywords (word, content, attachment) VALUES (?, ?, ?)", 
                       (word.lower(), content, attachment))
        conn.commit()
        return True
    except:
        return False
    finally:
        conn.close()

def add_product(name, content, attachment=""):
    """Добавляем товар в базу (для автоматизации)"""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO products (name, content, attachment) VALUES (?, ?, ?)", 
                   (name, content, attachment))
    conn.commit()
    conn.close()

def get_product_at(name):
    """Ищем контент товара по его названию (нечувствительно к регистру и знакам)"""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    # Получаем все товары и будем искать совпадение "вручную" для гибкости
    cursor.execute("SELECT name, content, attachment FROM products")
    products = cursor.fetchall()
    conn.close()

    target = name.lower().replace("!", "").replace(".", "").replace("?", "").strip()
    
    for p_name, p_content, p_attach in products:
        clean_name = p_name.lower().replace("!", "").replace(".", "").replace("?", "").strip()
        if clean_name in target or target in clean_name:
            return (p_content, p_attach)
    
    return None

def get_product_by_id(prod_id):
    """Ищем название товара по его ID в таблице products"""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM products WHERE id = ?", (prod_id,))
    res = cursor.fetchone()
    conn.close()
    return res[0] if res else None

def log_purchase(user_id, product_name):
    """Записываем факт покупки"""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("INSERT INTO purchases (user_id, product_name, timestamp) VALUES (?, ?, ?)", 
                   (user_id, product_name, int(time.time())))
    conn.commit()
    conn.close()

def create_order(user_id, product_name, amount):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO orders (user_id, product_name, amount, status, created_at) VALUES (?, ?, ?, 'pending', ?)",
        (user_id, product_name, str(amount), int(time.time()))
    )
    order_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return order_id

def mark_order_paid(order_id, robokassa_invoice_id=None):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE orders
        SET status = 'paid', robokassa_invoice_id = COALESCE(?, robokassa_invoice_id), paid_at = COALESCE(paid_at, ?)
        WHERE id = ? AND status IN ('pending', 'paid')
        """,
        (robokassa_invoice_id, int(time.time()), order_id)
    )
    conn.commit()
    changed = cursor.rowcount > 0
    conn.close()
    return changed

def get_order(order_id):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, user_id, product_name, amount, status, robokassa_invoice_id, created_at, paid_at, delivered_at FROM orders WHERE id = ?",
        (order_id,)
    )
    res = cursor.fetchone()
    conn.close()
    return res

def get_paid_undelivered_orders(limit=10):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, user_id, product_name, amount
        FROM orders
        WHERE status = 'paid' AND delivered_at IS NULL
        ORDER BY paid_at ASC
        LIMIT ?
        """,
        (limit,)
    )
    res = cursor.fetchall()
    conn.close()
    return res

def mark_order_delivered(order_id):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE orders SET delivered_at = ?, status = 'delivered' WHERE id = ?",
        (int(time.time()), order_id)
    )
    conn.commit()
    conn.close()

def get_material(word):
    """Ищем материал по кодовому слову"""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT content, attachment FROM keywords WHERE word = ?", (word.lower(),))
    res = cursor.fetchone()
    conn.close()
    return res

def get_stats():
    """Собираем базовую статистику"""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM users")
    u_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM purchases")
    p_count = cursor.fetchone()[0]
    conn.close()
    return u_count, p_count

def register_user(user_id):
    """Регистрируем нового пользователя"""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO users (user_id, first_seen) VALUES (?, ?)", 
                   (user_id, int(time.time())))
    conn.commit()
    conn.close()

def add_ticket(user_id, question):
    """Добавляем новый вопрос в техподдержку"""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    
    # Считаем номер вопроса за сегодня
    today = date.today().isoformat()
    cursor.execute("INSERT OR IGNORE INTO ticket_counter (date, last_id) VALUES (?, 0)", (today,))
    cursor.execute("UPDATE ticket_counter SET last_id = last_id + 1 WHERE date = ?", (today,))
    cursor.execute("SELECT last_id FROM ticket_counter WHERE date = ?", (today,))
    daily_id = cursor.fetchone()[0]

    cursor.execute("INSERT INTO tickets (daily_id, user_id, question) VALUES (?, ?, ?)", 
                   (daily_id, user_id, question))
    conn.commit()
    conn.close()
    return daily_id

def get_open_tickets():
    """Список всех открытых вопросов"""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT ticket_id, daily_id, user_id, question FROM tickets WHERE status = 'open' LIMIT 10")
    res = cursor.fetchall()
    conn.close()
    return res

def close_ticket(ticket_id):
    """Закрываем вопрос"""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("UPDATE tickets SET status = 'closed' WHERE ticket_id = ?", (ticket_id,))
    conn.commit()
    conn.close()

def add_keyword(word, content, attachment=""):
    """Добавляем или обновляем кодовое слово"""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO keywords (word, content, attachment) VALUES (?, ?, ?)", 
                   (word.lower(), content, attachment))
    conn.commit()
    conn.close()

def get_all_keywords():
    """Получаем полный список всех слов"""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT id, word, content FROM keywords")
    res = cursor.fetchall()
    conn.close()
    return res

def delete_keyword_by_id(kw_id):
    """Удаление по внутреннему ID"""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("DELETE FROM keywords WHERE id = ?", (kw_id,))
    conn.commit()
    conn.close()

def update_keyword_fields(kw_id, word=None, content=None):
    """Точечное обновление полей"""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    if word:
        cursor.execute("UPDATE keywords SET word = ? WHERE id = ?", (word.lower(), kw_id))
    if content:
        cursor.execute("UPDATE keywords SET content = ? WHERE id = ?", (content, kw_id))
    conn.commit()
    conn.close()

def get_latest_user_question(user_id):
    """Находим самый свежий открытый вопрос пользователя"""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT question, ticket_id FROM tickets WHERE user_id = ? AND status = 'open' ORDER BY ticket_id DESC LIMIT 1", (user_id,))
    res = cursor.fetchone()
    conn.close()
    return res
