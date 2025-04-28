from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove

# Главная клавиатура (горизонтальная)
main_menu_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="Поиск анкет 🔍"),
            KeyboardButton(text="Мой профиль 📝"),
            KeyboardButton(text="Редактировать ✏️")
        ]
    ],
    resize_keyboard=True,
    one_time_keyboard=False
)

# Клавиатура для меню редактирования профиля (горизонтальная)
edit_profile_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="Отредактировать ✏️"),
            KeyboardButton(text="Назад ⬅️")
        ]
    ],
    resize_keyboard=True,
    one_time_keyboard=True
)

# Клавиатура для удаления
remove_keyboard = ReplyKeyboardRemove()