DROP TABLE IF EXISTS Messages;
DROP TABLE IF EXISTS Matches;
DROP TABLE IF EXISTS Interactions;
DROP TABLE IF EXISTS Photos;
DROP TABLE IF EXISTS Ratings;
DROP TABLE IF EXISTS Profiles;
DROP TABLE IF EXISTS Users;

-- Создаём таблицу Users
CREATE TABLE Users (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT NOT NULL UNIQUE,
    username TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индекс для быстрого поиска по telegram_id
CREATE INDEX idx_users_telegram_id ON Users(telegram_id);

-- Создаём таблицу Profiles
CREATE TABLE Profiles (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES Users(id) ON DELETE CASCADE,
    age INTEGER NOT NULL,
    gender TEXT NOT NULL CHECK (gender IN ('м', 'ж')),
    interests TEXT,
    city TEXT NOT NULL,
    bio TEXT,
    profile_completeness INTEGER NOT NULL CHECK (profile_completeness >= 0 AND profile_completeness <= 100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для быстрого поиска по user_id, gender и city
CREATE INDEX idx_profiles_user_id ON Profiles(user_id);
CREATE INDEX idx_profiles_gender ON Profiles(gender);
CREATE INDEX idx_profiles_city ON Profiles(city);
CREATE INDEX idx_profiles_gender_city ON Profiles(gender, city); -- Композитный индекс для поиска по gender и city одновременно

-- Создаём таблицу Photos
CREATE TABLE Photos (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES Users(id) ON DELETE CASCADE,
    object_key VARCHAR(255) NOT NULL,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индекс для быстрого поиска фотографий по user_id и сортировки по uploaded_at
CREATE INDEX idx_photos_user_id ON Photos(user_id);
CREATE INDEX idx_photos_uploaded_at ON Photos(uploaded_at);

-- Создаём таблицу Ratings
CREATE TABLE Ratings (
    id SERIAL PRIMARY KEY,
    profile_id INTEGER NOT NULL REFERENCES Profiles(id) ON DELETE CASCADE,
    primary_rating FLOAT NOT NULL DEFAULT 0.0,
    behavioral_rating FLOAT NOT NULL DEFAULT 0.0,
    combined_rating FLOAT NOT NULL DEFAULT 0.0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индекс для быстрого поиска рейтингов по profile_id
CREATE INDEX idx_ratings_profile_id ON Ratings(profile_id);

-- Создаём таблицу Interactions
CREATE TABLE Interactions (
    id SERIAL PRIMARY KEY,
    from_profile_id INTEGER NOT NULL REFERENCES Profiles(id) ON DELETE CASCADE,
    to_profile_id INTEGER NOT NULL REFERENCES Profiles(id) ON DELETE CASCADE,
    action TEXT NOT NULL CHECK (action IN ('like', 'skip')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для быстрого поиска взаимодействий
CREATE INDEX idx_interactions_from_profile_id ON Interactions(from_profile_id);
CREATE INDEX idx_interactions_to_profile_id ON Interactions(to_profile_id);
CREATE INDEX idx_interactions_action ON Interactions(action);

-- Создаём таблицу Matches
CREATE TABLE Matches (
    id SERIAL PRIMARY KEY,
    profile1_id INTEGER NOT NULL REFERENCES Profiles(id) ON DELETE CASCADE,
    profile2_id INTEGER NOT NULL REFERENCES Profiles(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_match UNIQUE (profile1_id, profile2_id)
);

-- Индексы для быстрого поиска мэтчей
CREATE INDEX idx_matches_profile1_id ON Matches(profile1_id);
CREATE INDEX idx_matches_profile2_id ON Matches(profile2_id);

-- Создаём таблицу Messages (для будущей функциональности чата)
CREATE TABLE Messages (
    id SERIAL PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES Matches(id) ON DELETE CASCADE,
    sender_profile_id INTEGER NOT NULL REFERENCES Profiles(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для быстрого поиска сообщений
CREATE INDEX idx_messages_match_id ON Messages(match_id);
CREATE INDEX idx_messages_sender_profile_id ON Messages(sender_profile_id);
CREATE INDEX idx_messages_sent_at ON Messages(sent_at);