CREATE TABLE Users (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT NOT NULL UNIQUE,
    username TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Profiles (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES Users(id),
    age INTEGER NOT NULL,
    gender TEXT NOT NULL,
    interests TEXT,
    city TEXT NOT NULL,
    bio TEXT,
    photo_urls JSONB,
    profile_completeness INTEGER NOT NULL CHECK (profile_completeness >= 0 AND profile_completeness <= 100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Ratings (
    id SERIAL PRIMARY KEY,
    profile_id INTEGER NOT NULL REFERENCES Profiles(id),
    primary_rating FLOAT NOT NULL DEFAULT 0.0,
    behavioral_rating FLOAT NOT NULL DEFAULT 0.0,
    combined_rating FLOAT NOT NULL DEFAULT 0.0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Interactions (
    id SERIAL PRIMARY KEY,
    from_profile_id INTEGER NOT NULL REFERENCES Profiles(id),
    to_profile_id INTEGER NOT NULL REFERENCES Profiles(id),
    action TEXT NOT NULL CHECK (action IN ('like', 'skip')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Matches (
    id SERIAL PRIMARY KEY,
    profile1_id INTEGER NOT NULL REFERENCES Profiles(id),
    profile2_id INTEGER NOT NULL REFERENCES Profiles(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_match UNIQUE (profile1_id, profile2_id)
);


CREATE TABLE Messages (
    id SERIAL PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES Matches(id),
    sender_id INTEGER NOT NULL REFERENCES Profiles(id),
    text TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);